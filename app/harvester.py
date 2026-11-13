"""采集编排：首次全量，此后按水位增量；分页进度落库，可续采。

规则
----
1. 首次（无水位）：ListRecords 不带 from，window_from=NULL。
2. 此后：window_from = 上次成功发布的水位（含当天），window_until 由页面选择
   （默认今天）。from/until 都是含当天，因此同日多次变更不会漏采。
3. 每页成功后立即 UPSERT 到 SQLite，并把下一页 token 写入 runs 表；进程重启/
   断流后从 token 续采。重复页只会产生幂等 UPSERT。
4. badResumptionToken：不推进水位、不清空已入库记录，从原窗口第一页重采。
5. 采集只改 records；发布独立进行，只有发布登记成功才产生新水位。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import asyncio
import random

from .db import Database, Run, utcnow
from .oai import OAIFatalError, OAIError, OAIHttpClient, Page


class HarvestBlocked(Exception):
    pass


@dataclass
class HarvestOutcome:
    run: Run
    published: bool = False
    publication: dict | None = None
    message: str | None = None


@dataclass
class Publisher:
    """把"生成 + 原子发布"包成可替换的发布动作；失败抛异常，旧目录不动。"""

    db: Database
    output_dir: str
    fault: str = "none"
    exit_process: object = None

    def publish(self, run: Run, records: list, watermark: str) -> dict:
        from .catalog import write_publication
        import os

        return write_publication(
            self.db,
            self.output_dir,
            run.id,
            watermark,
            records,
            fault=self.fault,
            exit_process=self.exit_process or os._exit,
        )


class Harvester:
    def __init__(
        self,
        db: Database,
        endpoint: str,
        publisher: Publisher,
        *,
        max_pages_per_call: int | None = None,
        max_retries: int = 2,
        retry_delay: float = 0.4,
        page_delay: float = 0.0,
        max_token_resets: int = 3,
    ):
        self.db = db
        self.endpoint = endpoint
        self.publisher = publisher
        self.max_pages_per_call = max_pages_per_call
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.page_delay = page_delay
        self.max_token_resets = max_token_resets
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ 入口
    async def start(self, window_until: date) -> Run:
        if self.db.unresolved_run() is not None:
            raise HarvestBlocked("已有未结束的轮次，请先继续/重试或放弃。")
        watermark = self.db.watermark()
        if watermark is None:
            return self.db.create_run("full", None, window_until)
        window_from = date.fromisoformat(watermark)
        if window_until < window_from:
            raise HarvestBlocked(
                f"本轮截止日 {window_until} 早于当前水位 {window_from}；"
                "增量窗口必须含上次成功截止日，不能回退。"
            )
        return self.db.create_run("incremental", window_from, window_until)

    async def resume(self, run_id: int) -> HarvestOutcome:
        """继续未结束轮次：paused/running 续采；publish_failed 只重试发布。"""
        async with self._lock:
            run = self.db.get_run(run_id)
            if run is None:
                raise HarvestBlocked(f"轮次 #{run_id} 不存在")
            if not run.is_unresolved:
                raise HarvestBlocked(f"轮次 #{run_id} 已结束（{run.status}），无需继续")

            if run.status == "publish_failed":
                return await self._publish(run)
            if run.status == "publishing":
                return await self._publish(run)

            if run.status == "paused":
                with self.db.tx() as conn:
                    conn.execute(
                        "UPDATE runs SET status='running', error_code=NULL, error_message=NULL, updated_at=? "
                        "WHERE id=? AND status='paused'",
                        (utcnow(), run_id),
                    )
                run = self.db.get_run(run_id)
                assert run is not None
            return await self._harvest_loop(run_id)

    async def retry_publish(self, run_id: int) -> HarvestOutcome:
        async with self._lock:
            run = self.db.get_run(run_id)
            if run is None or run.status != "publish_failed":
                raise HarvestBlocked("该轮次当前不需要重试发布")
            self.db.begin_publish_retry(run_id)
            run = self.db.get_run(run_id)
            assert run is not None
            return await self._publish(run)

    async def retry_publish_after_begin(self, run_id: int) -> HarvestOutcome:
        """页面已把状态置为 publishing，这里直接重试（避免再次 begin 失败）。"""
        async with self._lock:
            run = self.db.get_run(run_id)
            if run is None or run.status != "publishing":
                raise HarvestBlocked("该轮次不在发布中")
            return await self._publish(run)

    async def start_in_background(self, window_until: date, on_created=None) -> HarvestOutcome:
        """创建轮次后立即采集并发布（测试/脚本便捷入口）。"""
        run = await self.start(window_until)
        if on_created:
            on_created(run)
        return await self.resume(run.id)

    # -------------------------------------------------------------- 采集循环
    async def _harvest_loop(self, run_id: int) -> HarvestOutcome:
        run = self.db.get_run(run_id)
        assert run is not None
        start_pages = run.pages_done
        token = run.resumption_token
        pages_done = run.pages_done
        records_seen = run.records_seen

        async with OAIHttpClient(self.endpoint, timeout=30.0) as client:
            while True:
                if (
                    self.max_pages_per_call is not None
                    and (pages_done - start_pages) >= self.max_pages_per_call
                ):
                    self.db.pause_run(run_id, "chunk_limit", "单次调用页数上限，进度已保存。")
                    run = self.db.get_run(run_id)
                    assert run is not None
                    return HarvestOutcome(run=run, message="progress saved (chunk limit)")

                try:
                    page = await self._fetch_page(client, run, token)
                except OAIError as exc:
                    if exc.code == "badResumptionToken":
                        if run.token_resets >= self.max_token_resets:
                            self.db.pause_run(
                                run_id,
                                "badResumptionToken",
                                f"分页凭据连续失效 {run.token_resets} 次，已停止以免循环重采。",
                            )
                            return await self._paused_outcome(run_id, "badResumptionToken")
                        note = (
                            f"分页凭据失效（badResumptionToken），已从原窗口第一页重采"
                            f"（第 {run.token_resets + 1} 次重置）；水位保持不变。"
                        )
                        self.db.reset_for_relist(run_id, note)
                        token = None
                        pages_done = 0
                        records_seen = 0
                        start_pages = 0
                        run = self.db.get_run(run_id)
                        assert run is not None
                        continue
                    self.db.pause_run(run_id, exc.code, exc.message or "OAI 错误")
                    return await self._paused_outcome(run_id, exc.code)
                except Exception as exc:
                    self.db.pause_run(run_id, "interrupted", f"{type(exc).__name__}: {exc}")
                    return await self._paused_outcome(run_id, "interrupted")

                for rec in page.records:
                    self.db.upsert_record(
                        {
                            "identifier": rec.identifier,
                            "datestamp": rec.datestamp,
                            "deleted": rec.deleted,
                            "metadata": rec.metadata,
                            "metadata_xml": rec.metadata_xml,
                            "sets": rec.sets,
                        },
                        run_id,
                    )

                pages_done += 1
                records_seen += len(page.records)
                token = page.token
                self.db.update_progress(
                    run_id,
                    token=token,
                    pages_done=pages_done,
                    records_seen=records_seen,
                    complete_list_size=page.complete_list_size,
                    cursor=page.cursor,
                )

                if not token:
                    break
                if self.page_delay > 0:
                    await asyncio.sleep(self.page_delay)

        self.db.complete_harvest(run_id)
        run = self.db.get_run(run_id)
        assert run is not None
        return await self._publish(run)

    async def _paused_outcome(self, run_id: int, code: str) -> HarvestOutcome:
        run = self.db.get_run(run_id)
        assert run is not None
        return HarvestOutcome(run=run, message=f"paused: {code}")

    async def _fetch_page(self, client: OAIHttpClient, run: Run, token: str | None) -> Page:
        window_from = date.fromisoformat(run.window_from) if run.window_from else None
        window_until = date.fromisoformat(run.window_until) if run.window_until else None
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                return await client.list_records(from_=window_from, until=window_until, token=token)
            except OAIError:
                raise
            except Exception as exc:  # 网络、超时、XML 损坏等：重试后交给上层暂停
                last_error = exc
            if attempt < self.max_retries:
                await asyncio.sleep(self.retry_delay * (2 ** attempt) + random.uniform(0, 0.2))
        assert last_error is not None
        raise last_error

    # ------------------------------------------------------------------ 发布
    async def _publish(self, run: Run) -> HarvestOutcome:
        watermark = run.window_until  # 只有发布登记成功，这个值才会成为新水位
        records = self.db.active_records()
        try:
            publication = await asyncio.to_thread(self.publisher.publish, run, records, watermark)
        except Exception as exc:
            # 故障注入的进程退出（SystemExit/os._exit）不落 publish_failed：
            # 进程重启后由启动恢复逻辑根据 publications 判断，保证水位与文件成对。
            self.db.mark_publish_failed(run.id, f"{type(exc).__name__}: {exc}")
            run2 = self.db.get_run(run.id)
            assert run2 is not None
            return HarvestOutcome(run=run2, published=False, message=str(exc))

        self.db.mark_completed(run.id)
        run2 = self.db.get_run(run.id)
        assert run2 is not None
        return HarvestOutcome(run=run2, published=True, publication=publication)
