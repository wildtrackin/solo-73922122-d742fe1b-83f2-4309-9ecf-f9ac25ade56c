"""馆藏目录采集网页（FastAPI 主应用）。

环境变量
--------
CATALOG_DATA_DIR      SQLite/输出目录（默认 <repo>/data）
OAI_ENDPOINT          固定 OAI-PMH 地址（默认 http://oai-source:8001/oai）
OAI_SOURCE_HOME       演示源首页（默认 http://localhost:8001/）
PUBLISH_FAILURE_MODE  启动时预置一次性发布故障：none|generate_error|commit_error|
                      crash_before_commit|crash_after_publish
MAX_PAGES_PER_CALL    单次后台采集最多处理页数（演示分批与续采）
RECLAIM_FAILURE_MODE  启动时预置一次性回收故障：none|delete_error|crash_after_file_delete
DOWNLOAD_THROTTLE_BPS 下载限速（字节/秒，演示慢下载）；单次请求可用 ?throttle= 覆盖
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from urllib.parse import quote
import asyncio
import logging
import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .catalog import KNOWN_FAULTS, cleanup_orphans
from .db import Database
from .download import LeasedFileResponse
from .harvester import HarvestBlocked, Harvester, Publisher
from .reclaim import KNOWN_RECLAIM_FAULTS, FileLeaseRegistry, Reclaimer

log = logging.getLogger("catalog")

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("CATALOG_DATA_DIR", os.path.join(HERE, "..", "data"))
DB_PATH = os.path.join(DATA_DIR, "catalog.db")
OUTPUT_DIR = os.path.join(DATA_DIR, "published")
ENDPOINT = os.environ.get("OAI_ENDPOINT", "http://oai-source:8001/oai")
SOURCE_HOME = os.environ.get("OAI_SOURCE_HOME", "http://localhost:8001/")
FAILURE_MODE = os.environ.get("PUBLISH_FAILURE_MODE", "none").strip() or "none"
if FAILURE_MODE not in KNOWN_FAULTS:
    FAILURE_MODE = "none"
_raw_max = os.environ.get("MAX_PAGES_PER_CALL", "").strip()
MAX_PAGES = int(_raw_max) if _raw_max.isdigit() and int(_raw_max) > 0 else None
RECLAIM_FAILURE_MODE = os.environ.get("RECLAIM_FAILURE_MODE", "none").strip() or "none"
if RECLAIM_FAILURE_MODE not in KNOWN_RECLAIM_FAULTS:
    RECLAIM_FAILURE_MODE = "none"
_raw_bps = os.environ.get("DOWNLOAD_THROTTLE_BPS", "").strip()
DEFAULT_THROTTLE_BPS = int(_raw_bps) if _raw_bps.isdigit() and int(_raw_bps) > 0 else 0
RECLAIM_KEEP = 2  # 回收始终保留最近两次发布（含当前水位对应的发布）

_STATUS_LABELS = {
    "running": "采集中",
    "publishing": "发布中",
    "paused": "已暂停·可续采",
    "publish_failed": "发布失败·可重试",
    "completed": "已完成",
    "canceled": "已放弃",
}


def _fmt_local(value: str | None) -> str:
    if not value:
        return "—"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value


def _status_label(value: str | None) -> str:
    return _STATUS_LABELS.get(value or "", value or "")


def _outcome_message(outcome) -> str:
    run = outcome.run
    if outcome.published:
        pub = outcome.publication or {}
        return (
            f"第 {run.id} 轮完成并发布：{pub.get('record_count', 0)} 条有效记录，"
            f"水位 {pub.get('watermark')}。"
        )
    if run.status == "paused":
        if run.error_code == "chunk_limit":
            return f"第 {run.id} 轮已按页数上限暂停，进度已保存，可继续采集。"
        return f"第 {run.id} 轮已暂停（{run.error_code}）：进度已保存，可继续采集。"
    if run.status == "publish_failed":
        return f"第 {run.id} 轮采集完成但发布失败，旧目录仍可下载；可重试发布。"
    if run.status == "running":
        return f"第 {run.id} 轮已在后台开始采集。"
    return f"第 {run.id} 轮状态：{run.status}"


def build_app() -> FastAPI:
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    db = Database(DB_PATH)

    # 一次性故障注入：环境变量只在"尚未消费过"时预置，避免重启后永远失败。
    if FAILURE_MODE != "none" and db.get_meta("publish_fault_seeded") is None:
        db.set_meta("publish_fault", FAILURE_MODE)
        db.set_meta("publish_fault_seeded", "1")
    if RECLAIM_FAILURE_MODE != "none" and db.get_meta("reclaim_fault_seeded") is None:
        db.set_meta("reclaim_fault", RECLAIM_FAILURE_MODE)
        db.set_meta("reclaim_fault_seeded", "1")

    # 启动恢复：不改动任何"已发布"引用，只把中断状态变成可续采/可重试。
    db.recover_after_restart()
    cleanup_orphans(
        OUTPUT_DIR,
        keep_paths={row["file_path"] for row in db.query("SELECT file_path FROM publications")},
    )
    for run in db.recent_runs(50):
        if run.status != "publishing":
            continue
        if db.publication_for_run(run.id) is not None:
            db.mark_completed(run.id)  # 发布记录已落库，仅状态没来得及写
        else:
            db.mark_publish_failed(run.id, "进程在发布过程中退出；旧目录仍可用，请重试发布。")

    harvester = Harvester(
        db,
        ENDPOINT,
        Publisher(db, OUTPUT_DIR, fault="none"),
        max_pages_per_call=MAX_PAGES,
    )

    leases = FileLeaseRegistry()
    reclaimer = Reclaimer(db, leases, keep=RECLAIM_KEEP)

    tasks: dict[int, asyncio.Task] = {}
    reclaim_tasks: dict[str, asyncio.Task] = {}

    def spawn_reclaim(reclaim_id: int) -> bool:
        """后台执行回收（单 Web 进程内，不另起服务）；已在执行时不重复派生。"""
        existing = reclaim_tasks.get("task")
        if existing is not None and not existing.done():
            return False
        task = asyncio.create_task(reclaimer.execute(reclaim_id), name=f"reclaim-{reclaim_id}")

        def _done(t: asyncio.Task) -> None:
            reclaim_tasks.pop("task", None)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log.error("reclaim task failed: %r", exc)

        task.add_done_callback(_done)
        reclaim_tasks["task"] = task
        return True

    def resume_pending_reclaim() -> int | None:
        """重启自动续办：有未完成的回收就接着清（不新建、不改目标集）。"""
        run = db.active_reclaim_run()
        if run is None:
            return None
        spawn_reclaim(run["id"])
        return run["id"]

    def reclaim_context() -> dict | None:
        run = db.latest_reclaim_run()
        if run is None:
            return None
        targets = db.reclaim_targets(run["id"])
        claimed = leases.claimed()
        waiting = {
            t["file_name"]: claimed[os.path.abspath(t["file_path"])]
            for t in targets
            if os.path.abspath(t["file_path"]) in claimed
        }
        task = reclaim_tasks.get("task")
        return {
            "run": run,
            "progress": db.reclaim_progress(run["id"]),
            "targets": targets[:20],
            "waiting": waiting,
            "task_active": bool(task is not None and not task.done()),
        }

    def spawn(run_id: int, coro) -> None:
        existing = tasks.get(run_id)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(coro, name=f"harvest-run-{run_id}")

        def _done(t: asyncio.Task) -> None:
            tasks.pop(run_id, None)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log.error("harvest task failed: %r", exc)

        task.add_done_callback(_done)
        tasks[run_id] = task

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        resume_pending_reclaim()  # 上次退出时未完成的回收，启动即自动续办
        yield
        for task in list(tasks.values()):
            task.cancel()
        current = reclaim_tasks.get("task")
        if current is not None:
            current.cancel()
        db.close()

    app = FastAPI(title="机构库 → 馆藏目录采集", lifespan=lifespan)
    app.state.db = db
    app.state.harvester = harvester
    app.state.leases = leases
    app.state.reclaimer = reclaimer
    app.state.resume_pending_reclaim = resume_pending_reclaim

    templates = Jinja2Templates(directory=os.path.join(HERE, "templates"))
    templates.env.filters["localtime"] = _fmt_local
    templates.env.filters["status_label"] = _status_label
    static_dir = os.path.join(HERE, "static")
    if os.path.isdir(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        # 有未结束轮次时聚焦它；否则展示最近一轮的结果（进度与错误仍可见）。
        recent = db.recent_runs(1)
        run = db.unresolved_run() or (recent[0] if recent else None)
        latest = db.latest_publication()
        active_count, deleted_count = db.record_stats()
        reclaim = reclaim_context()
        busy = any(not t.done() for t in tasks.values()) or bool(
            run and run.status in ("running", "publishing")
        ) or bool(
            reclaim and reclaim["run"]["status"] == "running" and reclaim["task_active"]
        )
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "run": run,
                "latest": latest,
                "watermark": db.watermark(),
                "active_count": active_count,
                "deleted_count": deleted_count,
                "runs": db.recent_runs(12),
                "publications": db.publications(8),
                "today": datetime.now(timezone.utc).date().isoformat(),
                "endpoint": ENDPOINT,
                "source_home": SOURCE_HOME,
                "fault": db.get_meta("publish_fault") or "none",
                "reclaim": reclaim,
                "reclaim_fault": db.get_meta("reclaim_fault") or "none",
                "reclaim_keep": RECLAIM_KEEP,
                "busy": busy,
                "changed": db.records_changed_in_run(run.id)[:30] if run else [],
                "run_is_active": bool(run and run.is_unresolved),
                "message": request.query_params.get("message"),
                "error": request.query_params.get("error"),
            },
        )

    @app.post("/harvest/start")
    async def harvest_start(request: Request):
        form = await request.form()
        raw = str(form.get("until", "") or "").strip()
        try:
            until = date.fromisoformat(raw) if raw else datetime.now(timezone.utc).date()
        except ValueError:
            return RedirectResponse(url="/?error=截止日格式无效", status_code=303)
        try:
            run = await harvester.start(until)
        except (HarvestBlocked, RuntimeError) as exc:
            return RedirectResponse(url=f"/?error={exc}", status_code=303)
        spawn(run.id, harvester.resume(run.id))
        return RedirectResponse(url=f"/?message=第 {run.id} 轮已在后台开始采集", status_code=303)

    @app.post("/harvest/{run_id}/resume")
    async def harvest_resume(run_id: int):
        run = db.get_run(run_id)
        if run is None or not run.is_unresolved:
            return RedirectResponse(url="/?error=该轮次无需继续", status_code=303)
        spawn(run_id, harvester.resume(run_id))
        return RedirectResponse(url=f"/?message=第 {run_id} 轮正在后台继续", status_code=303)

    @app.post("/harvest/{run_id}/retry-publish")
    async def harvest_retry_publish(run_id: int):
        run = db.get_run(run_id)
        if run is None or run.status != "publish_failed":
            return RedirectResponse(url="/?error=该轮次当前不需要重试发布", status_code=303)
        db.begin_publish_retry(run_id)
        spawn(run_id, harvester.retry_publish_after_begin(run_id))
        return RedirectResponse(url=f"/?message=第 {run_id} 轮正在重新发布", status_code=303)

    @app.post("/harvest/{run_id}/cancel")
    def harvest_cancel(run_id: int):
        run = db.get_run(run_id)
        if run is None or not run.is_unresolved:
            return RedirectResponse(url="/?error=该轮次不可放弃", status_code=303)
        task = tasks.get(run_id)
        if task is not None and not task.done():
            return RedirectResponse(url="/?error=采集任务仍在运行，请等其暂停后再放弃", status_code=303)
        db.cancel_run(run_id)
        return RedirectResponse(url="/?message=已放弃本轮，水位与已发布目录保持不变", status_code=303)

    @app.post("/config/fault")
    async def config_fault(request: Request):
        form = await request.form()
        mode = str(form.get("fault", "none"))
        if mode not in KNOWN_FAULTS:
            return RedirectResponse(url="/?error=未知故障模式", status_code=303)
        db.set_meta("publish_fault", mode)
        return RedirectResponse(url=f"/?message=下一次发布将使用故障注入：{mode}", status_code=303)

    # -------------------------------------------------------------- 回收
    @app.post("/reclaim/start")
    async def reclaim_start():
        """启动回收；已有未完成回收时沿用同次（不新建、不重拍目标快照）。"""
        run, created = reclaimer.start_or_reuse()
        if run is None:
            return RedirectResponse(
                url=f"/?message=没有可回收的历史目录（始终保留最近 {RECLAIM_KEEP} 次发布）",
                status_code=303,
            )
        spawn_reclaim(run["id"])
        if created:
            total = db.reclaim_progress(run["id"])["total"]
            message = f"回收 #{run['id']} 已启动：{total} 个历史文件及其发布登记待清理"
        else:
            message = f"已有未完成的回收 #{run['id']}，沿用同次继续清理"
        # 消息含 # 等字符，先编码再拼接（RedirectResponse 的 safe 字符集保留 #）
        return RedirectResponse(url=f"/?message={quote(message)}", status_code=303)

    @app.post("/config/reclaim-fault")
    async def config_reclaim_fault(request: Request):
        form = await request.form()
        mode = str(form.get("reclaim_fault", "none"))
        if mode not in KNOWN_RECLAIM_FAULTS:
            return RedirectResponse(url="/?error=未知回收故障模式", status_code=303)
        db.set_meta("reclaim_fault", mode)
        return RedirectResponse(url=f"/?message=下一次回收将注入故障：{mode}（一次性）", status_code=303)

    @app.get("/api/overview")
    def api_overview():
        """机器可读的总览（演示脚本/运维轮询用）：水位、轮次、发布与回收进度。"""
        recent = db.recent_runs(1)
        reclaim = reclaim_context()
        return {
            "watermark": db.watermark(),
            "latest_run": ({"id": recent[0].id, "status": recent[0].status} if recent else None),
            "publications": [dict(p) for p in db.publications(50)],
            "reclaim": (
                {
                    "id": reclaim["run"]["id"],
                    "status": reclaim["run"]["status"],
                    "task_active": reclaim["task_active"],
                    "waiting": reclaim["waiting"],
                    "progress": reclaim["progress"],
                    "targets": [dict(t) for t in reclaim["targets"]],
                }
                if reclaim
                else None
            ),
        }

    def _throttle_for(request: Request) -> int:
        raw = request.query_params.get("throttle", "").strip()
        if raw.isdigit() and int(raw) > 0:
            return min(int(raw), 20_000_000)
        return DEFAULT_THROTTLE_BPS

    @app.get("/catalog/download")
    async def catalog_download(request: Request):
        throttle = _throttle_for(request)
        for _ in range(3):
            latest = db.latest_publication()
            if latest is None:
                raise HTTPException(status_code=404, detail="尚无可下载的目录")
            path = latest["file_path"]
            if not leases.try_acquire(path):
                # 该文件正被回收删除（已拒绝新租约）：重新解析最新发布再试，
                # 绝不发送删了一半的文件，也绝不混版。
                await asyncio.sleep(0.05)
                continue
            try:
                # 租约随响应发送完毕/断连收尾才释放；此后回收才可删该文件。
                return LeasedFileResponse(
                    path,
                    leases=leases,
                    filename=latest["file_name"],
                    extra_headers={
                        "X-Catalog-Watermark": latest["watermark"],
                        "X-Catalog-Run": str(latest["run_id"]),
                    },
                    throttle_bps=throttle,
                )
            except OSError:
                leases.release(path)
                raise HTTPException(status_code=410, detail="已登记的目录文件缺失")
        raise HTTPException(status_code=409, detail="目录文件正在回收，请重试")

    @app.get("/catalog.xml")
    async def catalog_latest(request: Request):
        return await catalog_download(request)

    @app.get("/healthz", response_class=PlainTextResponse)
    def healthz():
        return "ok"

    return app


app = build_app()
