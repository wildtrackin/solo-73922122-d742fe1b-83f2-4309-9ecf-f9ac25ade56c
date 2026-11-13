"""馆藏目录采集网页（FastAPI 主应用）。

环境变量
--------
CATALOG_DATA_DIR      SQLite/输出目录（默认 <repo>/data）
OAI_ENDPOINT          固定 OAI-PMH 地址（默认 http://oai-source:8001/oai）
OAI_SOURCE_HOME       演示源首页（默认 http://localhost:8001/）
PUBLISH_FAILURE_MODE  启动时预置一次性发布故障：none|generate_error|commit_error|
                      crash_before_commit|crash_after_publish
MAX_PAGES_PER_CALL    单次后台采集最多处理页数（演示分批与续采）
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
import asyncio
import logging
import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .catalog import KNOWN_FAULTS, cleanup_orphans
from .db import Database
from .harvester import HarvestBlocked, Harvester, Publisher

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

    tasks: dict[int, asyncio.Task] = {}

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
        yield
        for task in list(tasks.values()):
            task.cancel()
        db.close()

    app = FastAPI(title="机构库 → 馆藏目录采集", lifespan=lifespan)
    app.state.db = db
    app.state.harvester = harvester

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
        busy = any(not t.done() for t in tasks.values()) or bool(
            run and run.status in ("running", "publishing")
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

    @app.get("/catalog/download")
    def catalog_download():
        latest = db.latest_publication()
        if latest is None:
            raise HTTPException(status_code=404, detail="尚无可下载的目录")
        path = latest["file_path"]
        if not os.path.exists(path):
            raise HTTPException(status_code=410, detail="已登记的目录文件缺失")
        return FileResponse(
            path,
            media_type="application/xml",
            filename=latest["file_name"],
            headers={"X-Catalog-Watermark": latest["watermark"], "X-Catalog-Run": str(latest["run_id"])},
        )

    @app.get("/catalog.xml")
    def catalog_latest():
        return catalog_download()

    @app.get("/healthz", response_class=PlainTextResponse)
    def healthz():
        return "ok"

    return app


app = build_app()
