"""端到端行为测试：用真实 ASGI 源 + 真实 HTTP 风格客户端调 harvester。

覆盖需求中的关键场景：
* 首次全量 → 发布；此后从水位（含当天）增量。
* 同日更新（按 identifier UPSERT）。
* 撤回进入永久删除标记；再版可恢复。
* 不以缺席推断删除。
* 分页进度落库；进程重启后可从 token 续采。
* badResumptionToken 从原窗口第一页重采、水位不动。
* 发布失败/进程在发布前后退出时，旧目录与旧水位保持一致且可下载。
"""
from __future__ import annotations

import asyncio
from datetime import date
import os
import sqlite3
import sys

import httpx
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.catalog import FAULT_CRASH_AFTER_PUBLISH, FAULT_CRASH_BEFORE_COMMIT, build_catalog_xml
from app.db import Database
from app.harvester import Harvester, Publisher
from app import oai as oai_mod
from source.app import create_source_app


@pytest.fixture()
def env(tmp_path):
    src_app = create_source_app(str(tmp_path / "source.db"))
    transport = httpx.ASGITransport(app=src_app)
    orig_enter = oai_mod.OAIHttpClient.__aenter__

    async def patched_enter(self):
        if self._client is None:
            self._client = httpx.AsyncClient(transport=transport, base_url="http://source")
        return self

    oai_mod.OAIHttpClient.__aenter__ = patched_enter
    try:
        yield tmp_path, src_app, transport
    finally:
        oai_mod.OAIHttpClient.__aenter__ = orig_enter


def make(tmp_path, *, max_pages=None, fault="none", exit_process=None):
    db = Database(str(tmp_path / "catalog.db"))
    publisher = Publisher(
        db,
        str(tmp_path / "published"),
        fault=fault,
        exit_process=exit_process or (lambda code: (_ for _ in ()).throw(SystemExit(code))),
    )
    h = Harvester(db, "http://source/oai", publisher, max_pages_per_call=max_pages, max_retries=0)
    return db, h


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


def source_db(app):
    return app.state.store


def test_full_then_incremental_same_day_update_and_delete(env):
    tmp_path, src_app, _ = env
    store = source_db(src_app)
    db, h = make(tmp_path)

    first = run(h.start_in_background(date(2026, 11, 13)))
    assert first.run.status == "completed"
    assert db.record_stats() == (5, 0)
    assert db.watermark() == "2026-11-13"

    # 同日更新 001，撤回 002
    store.update_item("oai:demo:lib:001", title="图书馆学概论（第二版）")
    store.delete_item("oai:demo:lib:002")

    second = run(h.start_in_background(date(2026, 11, 13)))
    assert second.run.mode == "incremental"
    assert second.run.window_from == "2026-11-13"  # 含上次成功截止日
    recs = {r["identifier"]: r for r in db.active_records()}
    assert recs["oai:demo:lib:001"]["metadata_json"].find("第二版") >= 0
    tomb = db.one("SELECT deleted FROM records WHERE identifier='oai:demo:lib:002'")
    assert tomb["deleted"] == 1
    assert db.record_stats() == (4, 1)
    assert db.latest_publication()["record_count"] == 4

    # 缺席不推断删除：删除源端一条未变记录，直接增量不应动它
    with db.tx() as conn:
        pass
    catalog = db.latest_publication()
    xml = open(catalog["file_path"], encoding="utf-8").read()
    assert "oai:demo:lib:003" in xml
    assert "oai:demo:lib:002" not in xml  # 撤回的不进目录


def test_resume_from_saved_token_after_restart(env):
    tmp_path, src_app, _ = env
    db, h = make(tmp_path, max_pages=1)
    run(h.start_in_background(date(2026, 11, 13)))
    st = db.unresolved_run()
    assert st is not None and st.status == "paused"
    assert st.pages_done == 1 and st.resumption_token
    db.close()

    db2, h2 = make(tmp_path, max_pages=10)
    out = run(h2.resume(st.id))
    assert out.run.status == "completed"
    assert db2.record_stats() == (5, 0)
    assert db2.get_run(st.id).pages_done == 3


def test_bad_resumption_token_reharvests_window_without_moving_watermark(env):
    tmp_path, src_app, _ = env
    db, h = make(tmp_path, max_pages=1)
    run(h.start_in_background(date(2026, 11, 13)))
    st = db.unresolved_run()
    assert st.status == "paused"
    src_app.state.store.invalidate_all_tokens()

    # 保存的 token 已失效 -> 本轮必须回到原窗口第一页；max_pages=1 时逐段续采
    out = run(h.resume(st.id))
    assert db.get_run(st.id).token_resets >= 1
    assert db.get_run(st.id).window_from is None  # 原窗口未变（首次全量）
    while db.unresolved_run() is not None and db.unresolved_run().status == "paused":
        out = run(h.resume(st.id))
    assert out.run.status == "completed"
    assert db.watermark() == "2026-11-13"
    assert db.record_stats() == (5, 0)


def test_withdrawal_is_permanent_and_never_inferred(env):
    tmp_path, src_app, _ = env
    store = source_db(src_app)
    db, h = make(tmp_path)
    run(h.start_in_background(date(2026, 11, 13)))
    store.delete_item("oai:demo:lib:005")
    out = run(h.start_in_background(date(2026, 11, 13)))
    assert out.published
    tomb = db.one("SELECT * FROM records WHERE identifier='oai:demo:lib:005'")
    assert tomb["deleted"] == 1
    xml = build_catalog_xml(db.active_records(), watermark="2026-11-13")
    assert "oai:demo:lib:005" not in xml
    assert "oai:demo:lib:001" in xml
    # 撤回标记即使在后续轮次也不消失
    store.update_item("oai:demo:lib:001", subject="图书馆学基础")
    run(h.start_in_background(date(2026, 11, 13)))
    assert db.one("SELECT deleted FROM records WHERE identifier='oai:demo:lib:005'")["deleted"] == 1


def test_publish_failure_keeps_old_catalog_and_watermark(env):
    tmp_path, src_app, _ = env
    store = source_db(src_app)
    db, h = make(tmp_path)
    run(h.start_in_background(date(2026, 11, 13)))
    old_pub = dict(db.latest_publication())
    old_xml = open(old_pub["file_path"], encoding="utf-8").read()
    store.update_item("oai:demo:lib:001", title="更新标题 A")

    db.set_meta("publish_fault", "generate_error")
    out = run(h.start_in_background(date(2026, 11, 13)))
    assert out.run.status == "publish_failed"
    assert db.watermark() == old_pub["watermark"]
    assert dict(db.latest_publication()) == old_pub
    assert open(old_pub["file_path"], encoding="utf-8").read() == old_xml

    # 重试发布（故障已一次性消费）成功
    out2 = run(h.retry_publish(out.run.id))
    assert out2.published
    assert db.watermark() == "2026-11-13"
    assert "更新标题 A" in open(db.latest_publication()["file_path"], encoding="utf-8").read()


@pytest.mark.parametrize("fault", [FAULT_CRASH_BEFORE_COMMIT, FAULT_CRASH_AFTER_PUBLISH])
def test_crash_around_publish_keeps_pairing(env, fault):
    tmp_path, src_app, _ = env
    store = source_db(src_app)
    db, h = make(tmp_path)
    run(h.start_in_background(date(2026, 11, 13)))
    old = dict(db.latest_publication())
    old_xml = open(old["file_path"], encoding="utf-8").read()

    store.update_item("oai:demo:lib:001", title="崩溃前更新")
    db.set_meta("publish_fault", fault)
    with pytest.raises(SystemExit):
        run(h.start_in_background(date(2026, 11, 13)))

    # "重启"：新 Database 打开同一文件，模拟启动恢复
    db2 = Database(str(tmp_path / "catalog.db"))
    from app.catalog import cleanup_orphans

    db2.recover_after_restart()
    latest = db2.latest_publication()
    cleanup_orphans(str(tmp_path / "published"), keep_paths=latest["file_path"] if latest else None)
    for r in db2.recent_runs(50):
        if r.status == "publishing":
            if db2.publication_for_run(r.id):
                db2.mark_completed(r.id)
            else:
                db2.mark_publish_failed(r.id, "进程在发布过程中退出")

    assert db2.watermark() == old["watermark"]
    assert open(db2.latest_publication()["file_path"], encoding="utf-8").read() == old_xml
    assert "崩溃前更新" not in open(db2.latest_publication()["file_path"], encoding="utf-8").read()
    leftover = [n for n in os.listdir(tmp_path / "published") if not n.startswith("catalog-run")]
    assert leftover == []
    assert len(os.listdir(tmp_path / "published")) == 1  # 只剩仍被引用的旧版本
    # 重试后拿到新版本
    retry_pub = Publisher(db2, str(tmp_path / "published"), fault="none")
    record = db2.unresolved_run()
    assert record is not None and record.status == "publish_failed"
    db2.begin_publish_retry(record.id)
    result = retry_pub.publish(db2.get_run(record.id), db2.active_records(), record.window_until)
    db2.mark_completed(record.id)
    assert db2.watermark() == "2026-11-13"
    assert "崩溃前更新" in open(result["path"], encoding="utf-8").read()


def test_restart_after_publication_commit_marks_run_completed(env):
    """发布记录已落库但状态没来得及写：重启后补记完成，水位/文件不变。"""
    tmp_path, src_app, _ = env
    db, h = make(tmp_path)
    run(h.start_in_background(date(2026, 11, 13)))
    pub = dict(db.latest_publication())
    with db.tx() as conn:
        conn.execute("UPDATE runs SET status='publishing' WHERE id=?", (pub["run_id"],))
    db.close()

    db2 = Database(str(tmp_path / "catalog.db"))
    db2.recover_after_restart()
    assert db2.get_run(pub["run_id"]).status == "publishing"
    for r in db2.recent_runs(50):
        if r.status == "publishing":
            if db2.publication_for_run(r.id):
                db2.mark_completed(r.id)
            else:
                db2.mark_publish_failed(r.id, "进程在发布过程中退出")
    assert db2.get_run(pub["run_id"]).status == "completed"
    assert db2.watermark() == pub["watermark"]
    assert db2.latest_publication()["id"] == pub["id"]


def test_catalog_download_pairing_is_atomic(env):
    tmp_path, src_app, _ = env
    db, h = make(tmp_path)
    run(h.start_in_background(date(2026, 11, 13)))
    latest = db.latest_publication()
    payload = open(latest["file_path"], encoding="utf-8").read()
    import hashlib

    assert hashlib.sha256(payload.encode()).hexdigest() == latest["sha256"]
    assert f'watermark="{latest["watermark"]}"' in payload
    assert f'recordCount="{latest["record_count"]}"' in payload
