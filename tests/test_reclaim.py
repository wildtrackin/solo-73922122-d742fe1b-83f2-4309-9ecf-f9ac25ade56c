"""回收网页行为测试：网页入口、进度显示、慢下载租约、断点续办、不碰新发布。

用真实 FastAPI 路由（TestClient）+ ASGI 直连的演示源，不依赖网络。
"""
from __future__ import annotations

import os
import sys
import threading
import time

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import oai as oai_mod
from source.app import create_source_app


@pytest.fixture()
def web(tmp_path, monkeypatch):
    src_app = create_source_app(str(tmp_path / "source.db"))
    transport = httpx.ASGITransport(app=src_app)

    async def patched_enter(self):
        if self._client is None:
            self._client = httpx.AsyncClient(transport=transport, base_url="http://source")
        return self

    monkeypatch.setattr(oai_mod.OAIHttpClient, "__aenter__", patched_enter)
    monkeypatch.setenv("CATALOG_DATA_DIR", str(tmp_path / "web"))
    monkeypatch.setenv("OAI_ENDPOINT", "http://source/oai")
    for var in ("PUBLISH_FAILURE_MODE", "MAX_PAGES_PER_CALL", "RECLAIM_FAILURE_MODE", "DOWNLOAD_THROTTLE_BPS"):
        monkeypatch.delenv(var, raising=False)

    import importlib

    import app.main as main_mod

    importlib.reload(main_mod)
    app = main_mod.build_app()
    with TestClient(app) as client:
        yield client, app, src_app, main_mod


FINAL = {"completed", "paused", "publish_failed", "canceled"}


def harvest(client, app, until="2026-11-13", timeout=30.0):
    client.post("/harvest/start", data={"until": until})
    db = app.state.db
    deadline = time.time() + timeout
    while time.time() < deadline:
        runs = db.recent_runs(1)
        if runs and runs[0].status in FINAL:
            return runs[0]
        time.sleep(0.05)
    raise TimeoutError("harvest did not settle")


def publish_rounds(client, app, src_app, n):
    for i in range(n):
        src_app.state.store.update_item("oai:demo:lib:001", title=f"标题 {time.time_ns()} {i}")
        run = harvest(client, app)
        assert run.status == "completed"


def wait_reclaim_done(app, timeout=30.0):
    db = app.state.db
    deadline = time.time() + timeout
    while time.time() < deadline:
        if db.active_reclaim_run() is None:
            return
        time.sleep(0.05)
    raise TimeoutError("reclaim did not complete")


def test_reclaim_keeps_two_newest_and_preserves_core_state(web):
    client, app, src_app, _ = web
    publish_rounds(client, app, src_app, 4)
    db = app.state.db
    pubs = db.publications(10)  # 新 → 旧
    assert len(pubs) == 4
    paths = {p["id"]: p["file_path"] for p in pubs}
    wm_before = db.watermark()
    runs_before = len(db.recent_runs(50))

    r = client.post("/reclaim/start", follow_redirects=True)
    assert r.status_code == 200
    wait_reclaim_done(app)

    run = db.latest_reclaim_run()
    assert run["status"] == "completed"
    left = db.publications(10)
    assert [p["id"] for p in left] == [pubs[0]["id"], pubs[1]["id"]]  # 最近两次保留
    for p in pubs[:2]:
        assert os.path.exists(paths[p["id"]])
    for p in pubs[2:]:
        assert not os.path.exists(paths[p["id"]])  # 文件已回收
    targets = db.reclaim_targets(run["id"])
    assert len(targets) == 2 and all(t["row_deleted_at"] for t in targets)

    # 源记录、轮次与当前水位保留
    assert db.watermark() == wm_before
    assert db.record_stats() == (5, 0)
    assert len(db.recent_runs(50)) == runs_before

    # 当前目录照常下载
    dl = client.get("/catalog/download")
    assert dl.status_code == 200
    assert dl.headers["x-catalog-watermark"] == wm_before
    assert dl.text.count("<record ") == 5


def test_reclaim_delete_error_shows_incomplete_and_reused_on_restart(web):
    client, app, src_app, _ = web
    publish_rounds(client, app, src_app, 3)
    db = app.state.db
    db.set_meta("reclaim_fault", "delete_error")

    client.post("/reclaim/start", follow_redirects=True)
    deadline = time.time() + 30
    while time.time() < deadline:
        run = db.active_reclaim_run()
        if run and db.reclaim_progress(run["id"])["errors"] >= 1:
            break
        time.sleep(0.05)

    run = db.active_reclaim_run()
    assert run is not None, "删除失败必须保持未完成"
    prog = db.reclaim_progress(run["id"])
    assert prog["errors"] == 1 and prog["done"] == 0
    targets = db.reclaim_targets(run["id"])
    assert len(targets) == 1
    assert os.path.exists(targets[0]["file_path"])  # 删除失败：文件仍在
    html = client.get("/").text
    assert "未完成" in html and "delete_error" in html

    # 重复启动沿用同次回收（故障一次性已消费，这次顺利清完）
    r = client.post("/reclaim/start", follow_redirects=True)
    assert "沿用同次" in r.text
    wait_reclaim_done(app)
    run2 = db.latest_reclaim_run()
    assert run2["id"] == run["id"] and run2["status"] == "completed"
    assert len(db.publications(10)) == 2


def test_reclaim_crash_between_file_and_registration_auto_resumes(web):
    client, app, src_app, main_mod = web
    publish_rounds(client, app, src_app, 3)
    db = app.state.db
    pubs = db.publications(10)
    target_path = pubs[-1]["file_path"]  # 最旧的发布是唯一目标

    class SimulatedExit(Exception):
        pass

    app.state.reclaimer.exit_process = lambda code: (_ for _ in ()).throw(SimulatedExit(code))
    db.set_meta("reclaim_fault", "crash_after_file_delete")
    client.post("/reclaim/start", follow_redirects=True)

    # 等"崩溃"发生：文件已删、登记未清、回收未完成
    deadline = time.time() + 30
    while time.time() < deadline:
        run = db.active_reclaim_run()
        if run:
            target = db.reclaim_targets(run["id"])[0]
            if target["file_deleted_at"] and not target["row_deleted_at"]:
                break
        time.sleep(0.05)
    run = db.active_reclaim_run()
    assert run is not None, "清理文件与登记之间退出必须保持未完成"
    target = db.reclaim_targets(run["id"])[0]
    assert not os.path.exists(target_path)  # 文件已删
    assert db.one("SELECT * FROM publications WHERE id = ?", target["publication_id"]) is not None  # 登记未清
    assert "未完成" in client.get("/").text

    # 模拟重启：新应用实例，启动即自动续办同一回收
    app2 = main_mod.build_app()
    with TestClient(app2) as client2:
        wait_reclaim_done(app2)
        db2 = app2.state.db
        run2 = db2.latest_reclaim_run()
        assert run2["id"] == run["id"] and run2["status"] == "completed"
        assert db2.reclaim_targets(run["id"])[0]["row_deleted_at"]
        assert db2.one("SELECT * FROM publications WHERE id = ?", target["publication_id"]) is None
        assert len(db2.publications(10)) == 2
        assert client2.get("/catalog/download").status_code == 200


def test_reclaim_waits_for_inflight_download_and_skips_new_publications(web):
    client, app, src_app, _ = web
    publish_rounds(client, app, src_app, 3)
    db = app.state.db
    pubs = db.publications(10)
    leased_path = pubs[0]["file_path"]  # 当前最新（即将被慢速下载）
    leased_pub_id = pubs[0]["id"]
    wm = pubs[0]["watermark"]

    downloaded: dict = {}

    def slow_download():
        with client.stream("GET", "/catalog/download?throttle=256") as resp:
            downloaded["status"] = resp.status_code
            downloaded["watermark"] = resp.headers.get("x-catalog-watermark")
            downloaded["body"] = b"".join(resp.iter_bytes())

    dl_thread = threading.Thread(target=slow_download)
    dl_thread.start()
    # 等下载被受理（租约生效）
    leases = app.state.leases
    deadline = time.time() + 10
    while leases.count(leased_path) == 0:
        assert time.time() < deadline, "下载租约未生效"
        time.sleep(0.05)

    # 慢下载期间连续发布两轮 → 被下载的文件排到最近两次之前，成为回收对象
    publish_rounds(client, app, src_app, 2)
    assert len(db.publications(10)) == 5

    client.post("/reclaim/start", follow_redirects=True)
    run = db.active_reclaim_run()
    assert run is not None
    assert db.reclaim_progress(run["id"])["total"] == 3  # 5 - 保留2

    # 回收等待期间再发布一轮：新发布绝不进入本次回收
    publish_rounds(client, app, src_app, 1)
    newest = db.latest_publication()

    # 下载未收尾：目标文件不得被删；当前目录照常下载
    assert os.path.exists(leased_path)
    assert leases.count(leased_path) == 1
    assert client.get("/catalog/download").status_code == 200

    dl_thread.join(timeout=60)
    assert downloaded["status"] == 200
    assert downloaded["watermark"] == wm  # 固定受理那一刻的版本
    assert downloaded["body"].count(b"<record ") == 5  # 完整文件
    assert f'watermark="{wm}"'.encode() in downloaded["body"]

    wait_reclaim_done(app)
    left = db.publications(10)
    left_ids = [p["id"] for p in left]
    assert leased_pub_id not in left_ids  # 目标已清
    assert not os.path.exists(leased_path)
    assert newest["id"] in left_ids  # 回收期间的新发布保留
    assert len(left) == 3  # 保留2 + 回收期间新发布1
    assert db.watermark() == newest["watermark"]


def test_reclaim_with_insufficient_publications_is_noop(web):
    client, app, src_app, _ = web
    publish_rounds(client, app, src_app, 2)
    r = client.post("/reclaim/start", follow_redirects=True)
    assert "没有可回收" in r.text
    db = app.state.db
    assert db.latest_reclaim_run() is None
    assert len(db.publications(10)) == 2


def test_api_overview_reports_reclaim_progress(web):
    client, app, src_app, _ = web
    publish_rounds(client, app, src_app, 3)
    client.post("/reclaim/start", follow_redirects=True)
    wait_reclaim_done(app)
    ov = client.get("/api/overview").json()
    assert ov["reclaim"]["status"] == "completed"
    prog = ov["reclaim"]["progress"]
    assert prog["done"] == prog["total"] == 1
    assert prog["freed_bytes"] == prog["total_bytes"] > 0
    assert len(ov["publications"]) == 2
    assert ov["watermark"]
