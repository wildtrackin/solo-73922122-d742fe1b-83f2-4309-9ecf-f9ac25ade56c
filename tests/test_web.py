"""网页端行为测试：后台采集、进度/错误显示、下载与水位成对。

用真实 FastAPI 路由（TestClient）+ ASGI 直连的演示源，不依赖网络。
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date

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
    monkeypatch.delenv("PUBLISH_FAILURE_MODE", raising=False)
    monkeypatch.delenv("MAX_PAGES_PER_CALL", raising=False)

    import importlib

    import app.main as main_mod

    importlib.reload(main_mod)
    app = main_mod.build_app()
    with TestClient(app) as client:
        yield client, app, src_app


FINAL = {"completed", "paused", "publish_failed", "canceled"}


def _wait(client: TestClient, timeout: float = 30.0) -> str:
    """等待最近一轮进入终态/暂停（直接看 DB 判定比页面轮询可靠）。"""
    db = client.app.state.db
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = db.recent_runs(1)[0] if db.recent_runs(1) else None
        if run is not None and run.status in FINAL:
            break
        time.sleep(0.1)
    return client.get("/").text


def test_home_renders(web):
    client, app, _ = web
    r = client.get("/")
    assert r.status_code == 200
    assert "本轮截止日" in r.text


def test_background_harvest_publishes_and_download_pairs(web):
    client, app, src_app = web
    r = client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    assert r.status_code == 200
    html = _wait(client)
    assert "已完成" in html

    db = app.state.db
    pub = db.latest_publication()
    dl = client.get("/catalog/download")
    assert dl.status_code == 200
    assert dl.headers["x-catalog-watermark"] == pub["watermark"]
    assert int(dl.headers["x-catalog-run"]) == pub["run_id"]
    assert f'watermark="{pub["watermark"]}"' in dl.text
    assert dl.text.count("<record ") == pub["record_count"]
    assert "oai:demo:lib:001" in dl.text

    # 新一轮：同日更新 + 撤回
    src_app.state.store.update_item("oai:demo:lib:001", title="网页测试改名")
    src_app.state.store.delete_item("oai:demo:lib/003".replace("/", ":"))
    client.post("/harvest/start", data={"until": "2026-11-13"})
    html = _wait(client)
    assert "已完成" in html
    pub2 = db.latest_publication()
    assert pub2["id"] != pub["id"]
    dl2 = client.get("/catalog/download")
    assert "网页测试改名" in dl2.text
    assert "oai:demo:lib:003" not in dl2.text
    assert db.watermark() == "2026-11-13"


def test_download_available_during_harvest_and_progress_visible(web, monkeypatch):
    client, app, src_app = web
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait(client)
    first = app.state.db.latest_publication()

    # 让源端第一页响应变慢，采集中时下载仍可用
    from source.app import SourceStore

    orig = SourceStore._all

    def slow(self, sql, *params):
        if "FROM items" in sql:
            time.sleep(0.4)
        return orig(self, sql, *params)

    monkeypatch.setattr(SourceStore, "_all", slow)
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    r = client.get("/")
    # 采集中：页面有进度，旧目录可下载
    assert "采集中" in r.text or "发布中" in r.text or "已完成" in r.text
    dl = client.get("/catalog/download")
    assert dl.status_code == 200
    assert int(dl.headers["x-catalog-run"]) == first["run_id"]
    _wait(client)


def test_publish_failure_shows_retry_and_keeps_old_catalog(web):
    client, app, src_app = web
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    _wait(client)
    db = app.state.db
    old = db.latest_publication()
    old_xml = client.get("/catalog/download").text

    src_app.state.store.update_item("oai:demo:lib:001", title="发布失败改名")
    db.set_meta("publish_fault", "generate_error")
    client.post("/harvest/start", data={"until": "2026-11-13"}, follow_redirects=True)
    deadline = time.time() + 30
    while time.time() < deadline and db.recent_runs(1)[0].status != "publish_failed":
        time.sleep(0.1)
    html = _wait(client)
    assert "发布失败" in html
    assert "/retry-publish" in html
    assert db.latest_publication()["id"] == old["id"]
    assert client.get("/catalog/download").text == old_xml
    assert "发布失败改名" not in old_xml

    run = db.recent_runs(1)[0]
    assert run.status == "publish_failed"
    client.post(f"/harvest/{run.id}/retry-publish", follow_redirects=True)
    deadline = time.time() + 30
    while time.time() < deadline and db.get_run(run.id).status != "completed":
        time.sleep(0.1)
    assert db.get_run(run.id).status == "completed"
    assert "发布失败改名" in client.get("/catalog/download").text


def test_paused_run_has_resume_button(web):
    client, app, src_app = web
    # 用故障注入让采集立刻中断：源端返回 500？这里直接暂停 run
    db = app.state.db
    run = db.create_run("full", None, date(2026, 11, 13))
    db.pause_run(run.id, "interrupted", "测试中断")
    html = client.get("/").text
    assert "已暂停·可续采" in html
    assert f"/harvest/{run.id}/resume" in html
    client.post(f"/harvest/{run.id}/resume", follow_redirects=True)
    deadline = time.time() + 30
    while time.time() < deadline and app.state.db.get_run(run.id).status != "completed":
        time.sleep(0.1)
    assert app.state.db.get_run(run.id).status == "completed"
    assert "已完成" in client.get("/").text
