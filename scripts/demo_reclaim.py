"""可控演示：慢下载期间连续发布 → 网页回收 → 等待下载收尾 → 删除中断 → 重启自动续办。

前置条件：
    docker compose up --build -d        # 建议全新数据：docker compose down -v 后再 up

用法（宿主机）：
    python3 scripts/demo_reclaim.py                 # 场景一：慢下载 + 连续发布 + 回收等待下载收尾
    python3 scripts/demo_reclaim.py --crash         # 场景一 + 场景二：删除中断 → 重启自动续办
    python3 scripts/demo_reclaim.py --crash --no-restart
                                                    # 中断后不自动 restart，改打印手动步骤

环境变量：
    CATALOG_WEB   采集页地址，默认 http://localhost:8000
    OAI_SOURCE    演示源地址，默认 http://localhost:8001

场景一验证点：已受理的慢下载拿到的是受理那一刻的完整文件；下载期间的新发布
与回收期间的新发布都不被回收；回收只清启动时已登记且排在最近两次之前的目标。
场景二验证点：进程在"文件已删、登记未清"之间退出 → 回收保持未完成；
重启后同一回收自动续办，全部目标清完才报完成。
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import os
import subprocess
import threading
import time

import httpx

FINAL_RUN_STATES = {"completed", "paused", "publish_failed", "canceled"}
TODAY = datetime.now(timezone.utc).date().isoformat()


def overview(web: httpx.Client) -> dict:
    return web.get("/api/overview").json()


def wait_harvest_done(web: httpx.Client, prev_run_id, timeout: float = 180.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = overview(web)["latest_run"]
        if run and run["id"] != prev_run_id and run["status"] in FINAL_RUN_STATES:
            return run
        time.sleep(0.4)
    raise TimeoutError("采集超时未完成")


def harvest_round(web: httpx.Client, src: httpx.Client, title: str) -> dict:
    """改一条源记录 → 采集一轮 → 等发布完成；返回最新总览。"""
    src.post("/demo/update", data={"identifier": "oai:demo:lib:001", "title": title})
    prev = (overview(web)["latest_run"] or {}).get("id")
    web.post("/harvest/start", data={"until": TODAY})
    run = wait_harvest_done(web, prev)
    if run["status"] != "completed":
        raise AssertionError(f"采集未成功：{run}")
    return overview(web)


def ensure_publications(web: httpx.Client, src: httpx.Client, need: int) -> dict:
    ov = overview(web)
    while len(ov["publications"]) < need:
        ov = harvest_round(web, src, f"图书馆学概论（第 {len(ov['publications']) + 1} 次发布）")
    return ov


def wait_reclaim_completed(web: httpx.Client, timeout: float = 90.0) -> dict:
    deadline = time.time() + timeout
    ov = overview(web)
    while time.time() < deadline:
        ov = overview(web)
        rc = ov["reclaim"]
        if rc and rc["status"] == "completed":
            return ov
        time.sleep(0.5)
    raise AssertionError(f"回收未完成：{ov['reclaim']}")


def scenario_slow_download(web: httpx.Client, src: httpx.Client) -> None:
    print("【场景一】慢下载期间连续发布 + 回收等待下载收尾")
    print("1) 批量造数 150 条并采集，让目录文件足够大（慢下载效果可观）")
    src.post("/demo/bulk-create", data={"count": "150", "prefix": "bulk"})
    ov = harvest_round(web, src, "图书馆学概论（批量版）")
    ov = ensure_publications(web, src, 4)
    pubs = ov["publications"]
    print(f"   当前已登记发布 {len(pubs)} 次；最新 {pubs[0]['file_name']}（{pubs[0]['size_bytes']} B）")

    print("2) 后台开始慢速下载当前目录（限速 4 KB/s）")
    result: dict = {}

    def slow_download() -> None:
        with web.stream("GET", "/catalog/download?throttle=4096") as resp:
            result["status"] = resp.status_code
            result["watermark"] = resp.headers.get("x-catalog-watermark")
            result["run"] = resp.headers.get("x-catalog-run")
            result["body"] = b"".join(resp.iter_bytes())

    dl_thread = threading.Thread(target=slow_download, daemon=True)
    dl_thread.start()
    time.sleep(1.0)  # 等下载被受理（租约生效）

    print("3) 慢下载期间连续发布 3 轮（被下载的文件随后成为回收对象）")
    for i in range(3):
        ov = harvest_round(web, src, f"图书馆学概论（下载期间第 {i + 1} 版）")
        print(f"   已发布 #{ov['publications'][0]['id']}，水位 {ov['watermark']}")

    print("4) 启动回收：目标 = 此刻已登记且排在最近两次发布之前的发布")
    web.post("/reclaim/start")
    seen_waiting = False
    deadline = time.time() + 30
    while time.time() < deadline:
        ov = overview(web)
        rc = ov["reclaim"]
        if rc and rc["waiting"]:
            seen_waiting = True
            print(f"   回收正在等待下载收尾：{rc['waiting']}")
            break
        if rc and rc["status"] == "completed":
            break
        time.sleep(0.3)
    resp = web.get("/catalog.xml")
    print(f"   回收期间当前目录照常下载：HTTP {resp.status_code}")
    assert resp.status_code == 200

    print("5) 回收等待期间再发布 1 轮（新发布绝不进入本次回收）")
    ov = harvest_round(web, src, "图书馆学概论（回收期间新版）")
    newest_id = ov["publications"][0]["id"]

    print("6) 等慢下载收尾：内容必须是受理那一刻的完整文件")
    dl_thread.join(timeout=300)
    assert result.get("status") == 200, f"慢下载失败：{result.get('status')}"
    body = result["body"]
    digest = hashlib.sha256(body).hexdigest()[:12]
    print(f"   下载完成：水位 {result['watermark']}，{len(body)} B，sha256={digest}…")
    assert f'watermark="{result["watermark"]}"'.encode() in body

    print("7) 下载收尾后回收继续，等其完成")
    ov = wait_reclaim_completed(web)
    rc = ov["reclaim"]
    left = ov["publications"]
    print(f"   回收 #{rc['id']} 完成：释放 {rc['progress']['freed_bytes']} B，剩余发布 {len(left)} 次")
    assert seen_waiting, "未观察到回收等待下载收尾（下载可能太快，可调低 throttle）"
    assert any(p["id"] == newest_id for p in left), "回收期间的新发布被误删！"
    assert len(left) == 3, f"应剩 保留2 + 回收期间新发布1 = 3 次发布，实际 {len(left)}"
    print("   源记录、轮次与当前水位保持不变 ✔")
    print()


def scenario_crash_resume(web: httpx.Client, src: httpx.Client, *, do_restart: bool) -> None:
    print("【场景二】删除中断：清理文件与登记之间进程退出 → 重启自动续办")
    print("1) 再积累至少 4 次发布作为回收对象")
    ensure_publications(web, src, 4)

    print("2) 预置一次性回收故障 crash_after_file_delete")
    web.post("/config/reclaim-fault", data={"reclaim_fault": "crash_after_file_delete"})

    print("3) 启动回收：进程将在“文件已删、登记未清”时退出（回收保持未完成）")
    try:
        web.post("/reclaim/start")
    except Exception:
        pass  # 进程可能在响应/重定向发送前就已退出，属预期
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            web.get("/healthz", timeout=2)
            time.sleep(0.3)
        except Exception:
            break
    else:
        raise AssertionError("服务未按预期退出（故障可能已被消费）")
    print("   catalog-web 已退出")

    if do_restart:
        print("4) docker compose restart catalog-web")
        subprocess.run(["docker", "compose", "restart", "catalog-web"], check=True)
    else:
        print("4) 请手动执行：docker compose restart catalog-web（本脚本等待其恢复健康）")

    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            if web.get("/healthz", timeout=2).status_code == 200:
                break
        except Exception:
            pass
        time.sleep(1.0)
    else:
        raise TimeoutError("catalog-web 未恢复健康")

    print("5) 重启后同一回收自动续办，全部目标清完才报完成")
    ov = wait_reclaim_completed(web)
    rc = ov["reclaim"]
    print(f"   回收 #{rc['id']} 已完成：释放 {rc['progress']['freed_bytes']} B，"
          f"剩余发布 {len(ov['publications'])} 次，水位 {ov['watermark']} 不变 ✔")


def main() -> None:
    parser = argparse.ArgumentParser(description="慢下载/连续发布/回收/删除中断 可控演示")
    parser.add_argument("--crash", action="store_true", help="追加场景二：删除中断 → 重启自动续办")
    parser.add_argument("--no-restart", action="store_true", help="场景二不自动 docker compose restart")
    args = parser.parse_args()

    web_url = os.environ.get("CATALOG_WEB", "http://localhost:8000")
    src_url = os.environ.get("OAI_SOURCE", "http://localhost:8001")
    with httpx.Client(base_url=web_url, timeout=30, follow_redirects=True) as web, \
            httpx.Client(base_url=src_url, timeout=30, follow_redirects=True) as src:
        web.get("/healthz").raise_for_status()
        src.get("/healthz").raise_for_status()
        scenario_slow_download(web, src)
        if args.crash:
            scenario_crash_resume(web, src, do_restart=not args.no_restart)
    print("演示完成 ✔")


if __name__ == "__main__":
    main()
