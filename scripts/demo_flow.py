"""命令行演示脚本（可选）：无需浏览器即可跑通同日更新、撤回与续采。

用法（在容器内）：
    python -m scripts.demo_flow

它直接调用 HTTP 接口，等价于在页面上点击。
"""
from __future__ import annotations

import time

import httpx

WEB = "http://catalog-web:8000"
SOURCE = "http://oai-source:8001"


def _top_section(html: str) -> str:
    """只取“本轮采集”区域，避免把历史表格里的状态当成当前轮次。"""
    body = html.split("当前发布", 1)[0]
    return body.split("本轮采集", 1)[-1]


def wait_idle(client: httpx.Client, timeout: float = 60.0) -> str:
    deadline = time.time() + timeout
    html = client.get("/").text
    while time.time() < deadline:
        html = client.get("/").text
        top = _top_section(html)
        if "已完成" in top or "发布失败" in top or "已暂停" in top:
            return html
        time.sleep(0.5)
    return html


def start(client: httpx.Client, until: str) -> None:
    client.post("/harvest/start", data={"until": until}, follow_redirects=True)
    wait_idle(client)


def download(client: httpx.Client):
    r = client.get("/catalog/download")
    r.raise_for_status()
    return r.headers.get("x-catalog-watermark"), r.text


def main() -> None:
    with httpx.Client(base_url=WEB, timeout=30, follow_redirects=True) as web, \
            httpx.Client(base_url=SOURCE, timeout=30, follow_redirects=True) as src:
        print("1) 首次全量")
        start(web, "2026-11-13")
        wm, xml = download(web)
        print(f"   水位={wm} 记录数={xml.count('<record ')}")

        print("2) 同日更新 001、撤回 002")
        src.post("/demo/update", data={"identifier": "oai:demo:lib:001", "title": "图书馆学概论（第二版）"})
        src.post("/demo/delete", data={"identifier": "oai:demo:lib:002"})
        start(web, "2026-11-13")
        wm, xml = download(web)
        print(f"   水位={wm} 记录数={xml.count('<record ')} 含第二版={'第二版' in xml} 含002={'oai:demo:lib:002' in xml}")
        assert "第二版" in xml and "oai:demo:lib:002" not in xml

        print("3) 下一次续页凭据失效，自动回到原窗口第一页重采")
        src.post("/demo/trip-token")
        start(web, "2026-11-13")
        wm, xml = download(web)
        print(f"   水位={wm} 记录数={xml.count('<record ')} 仍含第二版={'第二版' in xml}")
        assert "第二版" in xml


if __name__ == "__main__":
    main()
