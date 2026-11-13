"""带租约的文件下载响应：响应发送完毕或断连收尾后才释放租约。

回收线程只有在租约归零后才会删除对应文件，因此已受理的下载一定拿到
"受理那一刻"的完整文件，即使该文件随后成为回收对象。
"""
from __future__ import annotations

import asyncio
import os

from starlette.responses import Response


class LeasedFileResponse(Response):
    """流式发送已加租约的文件；finally 中释放租约（含客户端断连/任务取消）。

    租约必须在构造本响应前由调用方通过 FileLeaseRegistry.try_acquire 取得；
    若构造抛错（如文件意外缺失），调用方负责立即释放租约。
    """

    media_type = "application/xml"

    def __init__(
        self,
        path: str,
        *,
        leases,
        filename: str,
        extra_headers: dict | None = None,
        throttle_bps: int = 0,
        chunk_size: int = 65536,
    ):
        self.path = path
        self.leases = leases
        self.throttle_bps = max(0, int(throttle_bps or 0))
        if self.throttle_bps:
            # 限速时缩小分块，让发送节奏平滑（约每秒 5 块）
            chunk_size = min(chunk_size, max(1024, self.throttle_bps // 5))
        self.chunk_size = chunk_size
        size = os.stat(path).st_size  # 租约保护下文件必然存在；缺失则抛给调用方
        headers = {
            "content-length": str(size),
            "content-disposition": f'attachment; filename="{filename}"',
        }
        headers.update(extra_headers or {})
        super().__init__(content=b"", headers=headers, media_type=self.media_type)

    async def __call__(self, scope, receive, send):
        try:
            await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
            with open(self.path, "rb") as fh:
                while True:
                    chunk = fh.read(self.chunk_size)
                    if not chunk:
                        break
                    await send({"type": "http.response.body", "body": chunk, "more_body": True})
                    if self.throttle_bps:
                        await asyncio.sleep(len(chunk) / self.throttle_bps)
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        finally:
            # 发送完毕、客户端断连或任务取消都会走到这里，回收据此判断"收尾完成"
            self.leases.release(self.path)
