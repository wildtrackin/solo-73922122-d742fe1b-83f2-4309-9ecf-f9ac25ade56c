"""历史目录回收：固定目标快照、下载租约保护、断点续办。

规则
----
* 目标集在回收启动时固定：当时已登记、且排在最近 keep 次发布之前的发布
  （文件 + 发布登记一起清）；之后的新发布与正在生成的文件绝不进入本次回收。
  源记录、轮次与当前水位始终保留。
* 下载租约：受理下载即对文件加租约，响应发送完毕或断连收尾后才释放；
  回收先把路径标记为"删除中"（拒绝新租约），再等现有租约归零才删文件。
  等待与删除都不持 SQLite 写锁，采集/发布/当前目录下载照常进行。
* 断点：每个目标分两步落库（文件已删 → 登记已清）。删除失败或进程在
  两步之间退出，回收保持未完成；重启自动续办，重复启动沿用同次回收，
  全部目标清完才报完成。
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import threading

FAULT_NONE = "none"
FAULT_DELETE_ERROR = "delete_error"                      # 删除文件时报错 → 保持未完成，可续办
FAULT_CRASH_AFTER_FILE_DELETE = "crash_after_file_delete"  # 文件已删、登记未清时进程退出

KNOWN_RECLAIM_FAULTS = {FAULT_NONE, FAULT_DELETE_ERROR, FAULT_CRASH_AFTER_FILE_DELETE}


class FileLeaseRegistry:
    """下载文件租约：回收删除前必须等租约归零；删除中的路径拒绝新租约。

    租约计数与"删除中"标记在同一把锁下变更，因此下载受理与回收交错时：
    * 先拿到租约的下载一定完整发完（回收看到计数 > 0，等待）；
    * 回收先标记删除中的路径，下载受理会失败并重新解析最新发布，
      不会读到删了一半的文件，也不会混版。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}
        self._deleting: set[str] = set()

    @staticmethod
    def _key(path: str) -> str:
        return os.path.abspath(path)

    def try_acquire(self, path: str) -> bool:
        """受理下载：路径未被回收标记删除中时，租约 +1 并返回 True。"""
        key = self._key(path)
        with self._lock:
            if key in self._deleting:
                return False
            self._counts[key] = self._counts.get(key, 0) + 1
            return True

    def release(self, path: str) -> None:
        """响应发送完毕或断连收尾后调用。"""
        key = self._key(path)
        with self._lock:
            n = self._counts.get(key, 0) - 1
            if n <= 0:
                self._counts.pop(key, None)
            else:
                self._counts[key] = n

    def count(self, path: str) -> int:
        with self._lock:
            return self._counts.get(self._key(path), 0)

    def claim_for_deletion(self, path: str) -> None:
        """回收声明删除意图：此后该路径拒绝新租约（已持有的租约不受影响）。"""
        with self._lock:
            self._deleting.add(self._key(path))

    def unclaim(self, path: str) -> None:
        with self._lock:
            self._deleting.discard(self._key(path))

    def claimed(self) -> dict[str, int]:
        """当前处于"删除中"的绝对路径 → 进行中的下载数（页面展示用）。"""
        with self._lock:
            return {key: self._counts.get(key, 0) for key in self._deleting}


class Reclaimer:
    """执行一次回收：等下载收尾 → 删文件 → 删登记；失败即停，保持未完成。"""

    def __init__(
        self,
        db,
        leases: FileLeaseRegistry,
        *,
        keep: int = 2,
        wait_tick: float = 0.2,
        exit_process=None,
    ):
        self.db = db
        self.leases = leases
        self.keep = keep
        self.wait_tick = wait_tick
        self.exit_process = exit_process or os._exit

    def start_or_reuse(self):
        """返回 (run, created)。

        已有未完成的回收 → 沿用同次（created=False）；无可回收对象 → (None, False)。
        """
        existing = self.db.active_reclaim_run()
        if existing is not None:
            return existing, False
        try:
            run = self.db.create_reclaim_run(keep=self.keep)
        except sqlite3.IntegrityError:
            # 并发下已被他人创建：沿用同次
            return self.db.active_reclaim_run(), False
        if run is None:
            return None, False
        return run, True

    async def execute(self, reclaim_id: int) -> None:
        """依次清理全部目标；任一步失败即返回（回收保持未完成，等重启/手动续办）。"""
        while True:
            run = self.db.get_reclaim_run(reclaim_id)
            if run is None or run["status"] != "running":
                return
            target = self.db.next_pending_reclaim_target(reclaim_id)
            if target is None:
                self.db.complete_reclaim_run(reclaim_id)  # 全部目标清完才报完成
                return
            if not await self._process_target(target):
                return

    async def _process_target(self, target) -> bool:
        """处理一个目标；返回 False 表示本次回收应停下（出错/中断）。"""
        path = target["file_path"]
        self.leases.claim_for_deletion(path)
        try:
            # 已受理的下载收尾后才删：纯内存等待，不持 SQLite 写锁，
            # 采集、发布与当前目录下载照常进行。
            while self.leases.count(path) > 0:
                await asyncio.sleep(self.wait_tick)

            fault = self.db.take_reclaim_fault()
            if fault == FAULT_DELETE_ERROR:
                self.db.record_target_error(target["id"], "演示：删除文件失败（故障注入 delete_error）")
                return False
            try:
                os.remove(path)  # 删除文件同样不持 SQLite 写锁
            except FileNotFoundError:
                pass  # 上次中断前已删：视为成功，继续补登记
            except OSError as exc:
                self.db.record_target_error(target["id"], f"删除文件失败：{exc}")
                return False
            self.db.mark_target_file_deleted(target["id"])

            if fault == FAULT_CRASH_AFTER_FILE_DELETE:
                # 演示：清理文件与登记之间进程退出；重启后从这里自动续办
                self.exit_process(70)
                raise RuntimeError("演示进程未退出（exit hook 返回）")

            self.db.mark_target_row_deleted(target["id"], target["publication_id"])
            return True
        finally:
            self.leases.unclaim(path)
