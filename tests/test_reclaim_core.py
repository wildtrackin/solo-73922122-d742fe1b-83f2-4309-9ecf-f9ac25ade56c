"""回收核心逻辑测试（纯标准库）：目标快照、断点续办、租约等待、不碰新发布。

不依赖 FastAPI/httpx，直接驱动 Database + Reclaimer + FileLeaseRegistry。
"""
from __future__ import annotations

import asyncio
from datetime import date
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import Database
from app.reclaim import FileLeaseRegistry, Reclaimer


def _publish(db: Database, tmp_path, seq: int, watermark: str = "2026-11-13") -> str:
    """造一轮已完成轮次 + 一条发布登记 + 真实文件，返回文件路径。"""
    run = db.create_run("full" if seq == 0 else "incremental", None, date(2026, 11, 13))
    db.complete_harvest(run.id)
    db.mark_completed(run.id)
    out = tmp_path / "published"
    os.makedirs(out, exist_ok=True)
    name = f"catalog-run{run.id}-{seq}.xml"
    path = os.path.abspath(str(out / name))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"<catalog watermark='{watermark}' seq='{seq}'/>")
    db.record_publication(
        run_id=run.id,
        watermark=watermark,
        file_path=path,
        file_name=name,
        record_count=1,
        size_bytes=os.path.getsize(path),
        sha256=f"{seq:064d}",
    )
    return path


def test_snapshot_fixed_at_start_and_new_publications_untouched(tmp_path):
    db = Database(str(tmp_path / "catalog.db"))
    paths = [_publish(db, tmp_path, i) for i in range(4)]
    rec = Reclaimer(db, FileLeaseRegistry(), keep=2)

    run, created = rec.start_or_reuse()
    assert created and run is not None
    # 快照之后再发布的绝不进入本次回收
    new_path = _publish(db, tmp_path, 99)

    asyncio.run(rec.execute(run["id"]))

    assert db.latest_reclaim_run()["status"] == "completed"
    assert not os.path.exists(paths[0]) and not os.path.exists(paths[1])  # 目标已清
    assert os.path.exists(paths[2]) and os.path.exists(paths[3])          # 最近两次保留
    assert os.path.exists(new_path)                                       # 新发布保留
    left = db.publications(10)
    assert len(left) == 3
    assert db.watermark() == "2026-11-13"                                 # 水位不动
    assert len(db.recent_runs(10)) == 5                                   # 轮次保留
    targets = db.reclaim_targets(run["id"])
    assert len(targets) == 2 and all(t["row_deleted_at"] for t in targets)


def test_start_or_reuse_returns_same_run(tmp_path):
    db = Database(str(tmp_path / "catalog.db"))
    for i in range(3):
        _publish(db, tmp_path, i)
    rec = Reclaimer(db, FileLeaseRegistry(), keep=2)
    run1, created1 = rec.start_or_reuse()
    run2, created2 = rec.start_or_reuse()
    assert created1 and not created2
    assert run1["id"] == run2["id"]


def test_no_targets_means_no_run(tmp_path):
    db = Database(str(tmp_path / "catalog.db"))
    for i in range(2):
        _publish(db, tmp_path, i)
    rec = Reclaimer(db, FileLeaseRegistry(), keep=2)
    run, created = rec.start_or_reuse()
    assert run is None and not created
    assert db.latest_reclaim_run() is None
    assert len(db.publications(10)) == 2


def test_crash_between_file_and_registration_resumes(tmp_path):
    db = Database(str(tmp_path / "catalog.db"))
    paths = [_publish(db, tmp_path, i) for i in range(3)]
    exits: list[int] = []
    rec = Reclaimer(db, FileLeaseRegistry(), keep=2, exit_process=lambda code: exits.append(code))
    db.set_meta("reclaim_fault", "crash_after_file_delete")

    run, _ = rec.start_or_reuse()
    # exit hook 只是记录（模拟进程退出），随后代码按“未退出”报错并停下
    try:
        asyncio.run(rec.execute(run["id"]))
        raise AssertionError("应当抛出 RuntimeError")
    except RuntimeError:
        pass
    assert exits == [70]

    # 中间态：文件已删、登记未清、回收未完成
    assert db.active_reclaim_run() is not None
    target = db.reclaim_targets(run["id"])[0]
    assert target["file_deleted_at"] and not target["row_deleted_at"]
    assert not os.path.exists(paths[0])
    assert db.one("SELECT * FROM publications WHERE id = ?", target["publication_id"]) is not None

    # 重启续办（故障一次性已消费）：同一回收继续，登记被补清
    rec2 = Reclaimer(db, FileLeaseRegistry(), keep=2)
    run2, created = rec2.start_or_reuse()
    assert not created and run2["id"] == run["id"]
    asyncio.run(rec2.execute(run["id"]))
    assert db.latest_reclaim_run()["status"] == "completed"
    assert db.one("SELECT * FROM publications WHERE id = ?", target["publication_id"]) is None
    assert len(db.publications(10)) == 2


def test_delete_error_keeps_incomplete_then_retry_completes(tmp_path):
    db = Database(str(tmp_path / "catalog.db"))
    paths = [_publish(db, tmp_path, i) for i in range(3)]
    rec = Reclaimer(db, FileLeaseRegistry(), keep=2)
    db.set_meta("reclaim_fault", "delete_error")

    run, _ = rec.start_or_reuse()
    asyncio.run(rec.execute(run["id"]))

    assert db.active_reclaim_run() is not None, "删除失败必须保持未完成"
    target = db.reclaim_targets(run["id"])[0]
    assert target["last_error"] and not target["file_deleted_at"]
    assert os.path.exists(paths[0])  # 文件未被删

    asyncio.run(rec.execute(run["id"]))  # 续办（故障一次性已消费）
    assert db.latest_reclaim_run()["status"] == "completed"
    assert not os.path.exists(paths[0])
    assert len(db.publications(10)) == 2


def test_waits_for_lease_before_deleting(tmp_path):
    db = Database(str(tmp_path / "catalog.db"))
    paths = [_publish(db, tmp_path, i) for i in range(3)]
    leases = FileLeaseRegistry()
    assert leases.try_acquire(paths[0])  # 模拟下载进行中

    rec = Reclaimer(db, leases, keep=2, wait_tick=0.05)
    run, _ = rec.start_or_reuse()
    done = threading.Event()

    def worker():
        asyncio.run(rec.execute(run["id"]))
        done.set()

    th = threading.Thread(target=worker)
    th.start()
    time.sleep(0.4)
    # 租约未释放：文件不得被删，回收不得完成
    assert os.path.exists(paths[0])
    assert not done.is_set()
    assert leases.claimed().get(os.path.abspath(paths[0])) == 1

    leases.release(paths[0])  # 下载收尾
    th.join(timeout=10)
    assert done.is_set()
    assert not os.path.exists(paths[0])
    assert db.latest_reclaim_run()["status"] == "completed"


def test_lease_registry_blocks_new_leases_while_claimed():
    leases = FileLeaseRegistry()
    assert leases.try_acquire("/tmp/a")
    assert leases.try_acquire("/tmp/a")
    leases.claim_for_deletion("/tmp/a")
    assert not leases.try_acquire("/tmp/a")  # 删除中拒绝新租约
    assert leases.count("/tmp/a") == 2
    leases.release("/tmp/a")
    leases.release("/tmp/a")
    assert leases.count("/tmp/a") == 0
    leases.unclaim("/tmp/a")
    assert leases.try_acquire("/tmp/a")
    assert leases.claimed() == {}
