"""生成发现系统可导入的馆藏目录 XML，并原子发布。

发布顺序（不可颠倒）
1. 构造 XML（"生成"阶段，可失败；失败时旧目录与旧水位完全不动）。
2. 在输出目录写唯一临时文件 → fsync。
3. os.replace 为最终唯一文件名（同目录 rename 原子）。
4. 写 publications 行：水位、file_path、sha256、记录数在同一条记录里。

页面展示的水位与下载文件都只从"最新 publications 行"解析，因此进程在发布
过程中任意位置退出，重启后二者仍是同一次完整发布：
* 第 3 步后、第 4 步前退出：新文件无人引用，页面仍指向旧行+旧文件；
  启动时清除孤儿文件。
* 第 4 步后退出：页面指向新行+新文件（run 状态在启动时补记为完成）。
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
import secrets
import xml.etree.ElementTree as ET

from .db import Database

CATALOG_NS = "urn:library:catalog:1"
DC_NS = "http://purl.org/dc/elements/1.1/"

# 演示故障注入（一次性，value 由 meta 表保存）
FAULT_NONE = "none"
FAULT_GENERATE_ERROR = "generate_error"        # 生成阶段失败 → 保留旧目录，可重试
FAULT_COMMIT_ERROR = "commit_error"            # 写完文件但落库前报错 → 保留旧目录，可重试
FAULT_CRASH_BEFORE_COMMIT = "crash_before_commit"  # 临时文件写完、rename 前进程退出
FAULT_CRASH_AFTER_PUBLISH = "crash_after_publish"  # rename 后、落库前进程退出

KNOWN_FAULTS = {
    FAULT_NONE,
    FAULT_GENERATE_ERROR,
    FAULT_COMMIT_ERROR,
    FAULT_CRASH_BEFORE_COMMIT,
    FAULT_CRASH_AFTER_PUBLISH,
}


class PublishFault(RuntimeError):
    pass


def _text(parent: ET.Element, tag: str, value: str | None) -> ET.Element:
    el = ET.SubElement(parent, tag)
    if value is not None:
        el.text = value
    return el


def build_catalog_xml(records: list, *, watermark: str, generated_at: str | None = None) -> str:
    """records 为 sqlite Row；调用方只传 deleted=0 的记录，这里再校验一次。

    输出形如：
        <catalog xmlns="urn:library:catalog:1" watermark="..." recordCount="...">
          <record identifier="oai:...">
            <datestamp>...</datestamp><setSpec>...</setSpec>
            <metadata><dc:title>...</dc:title>...</metadata>
          </record>
        </catalog>
    """
    generated_at = generated_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    root = ET.Element("catalog", {"xmlns": CATALOG_NS})
    root.set("watermark", watermark)
    root.set("generatedAt", generated_at)
    root.set("recordCount", str(len(records)))
    for rec in records:
        if rec["deleted"]:
            raise ValueError(f"撤回记录 {rec['identifier']} 不应写入馆藏目录")
        item = ET.SubElement(root, "record")
        item.set("identifier", rec["identifier"])
        _text(item, "datestamp", rec["datestamp"])
        for set_spec in json.loads(rec["sets_json"] or "[]"):
            _text(item, "setSpec", set_spec)
        metadata = ET.SubElement(item, "metadata")
        for name, values in json.loads(rec["metadata_json"] or "{}").items():
            if not _is_xml_name(name):
                continue
            for value in values:
                _text(metadata, f"{{{DC_NS}}}{name}", value)
    ET.indent(root, space="  ")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode")


def _is_xml_name(name: str) -> bool:
    import re

    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", name or ""))


def cleanup_orphans(output_dir: str, keep_paths) -> list[str]:
    """删除未被 publications 引用的 catalog-* / 临时文件（上次发布中途退出遗留）。

    keep_paths 可以是一个路径或路径集合；保留最近若干次发布文件便于回溯，
    其余（孤儿/临时）一律清除。
    """
    removed: list[str] = []
    if not os.path.isdir(output_dir):
        return removed
    if isinstance(keep_paths, str):
        keep_paths = {keep_paths}
    keep = {os.path.abspath(p) for p in (keep_paths or []) if p}
    for name in os.listdir(output_dir):
        if not (name.startswith("catalog-") and name.endswith(".xml")) and not name.startswith(".catalog-tmp-"):
            continue
        path = os.path.abspath(os.path.join(output_dir, name))
        if path in keep:
            continue
        try:
            os.remove(path)
            removed.append(name)
        except OSError:
            pass
    return removed


def _keep_paths(db: Database) -> set[str]:
    """保留所有已登记的发布文件（便于审计/回溯）；只清理从未登记过的孤儿。"""
    return {row["file_path"] for row in db.query("SELECT file_path FROM publications")}


def write_publication(
    db: Database,
    output_dir: str,
    run_id: int,
    watermark: str,
    records: list,
    *,
    fault: str = FAULT_NONE,
    exit_process=os._exit,
) -> dict:
    """返回已落库的发布描述；任何失败都抛异常且不改变任何已发布状态。"""
    if fault not in KNOWN_FAULTS:
        raise PublishFault(f"未知故障注入模式: {fault}")
    fault = db.take_publish_fault() or fault

    os.makedirs(output_dir, exist_ok=True)

    # --- 1. 生成 ---
    try:
        xml_text = build_catalog_xml(records, watermark=watermark)
        payload = xml_text.encode("utf-8")
    except Exception as exc:
        raise PublishFault(f"目录生成失败：{exc}") from exc

    if fault == FAULT_GENERATE_ERROR:
        raise PublishFault("演示：目录生成阶段失败（旧目录保持可下载，水位不变）")

    digest = hashlib.sha256(payload).hexdigest()
    final_name = f"catalog-run{run_id}-{digest[:12]}.xml"
    final_path = os.path.abspath(os.path.join(output_dir, final_name))
    tmp_path = os.path.join(output_dir, f".catalog-tmp-{secrets.token_hex(8)}.xml")

    # --- 2/3. 写临时文件并原子改名 ---
    try:
        with open(tmp_path, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
    except OSError as exc:
        raise PublishFault(f"目录写入失败：{exc}") from exc

    if fault == FAULT_CRASH_BEFORE_COMMIT:
        # 演示：临时文件已写、尚未 rename，进程直接退出；旧目录引用不受影响
        exit_process(70)
        raise PublishFault("演示进程未退出（exit hook 返回）")

    try:
        os.replace(tmp_path, final_path)
        dir_fd = os.open(output_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError as exc:
        raise PublishFault(f"目录发布改名失败：{exc}") from exc

    if fault == FAULT_CRASH_AFTER_PUBLISH:
        # 演示：文件已原子替换，但发布记录尚未落库，进程退出
        exit_process(70)
        raise PublishFault("演示进程未退出（exit hook 返回）")

    if fault == FAULT_COMMIT_ERROR:
        try:
            os.remove(final_path)  # 无人引用，立即清除
        except OSError:
            pass
        raise PublishFault("演示：目录文件已写出但发布登记失败（旧目录仍为当前版本）")

    # --- 4. 水位与文件同一条记录落库 ---
    try:
        db.record_publication(
            run_id=run_id,
            watermark=watermark,
            file_path=final_path,
            file_name=final_name,
            record_count=len(records),
            size_bytes=len(payload),
            sha256=digest,
        )
    except Exception as exc:
        # 旧 publications 行未动；新文件成为孤儿
        try:
            os.remove(final_path)
        except OSError:
            pass
        raise PublishFault(f"发布登记失败：{exc}") from exc

    cleanup_orphans(output_dir, keep_paths=_keep_paths(db))
    return {
        "path": final_path,
        "name": final_name,
        "watermark": watermark,
        "record_count": len(records),
        "size_bytes": len(payload),
        "sha256": digest,
    }
