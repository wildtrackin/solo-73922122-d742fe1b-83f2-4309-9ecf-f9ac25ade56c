"""OAI-PMH 客户端：解析 ListRecords 响应与分页凭据。

只依赖 httpx + 标准库 ElementTree，方便在容器内运行。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc": "http://purl.org/dc/elements/1.1/",
}


class OAIFatalError(Exception):
    """无法重试的协议/服务器错误。"""


class OAIError(Exception):
    """服务端返回的 OAI 错误（OAI document 内 error 元素）。"""

    def __init__(self, code: str, message: str = ""):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}" if message else code)


@dataclass
class Record:
    identifier: str
    datestamp: str | None
    deleted: bool
    sets: list[str] = field(default_factory=list)
    metadata_xml: str | None = None
    metadata: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class Page:
    records: list[Record]
    token: str | None
    complete_list_size: int | None = None
    cursor: int | None = None


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_page(xml_text: str) -> Page:
    """解析 ListRecords 的 OAI-PMH 信封。

    抛出 OAIError 表示服务端明确拒绝（例如 badResumptionToken），
    抛出 ValueError 表示 XML 结构不可识别。
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError(f"响应不是合法 XML: {exc}") from exc

    if _local(root.tag) != "OAI-PMH":
        raise ValueError(f"响应根元素不是 OAI-PMH，而是 {root.tag!r}")

    error_el = root.find(".//oai:error", NS)
    if error_el is not None:
        raise OAIError(error_el.get("code", "unknown"), "".join(error_el.itertext()).strip())

    list_el = root.find(".//oai:ListRecords", NS)
    if list_el is None:
        # GetRecord 不应被使用；明确报错以免误以为空页。
        raise ValueError("响应中缺少 ListRecords 元素")

    records: list[Record] = []
    for rec_el in list_el.findall("oai:record", NS):
        header = rec_el.find("oai:header", NS)
        if header is None:
            raise ValueError("record 缺少 header")
        identifier = header.findtext("oai:identifier", default="", namespaces=NS).strip()
        if not identifier:
            raise ValueError("record 缺少 identifier")
        datestamp = header.findtext("oai:datestamp", default=None, namespaces=NS)
        status = header.get("status")
        deleted = status == "deleted"
        sets = [s.text.strip() for s in header.findall("oai:setSpec", NS) if s.text]

        metadata_xml = None
        metadata: dict[str, list[str]] = {}
        if not deleted:
            md_el = rec_el.find("oai:metadata", NS)
            if md_el is not None and len(md_el):
                dc_wrapper = md_el[0]
                metadata_xml = ET.tostring(dc_wrapper, encoding="unicode")
                for child in dc_wrapper:
                    name = _local(child.tag)
                    if name == "dc":
                        # dc 元素内部可能还有子节点，不做展开
                        continue
                    metadata.setdefault(name, []).append((child.text or "").strip())

        records.append(
            Record(
                identifier=identifier,
                datestamp=datestamp,
                deleted=deleted,
                sets=sets,
                metadata_xml=metadata_xml,
                metadata=metadata,
            )
        )

    token_el = list_el.find("oai:resumptionToken", NS)
    token = None
    complete_list_size = None
    cursor = None
    if token_el is not None:
        token = (token_el.text or "").strip() or None
        cls = token_el.get("completeListSize")
        cur = token_el.get("cursor")
        try:
            complete_list_size = int(cls) if cls is not None else None
        except ValueError:
            complete_list_size = None
        try:
            cursor = int(cur) if cur is not None else None
        except ValueError:
            cursor = None

    return Page(records=records, token=token, complete_list_size=complete_list_size, cursor=cursor)


class OAIHttpClient:
    """ListRecords 单页读取；网络错误由调用方重试或中断。"""

    def __init__(self, endpoint: str, timeout: float = 30.0, client: "httpx.AsyncClient | None" = None):
        self.endpoint = endpoint
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self):
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, *exc):
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def list_records(self, *, from_: date | None = None, until: date | None = None, token: str | None = None) -> Page:
        assert self._client is not None
        params: dict[str, str] = {"verb": "ListRecords"}
        if token:
            params["resumptionToken"] = token
        else:
            params["metadataPrefix"] = "oai_dc"
            if from_ is not None:
                params["from"] = from_.isoformat()
            if until is not None:
                params["until"] = until.isoformat()
        try:
            resp = await self._client.get(self.endpoint, params=params, follow_redirects=True)
        except Exception:
            raise
        if resp.status_code >= 500 or resp.status_code == 429:
            raise OAIFatalError(f"HTTP {resp.status_code}")
        if resp.status_code != 200:
            raise ValueError(f"HTTP {resp.status_code}")
        return parse_page(resp.text)


def parse_oai_datestamp(value: str) -> date:
    """OAI 的 datestamp 允许 YYYY-MM-DD 或完整 ISO8601，这里取日期部分。"""
    text = value.strip()
    if "T" in text:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    return date.fromisoformat(text[:10])


def now_date_utc() -> date:
    return datetime.now(timezone.utc).date()


def format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")
