"""本地演示 OAI-PMH 源（oai_dc），可模拟同日修改、撤回与分页凭据失效。

存储使用独立 SQLite 文件，支持：
* ListRecords 按 datestamp（日期粒度，含当天）过滤；删改会刷新 datestamp，
  因此"同日发生"也会被 from=当日 的增量窗口捕获。
* 恒定 2 条/页的分页；每次第一页生成一个新 nonce，旧 token 视为
  badResumptionToken（可手动触发，用于演示凭据失效）。
* 提供 /demo/* 管理接口（修改、撤回、再版、reset-tokens），页面里可直接操作。
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from email.utils import format_datetime
import json
import os
import secrets
import sqlite3
import threading
import xml.etree.ElementTree as ET

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

HERE = os.path.dirname(os.path.abspath(__file__))
PAGE_SIZE = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    identifier TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    creator TEXT,
    subject TEXT,
    description TEXT,
    publisher TEXT,
    date TEXT,
    type TEXT,
    datestamp TEXT NOT NULL,   -- YYYY-MM-DD（OAI 日期粒度）
    deleted INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS flags (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tokens (
    nonce TEXT NOT NULL,
    page INTEGER NOT NULL,
    window_from TEXT,
    window_until TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY(nonce, page)
);
"""

SEED = [
    ("oai:demo:lib:001", "图书馆学概论", "王知津", "图书馆学", "高等学校图书馆学基础教材", "示例出版社", 2019, "book"),
    ("oai:demo:lib:002", "数字资源建设与服务", "李静", "数字图书馆", "机构库与发现系统实践", "示例出版社", 2021, "book"),
    ("oai:demo:lib:003", "元数据理论与应用", "张明", "元数据", "OAI-PMH 与元数据互操作", "示例出版社", 2020, "book"),
    ("oai:demo:lib:004", "学术机构知识库指南", "陈芳", "机构库", "机构知识库建设与运营", "示例出版社", 2022, "book"),
    ("oai:demo:lib:005", "古籍数字化研究", "刘伟", "古籍数字化", "古籍数字化与长期保存", "示例出版社", 2018, "book"),
]


def today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


class SourceStore:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._seed()

    def _seed(self) -> None:
        row = self._conn.execute("SELECT COUNT(*) AS n FROM items").fetchone()
        if row and row["n"]:
            return
        with self._lock:
            self._conn.execute("BEGIN")
            for (ident, title, creator, subject, desc, publisher, year, typ) in SEED:
                self._conn.execute(
                    """INSERT INTO items(identifier,title,creator,subject,description,publisher,date,type,datestamp,deleted,version)
                       VALUES(?,?,?,?,?,?,?,?,?,0,1)""",
                    (ident, title, creator, subject, desc, publisher, str(year), typ, today_utc()),
                )
            self._conn.execute("COMMIT")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _one(self, sql: str, *params):
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def _all(self, sql: str, *params):
        with self._lock:
            return list(self._conn.execute(sql, params))

    def stats(self) -> dict:
        total = self._one("SELECT COUNT(*) n FROM items")["n"]
        deleted = self._one("SELECT COALESCE(SUM(deleted),0) d FROM items")["d"]
        return {"total": total, "active": total - deleted, "deleted": deleted, "page_size": PAGE_SIZE}

    def list_items(self) -> list[sqlite3.Row]:
        return self._all("SELECT * FROM items ORDER BY identifier")

    def get_item(self, identifier: str):
        return self._one("SELECT * FROM items WHERE identifier = ?", identifier)

    def _delete_tokens(self) -> None:
        self._conn.execute("DELETE FROM tokens")

    def create_item(self, *, identifier: str, title: str, creator: str, subject: str, description: str, publisher: str, date_: str, type_: str) -> None:
        now = today_utc()
        with self._lock:
            self._conn.execute("BEGIN")
            self._conn.execute(
                """INSERT INTO items(identifier,title,creator,subject,description,publisher,date,type,datestamp,deleted,version)
                   VALUES(?,?,?,?,?,?,?,?,?,0,1)""",
                (identifier, title, creator, subject, description, publisher, date_, type_, now),
            )
            self._delete_tokens()
            self._conn.execute("COMMIT")

    def update_item(self, identifier: str, **fields) -> sqlite3.Row | None:
        item = self.get_item(identifier)
        if item is None:
            return None
        allowed = {"title", "creator", "subject", "description", "publisher", "date", "type"}
        updates = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if not updates:
            return item
        sets = ", ".join(f"{k} = ?" for k in updates)
        params = list(updates.values())
        with self._lock:
            self._conn.execute("BEGIN")
            self._conn.execute(
                f"UPDATE items SET {sets}, datestamp = ?, version = version + 1 WHERE identifier = ?",
                (*params, today_utc(), identifier),
            )
            self._delete_tokens()
            self._conn.execute("COMMIT")
        return self.get_item(identifier)

    def delete_item(self, identifier: str) -> sqlite3.Row | None:
        item = self.get_item(identifier)
        if item is None:
            return None
        with self._lock:
            self._conn.execute("BEGIN")
            self._conn.execute(
                "UPDATE items SET deleted = 1, datestamp = ?, title = title || '（已撤回）' WHERE identifier = ?",
                (today_utc(), identifier),
            )
            self._delete_tokens()
            self._conn.execute("COMMIT")
        return self.get_item(identifier)

    def restore_item(self, identifier: str) -> sqlite3.Row | None:
        item = self.get_item(identifier)
        if item is None:
            return None
        with self._lock:
            self._conn.execute("BEGIN")
            self._conn.execute(
                "UPDATE items SET deleted = 0, datestamp = ?, version = version + 1 WHERE identifier = ?",
                (today_utc(), identifier),
            )
            self._delete_tokens()
            self._conn.execute("COMMIT")
        return self.get_item(identifier)

    # ---- 分页 ----
    def trip_next_token(self) -> None:
        """下一次用凭据取页时强制失效一次，稳定复现 badResumptionToken。"""
        with self._lock:
            self._conn.execute(
                "INSERT INTO flags(key, value) VALUES('trip_token','1') "
                "ON CONFLICT(key) DO UPDATE SET value='1'"
            )

    def consume_trip(self) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT value FROM flags WHERE key='trip_token'").fetchone()
            if row is None or row["value"] != "1":
                return False
            self._conn.execute("DELETE FROM flags WHERE key='trip_token'")
            return True

    def invalidate_all_tokens(self) -> int:
        with self._lock:
            n = self._conn.execute("SELECT COUNT(*) c FROM tokens").fetchone()["c"]
            self._delete_tokens()
            return n

    def issue_token(self) -> str:
        nonce = secrets.token_hex(8)
        return nonce

    def save_token(self, nonce: str, page: int, window_from: str | None, window_until: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO tokens(nonce, page, window_from, window_until, created_at) VALUES(?,?,?,?,?)",
                (nonce, page, window_from, window_until, datetime.now(timezone.utc).isoformat()),
            )

    def lookup_token(self, nonce: str, page: int):
        return self._one("SELECT * FROM tokens WHERE nonce = ? AND page = ?", nonce, page)


def _select(store: SourceStore, from_: str | None, until: str | None) -> list[sqlite3.Row]:
    sql = "SELECT * FROM items WHERE 1=1"
    params: list = []
    if from_:
        sql += " AND datestamp >= ?"
        params.append(from_)
    if until:
        sql += " AND datestamp <= ?"
        params.append(until)
    sql += " ORDER BY datestamp, identifier"
    return store._all(sql, *params)


def _dc_xml(store: SourceStore, item: sqlite3.Row) -> str:
    wrapper = ET.Element("{http://www.openarchives.org/OAI/2.0/oai_dc/}oai_dc")
    wrapper.set("{http://www.w3.org/2001/XMLSchema-instance}schemaLocation",
                "http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd")
    ns = "http://purl.org/dc/elements/1.1/"
    for tag, key in (("title", "title"), ("creator", "creator"), ("subject", "subject"),
                     ("description", "description"), ("publisher", "publisher"),
                     ("date", "date"), ("type", "type"), ("identifier", "identifier")):
        value = item[key]
        if value:
            ET.SubElement(wrapper, f"{{{ns}}}{tag}").text = str(value)
    return ET.tostring(wrapper, encoding="unicode")


def _oai_error(code: str, message: str) -> Response:
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
        f"<error code=\"{code}\">{message}</error></OAI-PMH>"
    )
    return Response(content=body, media_type="text/xml", status_code=200)


def _render_record(store: SourceStore, item: sqlite3.Row) -> str:
    if item["deleted"]:
        return (
            "<record><header status=\"deleted\">"
            f"<identifier>{item['identifier']}</identifier>"
            f"<datestamp>{item['datestamp']}</datestamp></header></record>"
        )
    return (
        "<record><header>"
        f"<identifier>{item['identifier']}</identifier>"
        f"<datestamp>{item['datestamp']}</datestamp>"
        "<setSpec>institution</setSpec></header>"
        f"<metadata>{_dc_xml(store, item)}</metadata></record>"
    )


def _envelope(inner: str, request: Request) -> Response:
    base = str(request.base_url).rstrip("/") + "/oai"
    now = format_datetime(datetime.now(timezone.utc), usegmt=True)
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
        f"<responseDate>{now}</responseDate>"
        f"<request verb=\"ListRecords\">{base}</request>"
        f"{inner}</OAI-PMH>"
    )
    return Response(content=body, media_type="text/xml")


def create_source_app(db_path: str | None = None) -> FastAPI:
    db_path = db_path or os.environ.get("SOURCE_DB_PATH", "/srv/data/source.db")
    store = SourceStore(db_path)
    app = FastAPI(title="Demo OAI-PMH Source", docs_url="/docs", redoc_url=None)
    app.state.store = store

    templates = Jinja2Templates(directory=os.path.join(HERE, "templates"))
    static_dir = os.path.join(HERE, "static")
    if os.path.isdir(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/healthz", response_class=PlainTextResponse)
    def healthz():
        return "ok"

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "items": store.list_items(),
                "stats": store.stats(),
                "today": today_utc(),
                "page_size": PAGE_SIZE,
            },
        )

    @app.get("/oai", response_class=Response)
    def oai(request: Request, verb: str | None = None, metadataPrefix: str | None = None,
            from_: str | None = None, until: str | None = None, resumptionToken: str | None = None,
            identifier: str | None = None, set: str | None = None):
        params = request.query_params
        verb = params.get("verb")
        resumption_token = params.get("resumptionToken")

        if verb is None:
            return _oai_error("badVerb", "缺少 verb")
        if verb not in ("Identify", "ListMetadataFormats", "ListSets", "ListRecords", "ListIdentifiers", "GetRecord"):
            return _oai_error("badVerb", f"不支持的 verb: {verb}")

        if verb == "Identify":
            inner = (
                "<Identify><repositoryName>示例机构库</repositoryName>"
                "<baseURL>" + str(request.base_url).rstrip("/") + "/oai</baseURL>"
                "<protocolVersion>2.0</protocolVersion>"
                "<adminEmail>lib@example.org</adminEmail>"
                "<earliestDatestamp>2018-01-01</earliestDatestamp>"
                "<deletedRecord>persistent</deletedRecord>"
                "<granularity>YYYY-MM-DD</granularity></Identify>"
            )
            return _envelope(inner, request)
        if verb == "ListMetadataFormats":
            return _envelope(
                "<ListMetadataFormats><metadataFormat><metadataPrefix>oai_dc</metadataPrefix>"
                "<schema>http://www.openarchives.org/OAI/2.0/oai_dc.xsd</schema>"
                "<metadataNamespace>http://www.openarchives.org/OAI/2.0/oai_dc/</metadataNamespace>"
                "</metadataFormat></ListMetadataFormats>",
                request,
            )
        if verb == "ListSets":
            return _envelope("<ListSets><set><setSpec>institution</setSpec><setName>机构库</setName></set></ListSets>", request)
        if verb == "GetRecord":
            if not identifier:
                return _oai_error("badArgument", "缺少 identifier")
            item = store.get_item(identifier)
            if item is None:
                return _oai_error("idDoesNotExist", "记录不存在")
            rec = _render_record(store, item)
            return _envelope(f"<GetRecord>{rec}</GetRecord>", request)

        # ListRecords / ListIdentifiers
        want_identifiers = verb == "ListIdentifiers"
        if resumption_token:
            if metadataPrefix:
                return _oai_error("badArgument", "resumptionToken 与 metadataPrefix 不能同时使用")
            nonce, _, page_txt = resumption_token.rpartition("-")
            try:
                page = int(page_txt)
            except ValueError:
                return _oai_error("badResumptionToken", "凭据格式无效")
            if store.consume_trip():
                return _oai_error("badResumptionToken", "凭据已失效（演示注入），请回到原窗口第一页重采")
            row = store.lookup_token(nonce, page)
            if row is None:
                return _oai_error("badResumptionToken", "凭据已失效，请回到原窗口第一页重采")
            window_from, window_until = row["window_from"], row["window_until"]
            all_items = _select(store, window_from, window_until)
            start = page * PAGE_SIZE
            page_items = all_items[start:start + PAGE_SIZE]
            next_token = None
            if start + PAGE_SIZE < len(all_items):
                next_nonce = nonce
                store.save_token(next_nonce, page + 1, window_from, window_until)
                next_token = f"{next_nonce}-{page + 1}"
            total = len(all_items)
            cursor = start
        else:
            if metadataPrefix is None:
                return _oai_error("badArgument", "缺少 metadataPrefix")
            if metadataPrefix != "oai_dc":
                return _oai_error("cannotDisseminateFormat", "仅支持 oai_dc")
            window_from = params.get("from")
            window_until = params.get("until")
            for label, value in (("from", window_from), ("until", window_until)):
                if value:
                    try:
                        date.fromisoformat(value[:10])
                    except ValueError:
                        return _oai_error("badArgument", f"{label} 日期格式无效")
            all_items = _select(store, window_from, window_until)
            page = 0
            page_items = all_items[:PAGE_SIZE]
            total = len(all_items)
            cursor = 0
            next_token = None
            if PAGE_SIZE < len(all_items):
                nonce = store.issue_token()
                store.save_token(nonce, 1, window_from, window_until)
                next_token = f"{nonce}-1"

        rendered = []
        for it in page_items:
            if want_identifiers:
                status = ' status="deleted"' if it["deleted"] else ""
                rendered.append(
                    f"<header{status}><identifier>{it['identifier']}</identifier>"
                    f"<datestamp>{it['datestamp']}</datestamp></header>"
                )
            else:
                rendered.append(_render_record(store, it))
        inner = f"<{verb}>" + "".join(rendered)
        if next_token:
            inner += (
                f'<resumptionToken completeListSize="{total}" cursor="{cursor}">{next_token}</resumptionToken>'
            )
        elif total > 0:
            inner += f'<resumptionToken completeListSize="{total}" cursor="{cursor}"/>'
        inner += f"</{verb}>"
        return _envelope(inner, request)

    # ------------------------------------------------------- 演示管理接口
    def _redirect():
        return RedirectResponse(url="/", status_code=303)

    @app.post("/demo/update")
    async def demo_update(request: Request):
        form = await request.form()
        ident = str(form.get("identifier", ""))
        fields = {k: str(form.get(k, "")) for k in ("title", "creator", "subject", "description", "publisher", "date", "type")}
        store.update_item(ident, **{k: v for k, v in fields.items() if v})
        return _redirect()

    @app.post("/demo/delete")
    async def demo_delete(request: Request):
        form = await request.form()
        store.delete_item(str(form.get("identifier", "")))
        return _redirect()

    @app.post("/demo/restore")
    async def demo_restore(request: Request):
        form = await request.form()
        store.restore_item(str(form.get("identifier", "")))
        return _redirect()

    @app.post("/demo/create")
    async def demo_create(request: Request):
        form = await request.form()
        ident = str(form.get("identifier", "")).strip()
        if ident:
            store.create_item(
                identifier=ident,
                title=str(form.get("title", "") or "未命名"),
                creator=str(form.get("creator", "")),
                subject=str(form.get("subject", "")),
                description=str(form.get("description", "")),
                publisher=str(form.get("publisher", "")),
                date_=str(form.get("date", "")),
                type_=str(form.get("type", "") or "book"),
            )
        return _redirect()

    @app.post("/demo/bulk-create")
    async def demo_bulk_create(request: Request):
        """批量新增记录（幂等）：让目录文件足够大，便于慢下载/回收演示。"""
        form = await request.form()
        try:
            count = int(str(form.get("count", "0")) or "0")
        except ValueError:
            count = 0
        count = max(0, min(count, 2000))
        prefix = "".join(ch for ch in str(form.get("prefix", "bulk")) if ch.isalnum() or ch in "-_") or "bulk"
        for i in range(count):
            ident = f"oai:demo:{prefix}:{i + 1:04d}"
            if store.get_item(ident) is None:
                store.create_item(
                    identifier=ident,
                    title=f"批量演示书目 {i + 1:04d}",
                    creator="演示馆员",
                    subject="批量演示数据",
                    description="用于慢下载与回收演示的批量记录",
                    publisher="示例出版社",
                    date_="2026",
                    type_="book",
                )
        return _redirect()

    @app.post("/demo/reset-tokens")
    def demo_reset_tokens():
        store.invalidate_all_tokens()
        return _redirect()

    @app.post("/demo/trip-token")
    def demo_trip_token():
        store.trip_next_token()
        return _redirect()

    return app
