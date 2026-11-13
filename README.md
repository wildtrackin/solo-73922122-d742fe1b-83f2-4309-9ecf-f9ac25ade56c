# 机构库 → 馆藏目录采集

把机构库 OAI-PMH（`oai_dc`）的新增、修改、撤回同步为一份可导入离线发现系统的
馆藏目录 XML。FastAPI + httpx + SQLite，附本地演示 OAI 源，Docker Compose 一条命令启动。

## 快速启动

```bash
docker compose up --build -d
```

* 采集页：<http://localhost:8000/>（采集、下载、进度、错误、续采）
* 本地演示 OAI-PMH 源：<http://localhost:8001/>（修改/撤回/凭据失效操作面板）
* 源端点：<http://localhost:8001/oai>（`ListRecords&metadataPrefix=oai_dc`）

```bash
docker compose logs -f catalog-web    # 观察采集中/发布失败等日志
docker compose down                   # 停止（数据保留在命名卷）
docker compose down -v                # 连数据一起清空
```

页面操作顺序（也可直接看页面底部"演示脚本"）：

1. **首次全量**：选本轮截止日（默认今天）→ 开始采集。无水位时 `ListRecords`
   不带 `from`，收齐后生成目录并发布，水位 = 截止日。
2. **同日更新 / 撤回**：在 8001 演示源修改标题或撤回一条记录（源端 datestamp
   刷新为今天）→ 再次采集。窗口为 `from=上次成功截止日`（含当天），按
   `identifier` 更新或删除。未出现的记录不动，**不以缺席推断删除**。
3. **分页凭据失效**：演示源上点“下一次凭据请求失效”→ 采集端起续页时收到
   `badResumptionToken`，立即回到原窗口第一页重采，水位不推进、已入库记录不丢。
   页面的“凭据重置 N 次”和橙色提示会记录此事件。
4. **发布前后退出**：采集页“发布故障演练”选 `crash_after_publish` 或
   `crash_before_commit`，再跑一轮 → 进程退出。执行
   `docker compose restart catalog-web` 后，页面水位与下载文件仍是上一版完整结果；
   `crash_after_publish` 的轮次显示“发布失败·可重试”，点重试即可发布新版本，
   无需重采。
5. **历史目录回收**：连续发布几轮后，采集页“历史目录回收”→ 开始回收。
   只清理**回收启动时已登记、且排在最近两次发布之前**的目录 XML 及其发布登记；
   源记录、轮次与当前水位始终保留。已受理的下载会等响应收尾后才删对应文件；
   删除失败或中途退出都会保持“未完成”，重启自动续办，重复启动沿用同次回收。

### 回收演示（慢下载 + 连续发布 + 删除中断）

```bash
docker compose up --build -d
python3 scripts/demo_reclaim.py           # 场景一：慢下载期间连续发布，回收等待下载收尾
python3 scripts/demo_reclaim.py --crash   # 追加场景二：删除中断 → 自动 restart → 自动续办
```

宿主机运行需 `pip install httpx`；也可以在容器内跑场景一：
`docker compose exec -e OAI_SOURCE=http://oai-source:8001 catalog-web python -m scripts.demo_reclaim`
（场景二会终止容器进程，只能在宿主机运行）。

* 场景一：演示源批量造数 → 慢速下载（`?throttle=` 限速）→ 下载期间连续发布 3 轮
  → 启动回收：回收会**等慢下载收尾后**才删其文件；回收期间的新发布与当前目录
  下载完全不受影响。
* 场景二：预置一次性故障 `crash_after_file_delete` → 回收在“文件已删、登记未清”
  时进程退出 → `docker compose restart catalog-web` 后同一回收自动续办，
  全部目标清完才报完成。

也可以在浏览器里手动操作：演示源“批量造数”→ 采集几轮 → 采集页用
`http://localhost:8000/catalog/download?throttle=4096` 慢速下载 → 下载期间再采集
两轮 → 点“开始回收”，在首页观察“等待下载收尾 → 登记已清”的进度变化。

## 行为设计

| 需求 | 实现 |
| --- | --- |
| 固定地址读取 oai_dc | `OAI_ENDPOINT` 固定，`ListRecords` + `resumptionToken` |
| 源按日标记变更、删除标记永久保留 | `records.identifier` 唯一，`deleted=1` 只置位不删除；重采/再版才改 |
| 首次全量，此后从上次成功截止日含当天重采 | `runs.mode=full/incremental`；`window_from=watermark`，`from/until` 均含当天 |
| 网页选择本轮截止日 | 首页日期表单 → `runs.window_until` |
| 按 identifier 更新或删除 | 每页记录 UPSERT；删除头写 tombstone |
| 不以缺席推断删除 | 本轮只处理返回记录，从不扫描本地记录做删除 |
| 同期只采一轮 | `runs` 上的部分唯一索引 + 应用层检查；未解决的轮次挡住新轮次 |
| 保存分页进度、断流/重启可续采 | 每页提交记录与 `resumption_token`（SQLite 事务）；启动把 `running` 转 `paused` |
| badResumptionToken 重采原窗口、不推进水位 | 收到该错误即重置页码/token 从原窗口第一页重采；水位只在发布时写 |
| 收齐后生成含全部有效记录的 XML | `records where deleted=0` 全量入目录，`<catalog watermark recordCount>` |
| 文件与水位一起发布 | 同一 `publications` 行保存文件路径+水位+sha256；页面/下载都只读该行 |
| 生成或发布失败保留旧目录 | 先构造 XML、写临时文件、原子 `os.replace`，最后落库；任一步失败旧行不动 |
| 发布中退出后重启，内容与水位对应同一完整结果 | 唯一文件名+原子改名；未落库的孤儿文件启动时清理；`publishing` 按有无发布记录补完成/转失败 |
| 采集中旧目录照常下载 | 下载只读最新 `publications` 行，与采集事务隔离 |
| 页面显示进度、错误、可续采 | 后台任务 + 2 秒自动刷新；进度条、错误码、继续采集/重试发布/放弃本轮 |
| 回收只清启动时已登记且排在最近两次之前的目标 | `reclaim_targets` 在回收启动时快照固定；新发布与正在生成的文件绝不进入本次回收 |
| 源记录、轮次、当前水位保留 | 回收只删 `publications` 行与对应文件；`records`/`runs`/最新发布不动 |
| 已受理下载固定当时完整文件，收尾后才删 | 下载租约（`FileLeaseRegistry`）：受理即加租约，响应发完或断连收尾才释放；回收先标记"删除中"（拒绝新租约）再等租约归零 |
| 等待与删除不持 SQLite 写锁 | 租约等待是纯内存轮询；`os.remove` 在事务外；登记清理是独立短事务 |
| 删除失败/中途退出显示未完成，重启自动续办 | 每个目标分两步落库（文件已删→登记已清）；`reclaim_runs` 保持 `running`；启动时自动继续未完成的回收 |
| 重复启动沿用同次回收，全部清完才报完成 | `reclaim_runs` 部分唯一索引保证最多一个未完成回收；`complete` 前复查无遗留目标 |

## 测试

```bash
python3 -m pytest tests -q        # 或 docker compose exec catalog-web python -m pytest tests -q
```

覆盖：全量→同日增量、按 identifier 更新、撤回永久保留、缺席不删除、分页进度重启续采、
`badResumptionToken` 重采且水位不动、生成/落库失败保留旧目录、发布前后退出后的
“水位 ↔ 文件”一致性、目录哈希与记录数；回收的目标快照固定、保留最近两次发布、
下载租约等待、删除失败/中断保持未完成并续办、不碰新发布与水位。

## 文件结构

```
app/         采集网页（main.py 路由、harvester.py 编排、oai.py 解析、catalog.py 发布、
             reclaim.py 回收、download.py 租约下载、db.py SQLite）
source/      本地演示 OAI-PMH 源（同样 FastAPI + SQLite）
tests/       端到端行为测试（真实 ASGI 源，无外部依赖）
scripts/     demo_flow.py：无浏览器跑通采集演示（容器内 python -m scripts.demo_flow）
             demo_reclaim.py：慢下载/连续发布/回收/删除中断的可控演示（宿主机运行）
Dockerfile / docker-compose.yml
```

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OAI_ENDPOINT` | `http://oai-source:8001/oai` | 固定 OAI-PMH 地址 |
| `CATALOG_DATA_DIR` | `/srv/data`（compose） | SQLite 与已发布目录 |
| `MAX_PAGES_PER_CALL` | 空（不限） | 单次后台采集页数上限，便于演示多次续采 |
| `PUBLISH_FAILURE_MODE` | `none` | 首次启动时预置一次性发布故障：`generate_error` / `commit_error` / `crash_before_commit` / `crash_after_publish` |
| `RECLAIM_FAILURE_MODE` | `none` | 首次启动时预置一次性回收故障：`delete_error` / `crash_after_file_delete` |
| `DOWNLOAD_THROTTLE_BPS` | 空（不限速） | 下载限速（字节/秒）；单次请求可用 `/catalog/download?throttle=字节/秒` 覆盖 |
| `SOURCE_DB_PATH` | `/srv/data/source.db` | 演示源 SQLite（`oai-source` 服务使用） |

演示源数据在 `source-data` 卷；采集端数据在 `catalog-data` 卷。
