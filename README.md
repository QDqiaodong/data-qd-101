# data-qd-101 — Research Catalog Harvester / 科研数据目录采集

为科研数据索引整理数据集题名、主题与撤回状态，并保留原始 XML 证据。

已实现：有界 OAI-PMH ListRecords/oai_dc 分页、本地 HTTP 模拟服务、opaque continuation 参数编码、协议/日期/元数据校验、修订去重与删除 tombstone、全遍历完成后 SQLite 事务保存、当前版本视图和页面哈希。

新增：可跨进程恢复的日期窗口增量采集（`python -m catalog_harvest.sync`），逐页 SQLite 检查点、成功水位 W、窗口冻结参数、opaque resumptionToken 续传、`badResumptionToken` 一次性持久化重启、失败放弃（discard）与只读 status。全量入口与增量入口互斥，业务键、规范化、同键冲突拒绝、首次页面归属和 tombstone 规则在两条路径上保持一致。

## 独立安装

需要 Python >=3.11（先确认 python3 --version）。运行依赖全部为标准库。 构建工具也已固定版本。
```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install -r requirements.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
```

首次安装可能需要联网；安装后核心演示与测试可离线运行。每仓独立 `.venv`，不使用其他项目源代码或环境。macOS/Linux 使用相同命令；具体已验证环境不由此声明。

## 演示、输出与测试

在仓库根目录执行。首次 `run/` 不存在即为干净数据状态；需要干净重演可指定新的输出路径。

```sh
.venv/bin/python -m catalog_harvest.demo --db run/catalog.sqlite
.venv/bin/python -m catalog_harvest.demo --db run/catalog.sqlite
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -c 'import sqlite3; db=sqlite3.connect("run/catalog.sqlite"); print(db.execute("SELECT identifier,datestamp,deleted,title FROM current_records ORDER BY identifier").fetchall())'
```

2 页共 4 次记录出现，3 个唯一修订，当前 1 个 active 与 1 个 deleted；重放新增修订 0。活动题名 River flow & sediment，B 的旧活动版本不覆盖较新删除。

demo 启动本题自己的短生命周期 loopback 服务、执行真实 HTTP GET 后关闭，不访问外部发布者。可用 CLI `--endpoint http://127.0.0.1:PORT/oai --db run/catalog.sqlite` 连接自管符合契约的本地模拟源。

## 增量日期窗口（跨进程可恢复）

增量入口是独立子命令，数据库与一个明确 endpoint 绑定，全量 CLI 与增量 CLI 互斥：

```sh
# 首次：绑定 endpoint 与固定起点 B，采集闭区间 [B, until]
python -m catalog_harvest.sync run --endpoint http://127.0.0.1:PORT/oai \
    --db run/inc.sqlite --start-date 2026-01-01 --until 2026-01-03
# 续采：下界为 max(B, W-(overlap-days-1))，上界为 --until，两端包含
python -m catalog_harvest.sync run --endpoint http://127.0.0.1:PORT/oai \
    --db run/inc.sqlite --until 2026-01-05 --overlap-days 2 --max-pages 20
python -m catalog_harvest.sync status --db run/inc.sqlite          # 只读本地状态
python -m catalog_harvest.sync discard --db run/inc.sqlite         # 放弃未发布窗口
```

也可一键启动自管 loopback 源并连续完成两个窗口（含迟到 C/Jan2、新 A/Jan4 与重复 B tombstone）：

```sh
python -m catalog_harvest.sync_demo --db run/inc.sqlite
```

日期严格 `YYYY-MM-DD`；`overlap-days` 为 1..365 整数（默认 2）；页面上限默认 20、范围 1..1000。
`until` 不得早于 B 或已有水位 W，允许等于 W 以便回采。绑定规则：

- 首次初始化保存 endpoint 与固定 B；已含非空全量 `pages/revisions` 的旧库在发出任何 HTTP 请求前被拒绝，不会从最大 datestamp 猜测来源或水位。
- 已绑定库拒绝更换 endpoint 或起点 B；存在未完成窗口时，续跑必须使用同一组冻结参数，参数不符在联网前报错，恢复过程不重算日期范围。
- 全量 `harvest`/demo/`python -m catalog_harvest` 遇到受管增量库会明确拒绝写入。单写者边界，无调度器、无并发采集。

### 请求、检查点与恢复语义

- 首请求：`verb=ListRecords&metadataPrefix=oai_dc&from=...&until=...`；续页只能带 `verb=ListRecords&resumptionToken=...`，不附加日期或 metadataPrefix。令牌完全 opaque（含首尾空白与特殊字符）。沿用 5 秒超时、每页 1 MiB、禁止重定向和既有 XML 安全限制。
- 所有返回记录（含删除标记）的 datestamp 必须落在冻结窗口内，否则整窗失败。
- 每页先完整校验，再用**一个 SQLite 事务**共同写入暂存原始 XML、解析修订、页序、请求形态（first/resume）、下一令牌与循环检测依据；只有检查点提交后才请求下一页。正式 `pages/revisions/current_records` 在整窗成功前保持原样。
- 进程退出或 HTTP 失败后，新进程从持久化的下一令牌继续，不重复已确认页面；终页已持久化时直接发布、不再发 HTTP。循环集合与页面预算跨进程有效。
- 两个明确 OAI 分支：首请求收到仅含 `noRecordsMatch` 且无 ListRecords 的合法响应表示空窗，正常完成并推进 W；续页 `noRecordsMatch` 不能当作正常结束。续页仅含 `badResumptionToken` 时最多自动重启当前窗口一次：重启额度持久化，丢弃该遍历全部暂存页/修订/令牌历史，再以完全相同的 from/until 重放首请求，不拼接旧遍历；第二次失效明确停止。首请求收到 `badResumptionToken` 直接失败。
- 整窗成功后用**一个事务**把最终有效遍历合并进正式表、置 W=until、标记窗口完成，三者共同提交、失败共同回滚（完整暂存仍可重试发布）；空窗也推进 W。重叠回采不会删除未返回标识，只有显式 tombstone 表示删除，旧活动修订不会使更新的删除标记复活。
- 硬失败（XML 错误、越界、修订冲突、令牌循环、预算或重启额度耗尽）进入 `failed`，后续 run 拒绝静默跳过，需先 `discard`。`discard` 不发 HTTP，只清理未发布暂存，不改正式事实与 W。旧数据库没有窗口历史，不能补造。

### 能力边界

重叠只能覆盖有限的迟到修订范围；OAI-PMH 分页不保证服务端在多次请求间保持同一时刻快照，因此跨窗口/跨进程看到的集合可能随源端变化。本项目只针对合成 loopback 源与文档化的日期粒度子集，不连接真实档案服务，不做自动 HTTP 重试（除上述一次性 badResumptionToken 重启外）、无调度器。完整内部表布局与状态转换见 `docs/incremental.md`。

## 结构与业务契约

- `catalog_harvest/protocol.py：XML 命名空间、字段及修订规范`
- `catalog_harvest/transport.py：有界 loopback HTTP 请求`
- `catalog_harvest/harvest.py：分页遍历、事务和版本视图`
- `catalog_harvest/demo.py：临时模拟来源，全流程后关闭`
- `docs/contract.md`：完整字段/格式、业务键、边界、输出含义与当前约束。
- `data/`：小型确定性合成输入和 `PROVENANCE.md`。
- `tests/`：真实业务值、重复运行、重要边界和失败行为测试。

当前范围：当前只有合成来源和 OAI-PMH 日期粒度子集，不声称兼容任意真实档案接口。全量路径没有定时调度；增量路径提供单写者、跨进程可恢复的日期窗口与断点（见 `docs/incremental.md`），但同样没有调度器或并发采集。

全部示例为原创合成数据，不代表真实客户、监管标准、实际测量或线上历史。项目没有真实账号、密钥或付费依赖；不包含第三方原始数据。代码及自制数据采用 MIT；依赖保留自身许可。
