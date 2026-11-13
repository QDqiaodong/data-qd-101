# data-qd-101 — Research Catalog Harvester / 科研数据目录采集

为科研数据索引整理数据集题名、主题与撤回状态，并保留原始 XML 证据。

已实现：有界 OAI-PMH ListRecords/oai_dc 分页、本地 HTTP 模拟服务、opaque continuation 参数编码、协议/日期/元数据校验、修订去重与删除 tombstone、全遍历完成后 SQLite 事务保存、当前版本视图和页面哈希。

新增：可跨进程恢复的日期窗口增量采集（`python -m catalog_harvest.sync`），逐页 SQLite 检查点、成功水位 W、窗口冻结参数、opaque resumptionToken 续传、`badResumptionToken` 一次性持久化重启、失败放弃（discard）与只读 status。全量入口与增量入口互斥，业务键、规范化、同键冲突拒绝、首次页面归属和 tombstone 规则在两条路径上保持一致。

再新增：离线历史主题数仓（`python -m catalog_harvest.warehouse`）。它只通过只读事务读取正式 `pages`/`revisions`（绝不读取 `sync_stage_*`、未完成窗口或水位，也不修改源库），结合固定静态分类 JSON，构建可脱离源库与分类文件单独使用的自包含分析 SQLite：半开版本时间线、传递闭包（最短距离、去重）、多值主题/直接主题/祖先桥、按边界扫描生成的稀疏半开数量区间（含零区间与未使用节点）以及稳定的逻辑来源摘要。配套 `as-of` 子命令回答任意历史日期的有效题名、主题与去重分类计数。完整表粒度、时间口径、去重汇总、来源与失败重建说明见 `docs/warehouse.md`。

再新增：独立的跨主题共现线索分析（`python -m catalog_harvest.affinity`）。它以已交付的历史主题数仓为唯一输入，不重新采集、建仓或改动分类，只在一个显式只读事务里读取当天版本与既有去重祖先桥，产出单个确定性 JSON 报告：一条活动 identifier 一票（含无主题、仅未映射与零命中记录；删除版本只入依据不入分母），先按闭包排除父子对再固定检验家族，固定边际单侧 Fisher 精确尾概率全程用任意精度整数与 `Fraction` 计算，整家族做 Benjamini–Hochberg 反向累计最小值调整，线索须同时满足 `a>=min_shared`、`lift>1` 与 `q<=alpha`，并附可独立重算 a/b/c/d 与 N 的逐记录依据和逻辑 SHA256。计划与报告契约、口径与解释边界见 `docs/affinity.md`。

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

## 离线历史主题数仓（无 HTTP、无服务）

数仓是单写者离线全量重建，输入是现有全量库或受管增量库，再加一个固定静态分类 JSON（原创合成示例：`data/taxonomy_demo.json`）。它不访问 HTTP、不启动服务，仅使用标准库和 SQLite，产物可脱离两个输入单独查询。

```sh
python -m catalog_harvest.warehouse build \
  --source-db run/catalog.sqlite --taxonomy data/taxonomy_demo.json \
  --start 2026-01-01 --end 2026-01-07 --database run/warehouse.sqlite
python -m catalog_harvest.warehouse as-of --database run/warehouse.sqlite --date 2026-01-03
# 一键原创合成示例（自建临时源、构建并逐日 as-of，全部在本地完成）
python -m catalog_harvest.warehouse_demo --workdir run/wh-demo
```

- 只读事务只取正式事实；同步暂存（可含无效 XML 与独有标识）、未完成窗口和成功水位不会被读取或导出，源库字节保持不变，源不存在不会创建空库。
- 每条正式修订重新校验严格日期、deleted/title/subjects/canonical 一致性、页面引用、原文字节 SHA256，并用现有解析器验证同键同规范载荷；损坏事实整次失败并定位到业务键或页面摘要。
- 版本为半开区间 `[valid_from, valid_to)`：修订自其日期生效至下一修订；`start` 前最近修订决定起始状态；首修订之前为未知（不回填、不伪造删除）；删除占据自身区间，之后活动版本可重新出现；相邻但身份不同的版本即使载荷相同也不合并。
- 分类严格校验（schema_version 仅整数 1、精确字段、唯一节点/绑定、拒绝自边/重边/任意分量环）。物化含自身的最短距离闭包；多 subject、同义绑定与菱形路径在每个版本/祖先上只投一票；未绑定 subject 保留在独立明细，绝不冒充真实 topic_id。
- 数量按修订/区间边界扫描成稀疏最大半开区间（含零区间和未使用节点），不按天展开长日期范围；另存全目录活动、删除、无主题活动、含未映射主题活动四项可重叠指标。所有数量为整数，`record_days` 可由区间表 SQL 重算。
- 目标同目录临时库写入后通过 `integrity_check`/`foreign_key_check`/区间覆盖检查，关闭后原子替换；任何失败非零退出、清理本次临时文件且旧目标完整，修正后可直接重跑。输出不得与源库或分类文件为同一文件（含符号/硬链接别名）。

## 跨主题共现线索分析（只读、离线、单报告）

分析器是数仓的独立消费者，只接受已经存在、格式与版本受支持的历史主题仓，不创建缺失库、不建表、不升级、不触发 build，也不修改源库的任何表、索引或状态。

```sh
python -m catalog_harvest.affinity \
  --database run/warehouse.sqlite --date 2026-01-03 \
  --plan data/affinity_plan_example.json --output run/affinity.json
```

- 计划是严格 UTF-8 JSON，顶层恰为 `schema_version`（仅整数 1）、`topics`（2–50 个唯一且已登记的 topic_id，不 trim 不改大小写）、`alpha`（`(0,1]` 普通十进制字符串，至多六位小数，拒绝指数/符号/空白/NaN/Infinity）与 `min_shared`（1..1000000000 整数）；重复 JSON 键拒绝，计划字节只捕获一次并据此计算 SHA256。
- 总体一条活动 identifier 一票：`valid_from <= date < valid_to` 且 `deleted=0`。无主题、仅未映射主题、零命中选中分类的活动记录同样进入分母 N；当天已知删除版本保留在逐记录依据和删除计数中但绝不进 N。每条记录的选中主题集合来自 `wh_version_ancestors` 的既有去重含义，同义词、多直接主题与菱形路径都只投一票。
- 检验家族在查看任何共现量之前确定：任一方向为祖先的分类对进入 `excluded_pairs`（附后代、祖先、最短距离依据）并不再检验；其余全部无序对组成固定家族 m，零共现、零边际与低共现对仍完整检验，且排除规则不意味着剩余检验相互独立。
- 每对输出 a/b/c/d、两边边际、a/N、两个方向 confidence 与 lift；分母为零的指标为 `null`，绝不出现 NaN/Infinity。单侧固定边际 Fisher 尾概率与 Benjamini–Hochberg 调整（按 p 再按对稳定排序、反向累计最小值、全家族参与）全部用任意精度整数与 `Fraction` 精确计算，无 float、无渐近近似、无随机抽样。线索条件为 `a>=min_shared 且 lift>1 且 q<=alpha`，`min_shared` 只在校正后筛选，不改变家族、排位或 p/q。
- 报告含日期、数仓范围与格式、计划字节 SHA256、选中主题与标签、家族定义与 m、全部排除对、全部检验与线索榜、总体与删除计数，以及当天全部已知版本的逐记录依据（identifier、datestamp、有效起止、deleted、题名、页面摘要、命中选中分类集合）。另存数仓既有的 `source_facts_sha256`、`taxonomy_sha256`，并为元数据、选中分类关系和逐记录依据定义可在移走数仓后仅凭报告重算的规范序列化与逻辑 SHA256；不把 SQLite 文件二进制摘要当读取快照。
- 先完成读取、验证、计算与序列化，再经 output 同目录临时文件写完关闭后 `os.replace` 原子替换；输出不得与数仓或计划为同一文件（含符号/硬链接别名）。任何失败非零退出、清理本次临时文件且旧报告完整，修正后可直接重跑；替换成功后的 stdout 失败不回滚报告。相同逻辑事实、计划字节与日期重复运行报告字节一致，不写当前时间、耗时、rowid 或机器绝对路径。
- p 是固定边际模型下的诊断值，q 是指定家族的 BH 调整；不宣称在任意相关结构下保证真实 FDR，不把非随机、不完整的收录数据解释成人群普遍规律或因果关系，线索也不是自动分类或已证实的跨学科关联。


## 结构与业务契约

- `catalog_harvest/protocol.py：XML 命名空间、字段及修订规范`
- `catalog_harvest/transport.py：有界 loopback HTTP 请求`
- `catalog_harvest/harvest.py：分页遍历、事务和版本视图`
- `catalog_harvest/sync.py：可恢复日期窗口增量采集`
- `catalog_harvest/warehouse.py：只读正式事实、静态分类、闭包/桥表与稀疏区间数仓`
- `catalog_harvest/affinity.py：只读数仓快照、精确 Fisher/BH 共现分析与原子 JSON 报告`
- `catalog_harvest/demo.py`、`catalog_harvest/sync_demo.py`、`catalog_harvest/warehouse_demo.py：本地确定性演示`
- `docs/contract.md`、`docs/incremental.md`、`docs/warehouse.md`、`docs/affinity.md`：采集契约、增量内部布局、数仓粒度/语义与共现分析计划/统计/报告契约`
- `data/`：小型确定性合成输入、共现计划示例和 `PROVENANCE.md`。
- `tests/`：真实业务值、重复运行、重要边界、独立数值参照和失败行为测试。

当前范围：当前只有合成来源和 OAI-PMH 日期粒度子集，不声称兼容任意真实档案接口。全量路径没有定时调度；增量路径提供单写者、跨进程可恢复的日期窗口与断点（见 `docs/incremental.md`），但同样没有调度器或并发采集。

全部示例为原创合成数据，不代表真实客户、监管标准、实际测量或线上历史。项目没有真实账号、密钥或付费依赖；不包含第三方原始数据。代码及自制数据采用 MIT；依赖保留自身许可。
