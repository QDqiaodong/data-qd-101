# data-qd-101 — Research Catalog Harvester / 科研数据目录采集

为科研数据索引整理数据集题名、主题与撤回状态，并保留原始 XML 证据。

已实现：有界 OAI-PMH ListRecords/oai_dc 分页、本地 HTTP 模拟服务、opaque continuation 参数编码、协议/日期/元数据校验、修订去重与删除 tombstone、全遍历完成后 SQLite 事务保存、当前版本视图和页面哈希。

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

## 结构与业务契约

- `catalog_harvest/protocol.py：XML 命名空间、字段及修订规范`
- `catalog_harvest/transport.py：有界 loopback HTTP 请求`
- `catalog_harvest/harvest.py：分页遍历、事务和版本视图`
- `catalog_harvest/demo.py：临时模拟来源，全流程后关闭`
- `docs/contract.md`：完整字段/格式、业务键、边界、输出含义与当前约束。
- `data/`：小型确定性合成输入和 `PROVENANCE.md`。
- `tests/`：真实业务值、重复运行、重要边界和失败行为测试。

当前范围：当前只有合成来源和 OAI-PMH 日期粒度子集，不声称兼容任意真实档案接口。没有定时调度、增量日期窗口或断点；每次有界全量遍历，单写者。

全部示例为原创合成数据，不代表真实客户、监管标准、实际测量或线上历史。项目没有真实账号、密钥或付费依赖；不包含第三方原始数据。代码及自制数据采用 MIT；依赖保留自身许可。
