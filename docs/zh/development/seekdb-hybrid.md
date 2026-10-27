---
title: seekdb 单 SQL 混合检索
description: 使用原生全文与向量索引获取候选，保留过滤和当前 Memory 引用。
---

# seekdb 单 SQL 混合检索

原生 Runtime 配置 `SeekDBConfig` 和 embedding model 后使用 `SeekDBMemoryIndex`。
`Memory.search(..., mode="hybrid")` 的候选获取通过已有 `AsyncConnection` 执行一条参数化 SQL，
复用产品数据库和原生索引。

SQL 使用 `UNION ALL` 合并两个分别排序、限制数量的子查询。全文分支使用
`MATCH ... AGAINST`，向量分支使用 L2 `ORDER BY ... APPROXIMATE`。两个分支都在数据库内应用
scope、请求中的 Memory artifact、可选的 all/any 标签过滤和 candidate limit。
结果保留通道和向量距离；外层按通道、分数及 artifact、entry、entry version 身份排序。

带标签的向量查询先按条目身份分组选择标签，再通过嵌套循环连接原生向量索引，使标签限制在候选截取前
生效，覆盖 candidate limit 为 1 的极窄过滤。若将相关标签计数留在 ANN 扫描上方，可能把已截取的
候选全部过滤掉。

已有 Memory 流程继续负责候选准入、RRF 融合、身份去重、最终数量限制和精确正文读取。
当前 head 校验和向量完整性检查可以执行额外语句，它们不属于候选查询。
FTS-only、vector-only 继续委托已有索引。SQLite、OceanBase 和未配置 embedding model 的
seekdb 保留原有组合方式。

## 当前版本与增量投影

投影写入、修订、停用、恢复、schema 和索引初始化均复用已有实现。追加其他条目后，
未修改的活跃条目可以保留较旧的投影 `head_revision`，因此候选 SQL 不按请求的 head revision
过滤投影行。后端沿用已有当前 head 校验，`memory_channel_hits` 将已验证的请求 head
附加到命中身份上。正文按命中的精确不可变 entry version 关联读取。
历史 head 仍按已有 capability contract 拒绝。

## 验证方式

`Competition seekdb hybrid` 工作流使用公开测评容器、Python 3.12 和
`pylibseekdb==1.3.0.post4`，沿用一个 CPU、2 GiB 内存和原生执行期间断网的限制。
首先运行未经修改的 `retrieval-02-hybrid-search` 公开验收，检查实际 EXPLAIN、状态变化、
当前引用、过滤、scope 隔离、历史 head 拒绝和单通道行为。

补充测试 `tests/e2e/test_seekdb_hybrid.py` 使用 173 篇原始 SciFact 文档、20 条公开开发集 claim、
固定 384 维单位 L2 向量和三个独立 scope。在同一数据库、head 和 embedding 上，对照原有双查询
组合执行 60 次完整搜索。两条路径均预热并交替运行顺序；逐查询检查 Recall@10、MRR@10 不降低，
在实际 SQLAlchemy 执行边界统计候选 SQL。重复查询核对身份和顺序稳定性。
额外带标签的 EXPLAIN 验证两种原生索引，以及 all/any 标签、candidate limit 和同 scope 其他
artifact 的隔离。

补充语料使用 TF-IDF/SVD 词项向量，公开相关性标注只参与测试评分，产品源码不读取这些文件。
该测量不能替代未公开的正式语料与 embedding 验收。日志报告逐查询结果、完整搜索延迟中位数、
双查询延迟和完整排序一致率；候选查询减少到一条不等于已经证明端到端加速。
数据署名和许可见 `tests/fixtures/seekdb_hybrid/README.md`。

[原生验证记录](https://github.com/Congolese-tribe/PowerContext-/actions/runs/40492741635)
在产品提交 `b346234b6abd95da130290c45f4f6369b05acdf9` 上通过原始公开验收及补充原生回归，
观测结果如下：

| 测量项 | 单 SQL 路径 | 原有双查询路径 |
| --- | --- | --- |
| 每次 hybrid 的候选 SQL 数 | 1 | 2 |
| 60 次查询的平均 Recall@10 | 1.000 | 1.000 |
| 平均 MRR@10 | 0.975 | 0.975 |
| 完整搜索延迟中位数 | 53.578 ms | 57.350 ms |
| 完整排序一致率 | 100% | 对照 |

所有查询逐条通过召回和倒数排名不退化检查。延迟中位数比为 0.9342。这些数值仅描述本次使用补充
词项向量的运行，不预测正式评测结果，也不代表普遍的性能优势。同次运行还通过了 all/any 标签、
candidate limit 为 1 和 2 时的原生索引计划及候选检查。

在固定 Linux 环境运行公开检查：

```bash
POWERCONTEXT_CANDIDATE="$PWD" docker compose -f competition/compose.yaml run --rm selftest \
  python -m pytest -q -s -c /selftest/pytest.ini --confcutdir=/selftest \
  /selftest/challenges/retrieval-02-hybrid-search/acceptance
```
