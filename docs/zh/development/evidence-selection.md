---
title: 按查询选择原文证据
description: 从 Memory 中选择保留精确引用的原文片段，并按完整 UTF-8 输出核算预算。
---

# 按查询选择原文证据

在原生 Runtime 的 `PrepareContextRequest` 中设置 `evidence_selection=True`，即可根据
查询从已召回的 Memory entry 内部选择原文证据。该字段只接受真正的布尔值，默认 `False`；
省略或关闭时，普通 prepare 的正文和行为保持兼容。

```python
from powercontext.builtin.runtime import PrepareContextRequest

prepared = await runtime.context.for_scope(scope_id).prepare(
    PrepareContextRequest(
        query="cobalt sensor calibration temperature",
        max_bytes=1400,
        evidence_selection=True,
    )
)
```

此模式只处理 Memory，并优先于 `assembly` 和 `include_code` 生效；Runtime 不为该请求
召回 Code、Topic Memory、Experience 或 Profile。作用域授权与精确 Memory revision
校验仍然生效。选择器本身不调用模型或网络，也不读取外部语料和标注。HTTP/MCP 请求契约
保持现状，该选项属于原生 Python API。

## 原文片段与完整预算

`PreparedContextBuilder.build_scopes_result` 的结果通过 `PreparedContextBuild.fragments`
提供片段元组。每个 `PreparedEvidenceFragment` 包含：

- `address`：`MemoryEntryAddress`，包含 scope、Memory revision、entry ID 和 entry version ID。
- `start`、`end`：原 entry 正文的半开 Python 字符坐标。
- `text`：严格等于 `entry.text[start:end]`，保留原始空白。

`origins` 与 `fragments` 的数量、顺序和引用逐项对应。候选按完整 scope、revision 和
entry/version 身份去重；不同 scope 的相同正文保留各自身份。同一 entry 可以贡献多个
独立片段，已选的相邻或重叠区间在输出前合并。

非空输出保留原有 trust policy，以及 `BEGIN_POWERCONTEXT_PREPARED_CONTEXT_V1` 和
`END_POWERCONTEXT_PREPARED_CONTEXT_V1` envelope。中间 JSON 包含
`trust="untrusted_history"` 和 `items`；每项包含 `citation`（`scope_id`、`memory_ref`、
`entry_id`、`entry_version_id`）、`content`、`start`、`end`。JSON 转义仅用于传输，
解码后的证据仍是完整原文切片。

每次尝试装入片段，都会测量完整渲染结果，包括 trust 文本、envelope、JSON、作用域与
引用身份、字符坐标和 UTF-8 正文。`content_bytes` 等于最终输出的 UTF-8 字节数，且不超过
`max_bytes`。没有相关片段能装入时，返回合法空 Context：`content=None`、
`content_bytes=0`，origins 和 fragments 均为空。关闭此模式时，fragments 总为空元组。

原生增量 Pack 接口也可接收开启证据选择的请求，打包同一份最终正文，并保留已有完整重建契约。

## 选择策略与工作量

选择器使用查询词项、轻量英文词形归一化、当前候选集合内的句子和文档逆频率，以及按长度
归一化的词频进行排序。具有标识符形态的查询词获得额外权重，相邻句子获得少量上下文分值，
以保留解释性后文。这些变换仅用于排序，输出内容不做改写。

无法单独装入预算的长句会在附近单词边界分段，避免只能返回长段落的开头。贪心预算搜索
考虑分值最高的 96 个单元，并比较相关性与新增 wire 成本；相邻单元合并后共用引用开销。
候选与字符区间采用稳定顺序，结果可确定性复现。

排序会扫描传入候选的全部正文，沿用既有 Memory 候选上限；预算探索限制在 96 个单元和
既有输出条目上限内。每次探索都进行实际序列化测量，因此相比前缀截断会增加 CPU 工作。
词项策略不能保证任意查询的语义蕴含关系或证据覆盖率。

## 独立公开数据质量检查

`tests/fixtures/evidence_selection/scifact-dev.json` 包含 24 条原始公开 SciFact development
claim，每条配三篇原始摘要。fixture README 记录来源、许可、确定性的干扰文档选择方式，
以及独立的 gold 预算可行性检查。Gold 区间仅保留在测试中，不传给选择器，也不写入产品源码。

补充回归在 1400 和 1800 bytes 两档预算下执行 48 次请求。计量时按原文档合并已选区间，
重复字符只计算一次，再用小写 `[a-z]+|\d+` 词频多重集合计算与公开 gold 的交集。
完整覆盖要求覆盖全部 gold 字符区间，包括原有空白。

Python 3.12.14、Windows 的实测结果：

| 指标 | 结果 | 要求门槛 |
| --- | ---: | ---: |
| 平均 evidence token recall | 68.45% | 65% |
| 完整证据请求 | 26 / 48（54.17%） | 40% |
| 查询与预算 | 24 × 2 | 48 次请求 |

这是补充的公开 development 数据测量，与比赛三项公开验收及隐藏的 48 次质量测量相互独立，
不能替代隐藏测评结论。

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -s \
  -p pytest_asyncio.plugin tests/builtin/runtime/test_evidence_selection.py \
  tests/e2e/test_evidence_selection.py
make -C competition test-evidence
```
