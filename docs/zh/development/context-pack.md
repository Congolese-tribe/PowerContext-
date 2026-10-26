---
title: 上下文增量传输
description: 将完整正文序列化为 Context Pack，并通过校验后的前缀增量完成重建。
---

# 上下文增量传输

`ScopedContextApplication.prepare_incremental(request, base_pack=None)` 通过既有
`prepare` 取得本轮正文，完整保留其中的引用。未提供 base 时返回 `ContextPack`，
提供完整 base 时返回 `ContextPackDelta`。合法的 `content=None` 对应空 pack。
原有作用域授权和正文准备规则继续生效；可选的 `authorize_scopes` 回调直接传给 `prepare`。

```python
from powercontext.builtin.runtime import (
    ContextPack,
    ContextPackDelta,
    PrepareContextRequest,
    apply_context_pack_delta,
)

# 发送端与接收端仅通过序列化字节交换数据。
request = PrepareContextRequest(query="remote", max_bytes=8192)
first = await context.prepare_incremental(request)
receiver = ContextPack.from_bytes(first.to_bytes())
update = await context.prepare_incremental(request, base_pack=receiver)
receiver = apply_context_pack_delta(
    receiver, ContextPackDelta.from_bytes(update.to_bytes())
)
body = b"".join(receiver.items).decode("utf-8")
```

## 传输契约

公开类型也可从 `powercontext.builtin.runtime.context_pack` 导入。
`ContextPack.build(items)` 保留传入的 UTF-8 bytes 元组及分块边界。
`ContextPack.from_content(content, chunk_bytes=512)` 按容量贪心分块，保持 UTF-8 字符完整。
上限必须为正整数，不接受布尔值；单个字符无法容纳时抛出 `ContextPackError`。
空文本不产生任何分块。

两种 wire 对象均使用 UTF-8 JSON，key 排序，分隔符为 `,` 和 `:`，分块采用标准规范
Base64 字符串数组。完整 pack 仅包含 `schema`、`items`、`digest`、`byte_count`，
schema 为 `powercontext.context-pack.v1`。`sha256:<hex>` 摘要对仅含 `schema` 和
`items` 的上述 JSON 求 SHA-256。`byte_count` 包括自身字段及十进制位数，等于
`len(pack.to_bytes())`。

Delta 仅包含 `schema`、`base_digest`、`target_digest`、`prefix_items`、`appended`，
schema 为 `powercontext.context-pack-delta.v1`。其 `byte_count` 属性为
`len(delta.to_bytes())`，不写入 JSON。发送端选择最长公共分块前缀；接收端支持包括零
在内的任意有效前缀，将保留前缀与新增后缀拼接，并校验 base 和目标摘要，包括目标的精确分块边界。

缺少、额外或重复字段，错误 schema，非法 UTF-8 或 Base64，非法前缀类型或越界，
错误字节数及摘要不符均抛出 `ContextPackError`。解码器允许 JSON 空白和 key 顺序变化，
完整 pack 的 `byte_count` 按规范序列化结果校验。分块和 delta 均不可变；重建不依赖
发送端缓存或持久化 Artifact。本接口属于原生 Python Runtime；HTTP 和 MCP 契约提供普通正文准备接口。

打包与校验的工作量随正文长度线性增长，公共前缀比较最多遍历较短 pack 的分块数。
复用可减少重复正文的传输字节，每轮仍执行普通正文准备和目标摘要计算。靠近正文首部的
变化可能要求重传大部分分块。

## 可复现的成本测量

`tests/e2e/test_context_pack.py` 实际运行原生 SQLite 采集、Memory flush、FTS 检索和
普通正文准备。确定性提取管线原样保留来源 turn，不调用模型。
QMSum 片段和 MIT 许可保存在 `tests/fixtures/context_pack/`。
这组序列是补充的本地回归，与比赛公开验收和隐藏成本序列相互独立。

数据取自 QMSum commit `83d7768c1f2b4dfeb091385d3dc7e239b8e5bb7e` 的 ES2004a，
保留十二个 turn 的 speaker、完整正文和原始顺序。首先写入八个 turn；在从零编号的
第 8、18、28、38 轮之前分别追加一个 turn 并 flush。检索词依次为 `remote`、
`control`、`mobile`、`buttons`，每个使用十轮，正文预算为 8192 bytes。
四十轮结果必须非空，每条 wire 消息必须重建普通正文及完整目标 pack。

Python 3.12.14、Windows、原生 SQLite 的实测结果：

| 计量对象 | 字节数 | 增量传输相对此项的节省率 |
| --- | ---: | ---: |
| 普通正文 UTF-8 | 55,662 | 54.39% |
| 每轮完整 v1 pack | 80,876 | 68.61% |
| 每轮独立 gzip | 29,580 | 14.16% |
| 首次完整 pack 加 39 个 delta | 25,390 | — |

本次产生八种不同 Context。首次完整包、Base64、摘要及全部协议字段均计入传输字节。
gzip 对每轮正文独立调用 `gzip.compress(content, compresslevel=9, mtime=0)`。
各节省率均为 `1 - incremental_bytes / comparison_bytes`，负值表示增量协议更大。
这组结果不代表隐藏测评结论，也不表明相对 gzip 的一般压缩优势。

```bash
PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q -s -p pytest_asyncio.plugin \
  tests/builtin/runtime/test_context_pack.py tests/e2e/test_context_pack.py
make -C competition test-incremental
```
