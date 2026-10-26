---
title: Sources 与采集
description: 采集证据、查看 Source 记录并配置后续处理。
---

# Sources 与采集

Sources 在信息被筛选为知识之前保留证据。Content Source 保存采集的文本，其他 Source 类型可引用 Connector 提供的材料。
Work Contract 和 Task Outcome 也会保留 Source 证据。

## 采集与检查

1. 选择已有 Scope。[API Quick Start](../develop/api-quickstart.md)演示通过 `POST /v1/sources/content` 采集内容，
   并保留返回的 Source 引用。
2. 使用 `GET /v1/scopes/{scope_id}/sources` 浏览 Source journal，再通过
   `GET /v1/scopes/{scope_id}/sources/{source_type}/{source_id}` 读取选中的 Source。
3. 将符合条件的证据处理为 Memory，需要配置生成模型，并启用定时处理或显式执行 Memory flush。
   步骤见[启用提取与向量搜索](../get-started/configure-models.md)。

采集成功表示证据已保存，不表示提取成功，也不表示内容一定值得长期保留。
通过 Source 引用追溯生成结果的输入。不要在同一身份下修改内容并重试采集，将其当作新的观察。

## 选择采集入口

- Agent Prompt Hook：在[接入指南](../integrations/index.md)中选择宿主对应的采集开关。
- 文件与对象存储：使用 [OpenDAL Connector](ingest-text-files-with-opendal.md)。
- Scope Profile 的主体证据：按 [Scope Profile](use-profiles.md)写入。
- 应用或评测事件：使用应用对应适配器；[Bub 采集](../integrations/evaluation.md)需要显式启用。

Source 读写受所选 [Scope 与访问策略](scopes-and-access.md)约束。

## 按主键采集 CSV 行

`powercontext.sources.csv_rows` 接收离线 UTF-8 CSV 字节，可带文件头 BOM，按主键将每行保存为独立的
CAPTURED Source，无需新增依赖。整份文件验证成功后才返回结果；编码错误、引号格式错误、表头或主键
重复或空白、行宽不一致均以 `ValueError` 失败。仅忽略表头之后完全空的物理记录，只有表头时返回 `()`。

```python
from pathlib import Path

from powercontext import CSV_ROW_SOURCE_DEFINITION, CSVDocumentInput, parse_csv_document
from powercontext.builtin.runtime import SubmitSourceObservation
from powercontext.sources import SourceDefinitionRegistry, manifest_for_definition, project_source_for_transport


async def capture_csv(runtime, scope_id: str, path: Path, document_id: str, primary_key: str):
    rows = parse_csv_document(CSVDocumentInput(
        document_id=document_id, primary_key=primary_key, content=path.read_bytes(),
    ))
    registry = SourceDefinitionRegistry((CSV_ROW_SOURCE_DEFINITION,))
    await runtime.ingestion.register(manifest_for_definition(CSV_ROW_SOURCE_DEFINITION))
    for row in rows:
        source = await registry.resolve(row)
        await runtime.ingestion.submit(SubmitSourceObservation(
            scope_id=scope_id,
            observation=project_source_for_transport(registry, source),
        ))
    return rows
```

列名、主键值和所有字段均保留原字符串、空格及列序，不转换数字、日期或布尔值。选择 `" id "` 这样的
列名时，`primary_key` 必须完全匹配。`document_id` 必须非空且没有首尾空白。行身份为
`csv:{encoded_document_id}:{encoded_primary_key_value}`，两个组件分别使用
`urllib.parse.quote(component, safe="")` 编码；超过 256 个字符的身份会被拒绝。重排行或修改非主键字段
不会改变行身份。

标准 `powercontext.text-evidence` 投影按原列序生成紧凑的 Unicode JSON，metadata 恰好包含
`document_id`、`primary_key`、`primary_key_value`、`columns` 和完整的 `row` 对象。
Memory pipeline 需要将每个投影及其 Source 引用保存为条目，并在检索前完成 flush。
回查搜索结果时，使用结果的 `memory_ref`、`entry_id`、`entry_version_id` 构造完整 `MemoryCitation`，
传给 `GetMemoryEntryRequest(citation=...)`。Runtime 持久化后，重启仍可通过精确引用读取该条目和完整行证据。

## 采集 PDF 全部物理页

安装 `powercontext[builtin,pdf]` 后可使用 PDF 页级 Source，其中 PDF 扩展固定使用 `pypdf==6.19.0`。
解析器接收完整原文件字节，按物理顺序返回所有页面，包括空白页；保留可提取正文与表格数字，不执行 OCR。
损坏、无法读取或需要非空密码的文件以 `ValueError` 失败，不返回部分页面。带空密码权限标记的文件可以读取。

```python
from pathlib import Path

from powercontext import PDF_PAGE_SOURCE_DEFINITION, PDFDocumentInput, parse_pdf_document, search_pdf_pages
from powercontext.builtin.runtime import GetMemoryEntryRequest, SubmitSourceObservation
from powercontext.sources import SourceDefinitionRegistry, manifest_for_definition, project_source_for_transport


async def capture_report(runtime, scope_id: str, path: Path, document_id: str):
    pages = parse_pdf_document(PDFDocumentInput(document_id=document_id, content=path.read_bytes()))
    registry = SourceDefinitionRegistry((PDF_PAGE_SOURCE_DEFINITION,))
    await runtime.ingestion.register(manifest_for_definition(PDF_PAGE_SOURCE_DEFINITION))
    for page in pages:
        source = await registry.resolve(page)
        await runtime.ingestion.submit(SubmitSourceObservation(
            scope_id=scope_id,
            observation=project_source_for_transport(registry, source),
        ))
    return pages


async def retrieve_pages(memory, question: str):
    citations = await search_pdf_pages(memory, question, limit=20)
    return tuple([
        (citation, (await memory.get(GetMemoryEntryRequest(citation=citation))).entry)
        for citation in citations
    ])
```

Memory 提取 pipeline 需要将每页保存为单独条目并保留 Source 引用，处理完成后才能检索；
仅提交 Source 不会自动创建可搜索的 Memory 条目。将 `runtime.memory.for_scope(scope_id)` 传入
`retrieve_pages`，也可以在关闭并重新打开 Runtime 后搜索。检索使用全文索引、通用财务词汇、报表标题及
本地词频重排，最多返回 `limit` 个不同页的精确引用。非正上限或没有可搜索词项的问题返回 `()`。

页身份由完整的 `(document_id, document_sha256, page_number)` 元组计算摘要产生；SHA-256 覆盖整份原 PDF
的字节。修改 PDF 会产生新身份，旧引用继续指向原有证据。标准文本投影为 `document_id + "\n" + page.text`，
metadata 恰好包含 `document_id`、`document_sha256`、`page_number`。调用方应按摘要保留原 PDF，
再用从 1 开始的物理页码定位证据，不能将其当作财报印刷页码。检索不读取 PDF、标注或外部服务。
