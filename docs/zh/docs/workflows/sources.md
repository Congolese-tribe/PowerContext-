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
