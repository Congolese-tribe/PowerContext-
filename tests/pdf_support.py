# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Small original PDFs with controlled physical pages and extractable text."""

import io

from pypdf import PdfWriter
from pypdf.generic import ArrayObject, DecodedStreamObject, DictionaryObject, NameObject, NumberObject


def build_pdf(*texts: str, password: str | None = None, damaged_page: int | None = None, title: str = "") -> bytes:
    writer = PdfWriter()
    writer.add_metadata({"/Title": title})
    for number, text in enumerate(texts, start=1):
        page = writer.add_blank_page(width=612, height=792)
        if text or number == damaged_page:
            page[NameObject("/Resources")] = DictionaryObject({
                NameObject("/Font"): DictionaryObject({
                    NameObject("/F1"): DictionaryObject({
                        NameObject("/Type"): NameObject("/Font"),
                        NameObject("/Subtype"): NameObject("/Type1"),
                        NameObject("/BaseFont"): NameObject("/Helvetica"),
                    })
                })
            })
            lines = [line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)") for line in text.splitlines()]
            operations = "BT /F1 12 Tf 14 TL 30 750 Td " + " T* ".join(f"({line}) Tj" for line in lines) + " ET"
            content = DecodedStreamObject()
            content.set_data(b"BT (unterminated" if number == damaged_page else operations.encode())
            page[NameObject("/Contents")] = writer._add_object(content)
    if password is not None:
        writer.encrypt(user_password=password, owner_password="owner-secret")  # noqa: S106
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def build_form_pdf(*, damaged: bool = False) -> bytes:
    writer = PdfWriter(io.BytesIO(build_pdf("First page", "Before form")))
    page = writer.pages[1]
    form = DecodedStreamObject()
    form.update({
        NameObject("/Type"): NameObject("/XObject"),
        NameObject("/Subtype"): NameObject("/Form"),
        NameObject("/BBox"): ArrayObject([NumberObject(value) for value in (0, 0, 612, 792)]),
        NameObject("/Resources"): page["/Resources"],
    })
    form.set_data(b"BT (broken" if damaged else b"BT /F1 12 Tf 30 720 Td (Text inside form 456) Tj ET")
    resources = page["/Resources"]
    assert isinstance(resources, DictionaryObject)
    resources[NameObject("/XObject")] = DictionaryObject({NameObject("/Form1"): writer._add_object(form)})
    content = DecodedStreamObject()
    original = page.get_contents()
    assert original is not None
    content.set_data(original.get_data() + b"\n/Form1 Do\n")
    page[NameObject("/Contents")] = writer._add_object(content)
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()
