# Public SciFact development evidence fixture

Source: [AllenAI SciFact](https://github.com/allenai/scifact/tree/68b98a56d93e0f9da0d2aab4e6c3294699a0f72e),
David Wadden, Shanchuan Lin, Kyle Lo, Lucy Lu Wang, Madeleine van Zuylen, Arman Cohan,
and Hannaneh Hajishirzi, *Fact or Fiction: Verifying Scientific Claims*, EMNLP 2020.

The public release is linked by that commit's
[download script](https://github.com/allenai/scifact/blob/68b98a56d93e0f9da0d2aab4e6c3294699a0f72e/script/download-data.sh).
Archive URL: <https://scifact.s3-us-west-2.amazonaws.com/release/latest/data.tar.gz>.
Downloaded archive SHA-256:
`11c621288d41ac144d29b13b0f8503b3820b7d6e8b1f6ff24dff335c196d76be`.

This supplemental fixture uses public `claims_dev.jsonl` and `corpus.jsonl`, with no
hidden competition annotations. Claims and original abstracts retain their identifiers.
Each candidate text is the original title, one space, and the original abstract sentences
joined by one space. Characters inside titles and sentences are unchanged. Gold character
offsets are calculated from the original sentence indices, merging adjacent gold sentences.
All public evidence annotations for the claim are included in the gold union.

Select the first 24 evidence-bearing development claims in original order that have
at most three gold documents and whose complete gold union plus the required wire
envelope and realistic-length citations fits 1400 bytes. The feasibility check is
independent of the selector. Each claim receives exactly three candidate abstracts.
Fill unused slots with corpus documents ranked by query-token overlap divided by the
fourth root of the document's distinct-token count, with document ID as tie-breaker.
This distractor tokenizer uses lowercase `[a-z]+|\d+` and removes
`a an the in on at of to is are was were and or for from by with this that`.
Finally, sort candidate documents by the SHA-256 of `claim_id:document_id`, avoiding a
fixed position for gold documents. Claim IDs, document IDs, gold intervals, and labels
are never sent to product selection code.

The test uses realistic-length scope, entry, and entry-version identifiers. There are
24 distinct original claims and 48 requests at 1400 and 1800 bytes. Scoring merges
overlapping/adjacent intervals per original document and uses token multisets, while
complete coverage checks all gold characters. The JSON's `gold_wire_bytes` records the
independent feasibility calculation; tests recompute the complete gold wire payload.

Licensing: original claims and evidence annotations are **CC BY 4.0**; original abstracts
come from **S2ORC**, licensed under **ODC-By 1.0**. Derived candidate ordering, concatenation,
and character coordinates are the transformations described above. Upstream licensing
is preserved in [LICENSE.md](LICENSE.md); this data is not relicensed as product code.
