# Determine saved-data and lifecycle handling

Read research-common.md in this directory first.

Question: Identify state written by original Native versus the added store. Determine a non-destructive cutover for real persisted state, restart, existing receipts and settings without inventing a migration absent release/use evidence. No operator databases may be opened.

Inputs: native_staged_observations.rs; existing original memory/session store schemas and lifecycle; product/upstream and distribution history; source-defined state roots only.

Sole output file: ../evidence/saved-data.md.

Do not edit any other file. Do not implement your proposal. Do not duplicate other reports. Send root the output path, the strongest evidence and any unresolved dependency when finished.
