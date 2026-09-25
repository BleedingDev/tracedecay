---
name: pm-conformance-ncm
overview: Run the same common compatibility program against the production NCM worker and real model.
todos:
  - id: real-ncm-common-factory
    content: Build the concrete NCM factory with actual worker lifecycle and physical legacy/corruption evidence, then require common compatibility without unresolved evidence.
    status: in_progress
isProject: false
---

# pm-conformance-ncm

Own one NCM common factory integration-test module. WorkerOptions::default and from_production_worker are mandatory; never HashEncoder/test_double. Share only installed model files; each scenario owns mutable state. Use real existing worker start/stop/reap and post-commit reply withholding.
