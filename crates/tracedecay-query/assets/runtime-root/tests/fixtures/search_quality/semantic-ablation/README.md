# Semantic ablation fixture

The workload and artifact describe an evaluator-owned, opaque 18-candidate
corpus with separate train and validation queries. `labels-v1.json` is the
held-out answer key: the production adapter receives the workload, corpus, and
identity pins, while the evaluator supplies labels only after rankings return.
The artifact contains identity and manifest pins only; it has no ranking or
label data.

Every query has at least ten eligible candidates, and candidate metadata has no
production-token overlap with query text. The fixed matrix requires ten cold,
warm, and restart repetitions for lexical-baseline, semantic-only, and hybrid
modes. Quality uses Recall@10 and a fixed ten-item precision denominator.
