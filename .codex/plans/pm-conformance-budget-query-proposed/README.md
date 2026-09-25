# Budget fixture draft

`budget-query.delta.patch` changes only the common budget scenario's synthetic source text and query/objective, plus its existing fixture regression. Both independent long sources repeat the same Unicode sentence, have distinct trailing content, and receive the same full/bounded query. All budgets, expectations, original-source digest checks and truncation assertions are unchanged.

Preserved diagnostic: the original NCM cc1363 `budgets.full` query `compatibility beacon` produced zero matches for the prior long sources with unrelated repeated tails. It did not establish the positive retrieval prerequisite needed to assess the budgets. This draft changes the synthetic budget probe; it does not establish a retrieval fix or change that observed result to a pass. Actual provider validation remains for the lead's next run.

No model, threshold, ranking algorithm, provider policy, held-out data or adapter behavior is changed. `rustfmt` and `git apply --check` pass. No Cargo or live patch application was performed by this worker.
