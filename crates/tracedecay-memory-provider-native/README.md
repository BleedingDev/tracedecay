# TraceDecay Native Memory Provider Adapter

`tracedecay.native` is upstream TraceDecay memory inside the provider host. Selecting Native is observationally identical to running TraceDecay with no provider host: the same facts, ranking and scores, the same session results, and the same `tracedecay_context` output.

Facts via `MemoryApplication`, sessions via the `message_search` kernel; no staging.

The adapter:

- accepts only a port that declares the stable `tracedecay.native` identity;
- retains validated immutable identity, schema, protocol, capability, and limit fields while refreshing only monotonic Native state generation;
- revalidates complete public handshake and operation envelopes, including the exact canonical payload contract, before any Native application-port contact;
- projects the application-port descriptor to exactly two capabilities: `provider.health.v1` and `recall.query.v1`. Native declares no observation, feedback, maintenance, inspection, correction, deletion, snapshot, or replay capability, and every such call is refused with `capability_unsupported` before the port is reached;
- routes health and recall without rewriting canonical payload or exact scope, and requires every dispatched reply to claim no committed effect.

The application port behind this boundary answers recall from upstream authorities only:

- **Facts** (objectives `search`, `probe`, `related`, `reason`): the owner-bound `MemoryApplication` read for the project owner, then the profile owner, built with the same `memory_mapping` query helpers as the `tracedecay_fact_store_*` tools. Each candidate carries the upstream `FactSearchHitV1` and its `score_millionths` unchanged, attests `project_facts` or `profile_facts`, and has `memory_class = "fact"`. Hits are deduplicated by `fact_id` and then content. Recall is a read, so it records no retrieval telemetry.
- **Session history** (objective `session_history`, explicit only): the upstream `tracedecay_message_search` temporal query over every session in the authorized project root. The kernel page order, score, freshness, partial or stale outcome, and cursor are carried verbatim with `memory_class = "session_message"`. Reads never refresh or ingest.
- Only current-state recall exists upstream; `as_of`, `interval`, and `history` return `capability_unsupported` (`native.recall_semantics_unsupported`) and never fall back to another read.

The crate contains no TraceDecay database, store, graph, code-index, daemon, host, dashboard, transport, NCM, or OCEAN dependency. No fact store, score implementation, curation path, staging table, cursor format, or persistence format is introduced here.
