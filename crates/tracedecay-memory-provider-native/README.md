# TraceDecay Native Memory Provider Adapter

This product-owned crate places the existing TraceDecay Native memory application behind the provider-neutral `MemoryProvider` boundary. It owns no Native data or algorithms.

The adapter:

- accepts only a port that declares the stable `tracedecay.native` identity;
- retains validated immutable identity, schema, protocol, capability, and limit fields while refreshing only monotonic Native state generation;
- revalidates complete public handshake and operation envelopes, including the exact canonical payload contract for every operation, before any Native application-port contact;
- routes mandatory health and recall calls without rewriting canonical payload or exact scope;
- validates generic observation envelopes and classifies them into exactly two typed variants before forwarding them unchanged to the trusted Native application port: the explicitly authorized `native.fact_promoted.v1` promotion, and `session.message_committed.v1`, which the port stages as provider-local advisory state. Every other contract-known kind is refused with `capability_unsupported` / `native.observation_unsupported` before the port is reached;
- opens no store of its own for either variant: staging is a durable consequence owned entirely by the application port behind this boundary;
- requires that application authority to reject non-equivalent observations and authorize promotion into canonical Native facts only through a typed Native path that preserves owner, provenance, trust, temporal state, idempotency, and durable receipts; a staged session row is never a canonical fact;
- routes only declared optional lifecycle capabilities through operation-specific feedback, maintenance, inspection, correction, deletion-by-source, snapshot, and replay methods;
- delegates dispatched-operation terminal records, provenance, receipts, scoring, and temporal state to the Native application authority while constructing zero-contact boundary rejections locally;
- contains no TraceDecay database, store, graph, code-index, daemon, host, dashboard, transport, NCM, or OCEAN dependency.

M2 proves the boundary with a mock application port. M3 supplies the real owner-bound TraceDecay application bridge and direct-versus-provider parity journeys. No second fact store, score implementation, curation path, or persistence format is introduced here.
