# Memory Fabric dependency policy

`dependency_direction_rules` in `product/upstream/patch-footprint-policy.json` define forbidden crate edges for the provider API, fabric-adjacent product crates, concrete adapters, transports, and upstream packages.

`scripts/product/check-memory-dependency-policy.py` reads every `crates/*/Cargo.toml`, including normal, development, build, and target-specific dependencies. CI fails when a package selected by a rule imports a forbidden package.

## Exceptions

Dependency exceptions are denied by default. A temporary exception must be one row in `dependency_direction_exceptions` and must name exactly one rule, one source package, and one dependency package. Globs are forbidden. Every active row must carry a product ADR path, a non-empty rationale, reviewer identity, and active/retired status. An active row is rejected when the exact forbidden edge no longer exists, preventing stale exemptions. A retired row never suppresses an edge.

The repository currently has no dependency exceptions.
