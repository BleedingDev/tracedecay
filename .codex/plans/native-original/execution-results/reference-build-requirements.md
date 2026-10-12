# Root-reviewed original build requirements

Source b3b43410e47115056f2066449aafa1822bbb6049, clean detached reference checkout named in rn-build-reference. Root inspected the exact b3 crates/tracedecay-cli/Cargo.toml: package tracedecay-cli, binary tracedecay, default production feature, test-transport forwards to tracedecay/test-transport. The original production CLI supplies tool and daemon entry points consumed by the runner. Product-only memory-provider-host does not exist at b3 and is not enabled there.

Requested broker build: cargo build -p tracedecay-cli --bin tracedecay --features test-transport, preserving default features. Follow Cargo ownership, diagnostics, broker and target/data rules. Publish binary hash, source identity, features and build logs. Probe only CLI help/version in isolated environment if necessary; runtime comparisons remain blocked on accepted runner and fixtures. No source patch, substitute binary or parity claim.

Root separated this independent build from runner authoring after reviewing the manifest, to avoid serializing compilation behind Python lifecycle work. rn-source-docs is accepted and supplies source-baseline evidence. Any additional feature requirement must be reported, not silently patched into original source.
