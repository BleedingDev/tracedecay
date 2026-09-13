# Independent original Native build result

The reviewed `rn-build-reference` build completed successfully in Cargo Hauler ticket `cc-1448`.

## Source and build identity

- Reference checkout: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60`
- Source revision: `57006f60cb45bcee8487e73a40d4fad1a12ee2b6`
- Source tree: `af7edbf22b814c011d9581c5adba58befefe5a45`
- Source status: clean detached checkout
- Source subject: `fix(cli): preserve linked worktree scope`
- Cargo.lock SHA-256: `8705049aea42a8495b59c6950285316b3b62738cf4233c8b55503445fac65c7d`

The build command was:

```text
cargo build --locked -p tracedecay-cli --bin tracedecay --features test-transport
```

The effective features were `default`, `production`, and `test-transport`. The product-only `memory-provider-host` feature was absent and was not enabled. The ticket exited `0` after `21m20s`; the resulting profile was `dev (optimized + debuginfo)`.

## Binary and evidence

- Binary: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60/target/debug/tracedecay`
- Binary size: `569418488` bytes
- Binary SHA-256: `a655906992e02eb2ede7666d8b8e4d3a7e0e9d3fb60e3e91c72662399503936d`
- Version probe: `tracedecay 0.1.0-beta.37+57006f60cb45bcee8487e73a40d4fad1a12ee2b6`
- Build log: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60/target/task-scratch/native-original/reference-build/cc-1448.log`
- Build log size: `26977` bytes
- Build log SHA-256: `38b6c6c6d9bd79225d6faa51adac8b3b7fa4fa38c4269f71c45e589a58a08f87`
- Build metadata: `target/task-scratch/native-original/reference-build/build-metadata.json` in the detached reference checkout

The build used the isolated test-owned data root:

```text
/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60/target/test-profile/.tracedecay
```

The authoritative binary, metadata and log bundle lives under the detached checkout's `target` path above. The active product checkout's `target/task-scratch/native-original/reference-build` currently contains older diagnostics and is not evidence for this build.

## Bounded warnings

The successful build emitted four bounded warnings:

1. Unused import `std::fs` in `crates/tracedecay-search-eval/src/candidate_output/peak_rss.rs:3:5`.
2. Dead enum `ResidentMemoryLogTransitionV1` in `crates/tracedecay/src/daemon/maintenance.rs:867:6`.
3. Dead method `ResidentMemoryLogStateV1::observe` in `crates/tracedecay/src/daemon/maintenance.rs:875:8`.
4. Linker warning that the `__eh_frame` section was too large (maximum 16 MB) for compact unwind offsets, so exception-handling performance may be affected.

These warnings did not change the successful exit status. The binary is ready for the accepted Native comparison runner to consume; no comparison was run as part of this build node.
