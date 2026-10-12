# Bound the removal of the substitute Native implementation

Read research-common.md in this directory first.

Question: Map every production path using the added Native staged store and scorer. Propose exact adapter and composition edits to remove that substitute from Native while preserving the original direct operation and avoiding dead routes.

Inputs: crates/tracedecay-memory-provider-native/**; crates/tracedecay/src/daemon/retained_owner/native_provider*.rs; native_staged_observations.rs; current callers.

Sole output file: ../evidence/native-adapter.md.

Do not edit any other file. Do not implement your proposal. Do not duplicate other reports. Send root the output path, the strongest evidence and any unresolved dependency when finished.
