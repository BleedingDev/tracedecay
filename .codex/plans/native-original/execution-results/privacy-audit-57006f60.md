# Current 570 privacy audit gate

Date: 2026-09-13

Active original reference: `57006f60cb45bcee8487e73a40d4fad1a12ee2b6`.
Candidate reviewed: `1fe250fed7ca615f1dfdcd580befeb276328e910`.

The current detector comparison is equal: `crates/tracedecay-privacy/src/detector_kernel.rs` has blob `9ce4488a34c5bd121d43c35c33f1daf925ab38ba` at both revisions, and the root-supplied `git diff --exit-code` returned exit `0`.

The typed Claude history source-field seam remains pending review. The audit must trace admission, source identity/replacement checks, no-follow bounds and no-write deferral before any privacy gate is accepted. The earlier b3-to-571 privacy report is historical and is not relabeled by this result. No detector or product source was edited for this gate.
