# Draft replay input correction

UNAPPLIED; separate from the frozen legacy-evidence amendment. Apply only after
root review. This patch is based on the 192-action legacy candidate whose patch
SHA-256 is `f2c7c11c1592ea45a486778d0265a79a4726e7a96d6fb204db0f8428fae9efec`.
It changes only `compatibility.rs` and the common-profile protocol explanation.
The copied legacy helper is identical on both sides and is absent from this diff.
No Cargo/model/provider runs or live-source edits were performed.

The existing 190-action cc1384 report and artifacts remain unchanged. This is an
explicit fixture input correction; it is not evidence that a provider passed.
It neither adds nor removes actions from the proposed 192-action program.

## Exact changes

For both current and fresh snapshot-restore scenarios:

- The successful replay page contains deleted source sequence 1 and available
  replay-only source sequence 2. It starts at the actual initial replay cursor 0
  and expects applied=1, rejected=1, source-already-applied=0.
- The subsequent fresh-key replay repeats that same full page. A ReplyBinding
  takes `/acknowledged_sequence` from the actual preceding `replay_positive`
  reply into `/expected_previous_acknowledged_sequence`. It expects applied=0,
  source-already-applied=1, rejected=1, no effect and unchanged generation.

The predelete replay after restart contains the previously observed available
control at sequence 1 and the deleted target at sequence 2. It expects applied=0,
source-already-applied=1, rejected=1, no effect and unchanged generation. The
original control-visible, deleted-absent, refusal and receipt assertions stay.

The existing replay/history-grant helpers now take each original observation
paired with its explicitly authored SourceDisposition. They copy unchanged source
attribution and settled receipt references. The exact trusted fixture authority
still independently resolves current source disposition from its own inventory
and actual host RecordSourceDisposition actions. The unrelated authority scenario
uses the same available state and produces the same grant/replay JSON as before.
No grant authorizes access merely because this helper places it in JSON.

## Protocol evidence and coordination

The published lifecycle contract requires replay sequence monotonicity, rejects
sequence gaps, compares expected previous acknowledgement, and reports the actual
acknowledged sequence. The original fixture sent sequence 2 at previous ack0
after an Observe/restore or a rejected no-change replay; those operations do not
establish a replay acknowledgement. The original gap refusal remains valid.

The NCM replay owner confirmed `/acknowledged_sequence` is the actual public
response pointer. A successful contiguous [deleted1,available2] page returns2;
repeating it with actual previous2 preserves ack2. The predelete no-change page
[available1,deleted2] at previous0 preserves ack0. Runtime/adapter replay changes
remain owned by that agent. This proposal does not relax gap, cursor CAS, privacy,
receipt identity, acknowledgement or generation requirements.

## Validation

One new pure fixture regression checks all seven affected replay calls: contiguous
source order and receipt membership, immutable grant attribution, each source's
current disposition from earlier host actions, exact result accounting, preserved
no-effect/generation expectations, and the actual-reply cursor binding. Formatting
and patch checks pass; compilation and real provider runs remain root-owned.
