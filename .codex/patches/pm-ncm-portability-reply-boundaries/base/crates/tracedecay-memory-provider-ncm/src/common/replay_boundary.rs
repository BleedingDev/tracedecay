//! Narrow validation for inspectable canonical replay partial outcomes.

use super::*;
use crate::NcmSurfaceCall;
use tracedecay_memory_provider_api::ProviderOperation;

pub(crate) fn valid_replay_payload(surface: &NcmSurfaceCall, reply: &ProviderReply) -> bool {
    if surface.operation != ProviderOperation::Replay {
        return false;
    }
    let Some(payload) = reply.payload.as_ref() else {
        return false;
    };
    let Ok(request) = serde_json::from_slice::<Value>(&surface.payload.bytes) else {
        return false;
    };
    let Ok(response) = serde_json::from_slice::<Value>(&payload.bytes) else {
        return false;
    };
    validate(
        &request["common_portability"],
        &response,
        surface.expected_state_generation,
        reply.state_generation,
        reply.terminal.terminal_code(),
        reply.terminal.committed_effect().state(),
    )
    .is_some()
}

fn validate(
    request: &Value,
    response: &Value,
    before: u64,
    after: u64,
    terminal: TerminalCode,
    effect: crate::CommittedEffectState,
) -> Option<()> {
    fields(
        response,
        &[
            "common_portability",
            "first_source_sequence",
            "last_source_sequence",
            "acknowledged_sequence",
            "state_generation_before",
            "state_generation_after",
            "applied_observations",
            "duplicate_observations",
            "sources_already_applied",
            "rejected_observations",
            "effect_unknown_observations",
            "partial",
            "replayed",
            "warnings",
            "items",
        ],
    )?;
    if request["action"] != "replay" || response["common_portability"] != "replay" {
        return None;
    }
    let requested = request["items"].as_array()?;
    let rows = response["items"].as_array()?;
    if requested.is_empty()
        || requested.len() > 4096
        || rows.len() != requested.len()
        || !response["warnings"].as_array()?.is_empty()
    {
        return None;
    }
    let replayed = response["replayed"].as_bool()?;
    let partial = response["partial"].as_bool()?;
    let stored_before = response["state_generation_before"].as_u64()?;
    let stored_after = response["state_generation_after"].as_u64()?;
    if stored_after < stored_before
        || (!replayed && (stored_before != before || stored_after != after))
        || (replayed && (stored_after > after || effect != crate::CommittedEffectState::Duplicate))
    {
        return None;
    }
    for field in ["first_source_sequence", "last_source_sequence"] {
        if request[field] != response[field] {
            return None;
        }
    }
    let acknowledged = response["acknowledged_sequence"].as_u64()?;
    let previous = request["expected_previous_acknowledged_sequence"].as_u64()?;
    let last = request["last_source_sequence"].as_u64()?;
    if acknowledged < previous || acknowledged > previous.max(last) {
        return None;
    }
    let mut counts = [0_u64; 5];
    let mut seen = BTreeSet::new();
    for (input, row) in requested.iter().zip(rows) {
        fields(
            row,
            &[
                "source_sequence",
                "receipt_digest",
                "state",
                "reason",
                "record_id",
                "state_generation",
            ],
        )?;
        let sequence = row["source_sequence"].as_u64()?;
        let receipt = row["receipt_digest"].as_str()?;
        if !seen.insert(sequence)
            || input["source_sequence"] != sequence
            || input["receipt_digest"] != receipt
            || !crate::NcmProviderAdapter::valid_sha256(receipt)
        {
            return None;
        }
        let index = match row["state"].as_str()? {
            "applied" => 0,
            "delivery_duplicate" => 1,
            "source_already_applied" => 2,
            "rejected" => 3,
            "effect_unknown" => 4,
            _ => return None,
        };
        counts[index] += 1;
        let generation = row["state_generation"].as_u64()?;
        if generation > stored_after || (index == 0 && generation <= stored_before) {
            return None;
        }
        if !row["record_id"].is_null() && row["record_id"].as_u64()? == 0 {
            return None;
        }
        if let Some(reason) = row["reason"].as_str() {
            if reason.is_empty()
                || reason.len() > 64
                || !reason
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            {
                return None;
            }
        } else if !row["reason"].is_null() {
            return None;
        }
        if index == 1 && input["delivery_key"].as_str().is_none_or(str::is_empty) {
            return None;
        }
        if index == 4 && row["reason"].is_null() {
            return None;
        }
    }
    for (field, count) in [
        "applied_observations",
        "duplicate_observations",
        "sources_already_applied",
        "rejected_observations",
        "effect_unknown_observations",
    ]
    .iter()
    .zip(counts)
    {
        if response[*field].as_u64()? != count {
            return None;
        }
    }
    match effect {
        crate::CommittedEffectState::Partial => {
            if !partial
                || counts[4] > 0
                || acknowledged <= previous
                || stored_after <= stored_before
                || (acknowledged < last && counts[3] == 0)
                || terminal != TerminalCode::PartialEffect
            {
                return None;
            }
        }
        crate::CommittedEffectState::Unknown => {
            if !partial
                || !matches!(
                    terminal,
                    TerminalCode::EffectUnknown
                        | TerminalCode::Cancelled
                        | TerminalCode::DeadlineExceeded
                )
                || (counts[4] == 0 && stored_after == stored_before)
            {
                return None;
            }
        }
        crate::CommittedEffectState::Committed | crate::CommittedEffectState::Duplicate => {
            if partial || counts[4] != 0 || terminal != TerminalCode::Success {
                return None;
            }
        }
        _ => return None,
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (Value, Value) {
        let receipt = "a".repeat(64);
        (
            json!({"action":"replay", "first_source_sequence":1, "last_source_sequence":2, "expected_previous_acknowledged_sequence":0,
            "items":[{"source_sequence":1,"receipt_digest":receipt,"delivery_key":"key1"},{"source_sequence":2,"receipt_digest":receipt,"delivery_key":"key2"}]}),
            json!({"common_portability":"replay", "first_source_sequence":1, "last_source_sequence":2, "acknowledged_sequence":1,
            "state_generation_before":0,"state_generation_after":1,"applied_observations":1,"duplicate_observations":0,"sources_already_applied":0,
            "rejected_observations":0,"effect_unknown_observations":1,"partial":true,"replayed":false,"warnings":[],
            "items":[{"source_sequence":1,"receipt_digest":receipt,"state":"applied","reason":null,"record_id":1,"state_generation":1},
            {"source_sequence":2,"receipt_digest":receipt,"state":"effect_unknown","reason":"commit_unknown","record_id":null,"state_generation":1}]}),
        )
    }

    #[test]
    fn interrupted_replay_preserves_exact_known_and_unknown_partition() {
        let (request, response) = fixture();
        assert!(
            validate(
                &request,
                &response,
                0,
                1,
                TerminalCode::EffectUnknown,
                crate::CommittedEffectState::Unknown
            )
            .is_some()
        );
    }

    #[test]
    fn malformed_or_foreign_error_accounting_is_rejected() {
        let (request, response) = fixture();
        for changed in ["counter", "foreign", "hidden_unknown", "item_source"] {
            let mut response = response.clone();
            match changed {
                "counter" => response["applied_observations"] = json!(2),
                "foreign" => response["common_portability"] = json!("inspection"),
                "hidden_unknown" => {
                    response["effect_unknown_observations"] = json!(0);
                    response["rejected_observations"] = json!(1);
                }
                _ => response["items"][1]["source_sequence"] = json!(9),
            }
            assert!(
                validate(
                    &request,
                    &response,
                    0,
                    1,
                    TerminalCode::EffectUnknown,
                    crate::CommittedEffectState::Unknown
                )
                .is_none()
            );
        }
    }
}
