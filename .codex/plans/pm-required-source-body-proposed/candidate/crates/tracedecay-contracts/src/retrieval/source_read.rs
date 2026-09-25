use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracedecay_domain::UtcMicros;

use crate::context::RequestContext;
use crate::error::ApplicationContractError;
use crate::handlers::ApplicationOperation;
use crate::result::OperationBudgetUsage;

use super::RetrievalRequestMeta;

pub const MAX_SOURCE_READ_PATH_BYTES: usize = 4_096;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceReadModeV1 {
    Full,
    Lines,
    Map,
    Signatures,
}

impl SourceReadModeV1 {
    #[hotpath::skip]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Lines => "lines",
            Self::Map => "map",
            Self::Signatures => "signatures",
        }
    }
}

/// Whether a source read may return an unchanged cache receipt without a body.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceReadBodyPolicyV1 {
    /// Preserve the ordinary metadata-only cache response for unchanged files.
    #[default]
    IfChanged,
    /// Reread and return the complete decoded full/lines body for this operation.
    Required,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceReadPrimitiveRequest {
    pub file: String,
    pub mode: SourceReadModeV1,
    #[serde(default)]
    pub body_policy: SourceReadBodyPolicyV1,
    pub lines: Option<String>,
    pub include_symbols: bool,
    pub meta: RetrievalRequestMeta,
}

impl SourceReadPrimitiveRequest {
    pub fn validate(&self) -> Result<(), ApplicationContractError> {
        let file_is_valid = !self.file.is_empty()
            && self.file.len() <= MAX_SOURCE_READ_PATH_BYTES
            && !self.file.contains('\0');
        let range_shape_is_valid = match self.mode {
            SourceReadModeV1::Lines => self.lines.is_some(),
            SourceReadModeV1::Full | SourceReadModeV1::Map | SourceReadModeV1::Signatures => {
                self.lines.is_none()
            }
        };
        let body_policy_is_valid = self.body_policy == SourceReadBodyPolicyV1::IfChanged
            || matches!(self.mode, SourceReadModeV1::Full | SourceReadModeV1::Lines);
        if file_is_valid
            && range_shape_is_valid
            && body_policy_is_valid
            && self.meta.page.cursor.is_none()
        {
            Ok(())
        } else {
            Err(ApplicationContractError::Inconsistent {
                field: "source read request",
            })
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceReadResultV1 {
    pub file: String,
    pub mode: SourceReadModeV1,
    pub mtime_ns: u64,
    pub digest: String,
    pub token_count: usize,
    pub unchanged: bool,
    pub body: Option<String>,
    pub context: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SourceReadPortOutcome {
    Completed {
        result: SourceReadResultV1,
        finished_at: UtcMicros,
        budget: OperationBudgetUsage,
    },
    Partial {
        result: SourceReadResultV1,
        finished_at: UtcMicros,
        budget: OperationBudgetUsage,
    },
    Failed {
        finished_at: UtcMicros,
        budget: OperationBudgetUsage,
    },
}

pub type SourceReadPortFuture<'a> =
    Pin<Box<dyn Future<Output = SourceReadPortOutcome> + Send + 'a>>;

#[derive(Clone, Copy, Debug)]
pub struct SourceReadPortContext<'a> {
    pub request: &'a RequestContext,
    pub operation: &'a ApplicationOperation,
    pub observed_at: UtcMicros,
}

/// Async application port for compatibility-preserving source reads.
///
/// Implementations must delegate range parsing, rendering, and cache handling
/// to the existing source-read kernel.
pub trait SourceReadPrimitivePort {
    fn source_read<'a>(
        &'a self,
        context: SourceReadPortContext<'a>,
        request: &'a SourceReadPrimitiveRequest,
    ) -> SourceReadPortFuture<'a>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PageRequest, ResultProjection};
    use crate::retrieval::RetrievalOrder;
    use serde_json::json;

    fn source_request(mode: SourceReadModeV1) -> SourceReadPrimitiveRequest {
        SourceReadPrimitiveRequest {
            file: "src/lib.rs".to_owned(),
            mode,
            body_policy: SourceReadBodyPolicyV1::IfChanged,
            lines: (mode == SourceReadModeV1::Lines).then(|| "1-2".to_owned()),
            include_symbols: false,
            meta: RetrievalRequestMeta::current(
                PageRequest::first(1).expect("page"),
                ResultProjection::Evidence,
                RetrievalOrder::SourcePosition,
            ),
        }
    }

    #[test]
    fn source_read_body_policy_defaults_and_rejects_unknown_values() {
        let mut value = serde_json::to_value(source_request(SourceReadModeV1::Full)).unwrap();
        value.as_object_mut().unwrap().remove("body_policy");
        let request: SourceReadPrimitiveRequest = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(request.body_policy, SourceReadBodyPolicyV1::IfChanged);
        for (wire, expected) in [
            ("if_changed", SourceReadBodyPolicyV1::IfChanged),
            ("required", SourceReadBodyPolicyV1::Required),
        ] {
            value["body_policy"] = json!(wire);
            let request: SourceReadPrimitiveRequest = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(request.body_policy, expected);
            assert_eq!(serde_json::to_value(request).unwrap()["body_policy"], wire);
        }
        for invalid in [json!("always"), json!(""), json!(null), json!(true), json!(1), json!([])] {
            value["body_policy"] = invalid;
            assert!(serde_json::from_value::<SourceReadPrimitiveRequest>(value.clone()).is_err());
        }
    }

    #[test]
    fn source_read_required_body_accepts_only_source_modes() {
        for mode in [SourceReadModeV1::Full, SourceReadModeV1::Lines, SourceReadModeV1::Map, SourceReadModeV1::Signatures] {
            let mut request = source_request(mode);
            assert!(request.validate().is_ok(), "default policy must retain {mode:?}");
            request.body_policy = SourceReadBodyPolicyV1::Required;
            assert_eq!(request.validate().is_ok(), matches!(mode, SourceReadModeV1::Full | SourceReadModeV1::Lines));
        }
    }
}
