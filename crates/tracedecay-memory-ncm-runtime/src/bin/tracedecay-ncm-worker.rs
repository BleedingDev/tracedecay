//! Supervised stdio worker for the native NCM runtime.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tracedecay_memory_ncm_core::types::NcmConfig;
use tracedecay_memory_ncm_runtime::embedding::doubles::HashEncoder;
#[cfg(feature = "real-encoder")]
use tracedecay_memory_ncm_runtime::embedding::{MiniLmEncoder, PinnedEncoder};
use tracedecay_memory_ncm_runtime::engine::NcmEngine;
#[cfg(feature = "real-encoder")]
use tracedecay_memory_ncm_runtime::ports::{Deadline, Embedding, EncoderError, EncoderIdentity};
use tracedecay_memory_ncm_runtime::ports::{StateRoot, TextEncoder};
use tracedecay_memory_ncm_runtime::worker::{ServeOptions, serve_with_options};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(()) => ExitCode::from(2),
    }
}

fn run() -> Result<(), ()> {
    let arguments = parse_arguments(std::env::args_os().skip(1))?;
    let root = StateRoot::new(arguments.state_root).map_err(|_| ())?;
    let (encoder, encoder_ready): (Arc<dyn TextEncoder>, bool) =
        select_encoder(&root, arguments.test_double, arguments.no_encoder_required)?;
    let config = if arguments.test_double {
        test_config()
    } else {
        NcmConfig::default()
    };
    let engine = Arc::new(NcmEngine::new(root, encoder, config));
    serve_with_options(
        std::io::stdin().lock(),
        std::io::stdout().lock(),
        engine,
        ServeOptions {
            allow_test_delays: arguments.test_double,
            encoder_ready,
        },
    )
    .map_err(|_| ())
}

struct Arguments {
    state_root: PathBuf,
    test_double: bool,
    no_encoder_required: bool,
}

fn parse_arguments(
    mut arguments: impl Iterator<Item = std::ffi::OsString>,
) -> Result<Arguments, ()> {
    let mut state_root = None;
    let mut test_double = false;
    let mut no_encoder_required = false;
    while let Some(argument) = arguments.next() {
        if argument == "--state-root" {
            if state_root.is_some() {
                return Err(());
            }
            state_root = arguments.next().map(PathBuf::from);
        } else if argument == "--test-double" {
            test_double = true;
        } else if argument == "--no-encoder-required" {
            no_encoder_required = true;
        } else {
            return Err(());
        }
    }
    let state_root = state_root.ok_or(())?;
    if !state_root.is_absolute() {
        return Err(());
    }
    Ok(Arguments {
        state_root,
        test_double,
        no_encoder_required,
    })
}

fn select_encoder(
    root: &StateRoot,
    test_double: bool,
    no_encoder_required: bool,
) -> Result<(Arc<dyn TextEncoder>, bool), ()> {
    if test_double {
        return Ok((Arc::new(HashEncoder::new()), true));
    }

    #[cfg(feature = "real-encoder")]
    {
        let expected = PinnedEncoder::reference().map_err(|_| ())?;
        match MiniLmEncoder::open(root, &expected) {
            Ok(encoder) => Ok((Arc::new(encoder), true)),
            Err(error) => {
                let _ = no_encoder_required;
                Ok((Arc::new(UnavailableEncoder::from_error(error)), false))
            }
        }
    }

    #[cfg(not(feature = "real-encoder"))]
    {
        let _ = root;
        let _ = no_encoder_required;
        Err(())
    }
}

fn test_config() -> NcmConfig {
    let mut config = NcmConfig {
        terrain_resolution: 3,
        ..NcmConfig::default()
    };
    config.stm.n_centers = 8;
    config.stm.top_k_read = 8;
    config.stm.top_k_write = 8;
    config.ltm.n_centers = 8;
    config.ltm.top_k_read = 8;
    config.ltm.top_k_write = 8;
    config.hybrid_candidates = 8;
    config
}

#[cfg(feature = "real-encoder")]
struct UnavailableEncoder {
    detail: String,
}

#[cfg(feature = "real-encoder")]
impl UnavailableEncoder {
    fn from_error(error: EncoderError) -> Self {
        Self {
            detail: error.to_string(),
        }
    }
}

#[cfg(feature = "real-encoder")]
impl TextEncoder for UnavailableEncoder {
    fn identity(&self) -> EncoderIdentity {
        EncoderIdentity {
            model: "not-ready".to_owned(),
            artifact_sha256: "not-ready".to_owned(),
            max_length: 128,
        }
    }

    fn encode(&self, _texts: &[&str], _deadline: Deadline) -> Result<Vec<Embedding>, EncoderError> {
        Err(EncoderError::ArtifactsMissing(self.detail.clone()))
    }
}
