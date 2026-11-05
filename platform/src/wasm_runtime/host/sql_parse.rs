use super::{block_on_within, read_guest_bytes, wrap, HostIoError, HostState};
use crate::sql_parser::ParseFailure;
use crate::wasm_runtime::budget::{Hold, HostCaps, Refused};
use cufflink_types::sql_parse::{ParseError, ParseResponse};
use std::collections::HashSet;
use std::sync::{Arc, LazyLock};
use wasmtime::{Caller, Linker};

const HOST_FN: &str = "sql_parse";

static AROUND: LazyLock<(String, &'static str)> = LazyLock::new(cufflink_sql_parse::answer_around);

/// The longest tree text an answer under the response cap holds.
fn max_tree(caps: &HostCaps) -> usize {
    let (before, after) = &*AROUND;
    caps.response.saturating_sub(before.len() + after.len())
}

/// The answer's buffer, charged for in full, before a tree of `len`
/// bytes is read into it.
fn charged(caps: &HostCaps, hold: &mut Hold, len: usize) -> Result<Vec<u8>, Refused> {
    let (before, after) = &*AROUND;
    let total = before.len() + len + after.len();
    caps.response(HOST_FN, total).map_err(Refused::Cap)?;
    hold.grow(total).map_err(Refused::Budget)?;
    let mut answer = Vec::with_capacity(total);
    answer.extend_from_slice(before.as_bytes());
    Ok(answer)
}

fn parse(caller: &mut Caller<'_, HostState>, ptr: i32, len: i32) -> i32 {
    caller.data_mut().clear_host_response();
    let (bytes, _input) = host_try!(read_guest_bytes(caller, HOST_FN, ptr, len)).into_parts();
    let Ok(sql) = std::str::from_utf8(&bytes) else {
        return caller
            .data_mut()
            .respond_json(HOST_FN, &ParseResponse::Error(ParseError::syntax()));
    };
    let state = caller.data();
    let parser = Arc::clone(&state.app_state.sql_parser);
    let tenant = state.identity.tenant_id;
    let caps = state.caps().clone();
    let bounds = state.bounds();
    let mut hold = state.hold();
    let parsed = block_on_within(
        bounds.remaining,
        &bounds.cancel,
        parser.parse(tenant, sql, max_tree(&caps), bounds.remaining, |len| {
            charged(&caps, &mut hold, len)
        }),
    );
    let state = caller.data_mut();
    match parsed.map_err(|ended| match ended {
        HostIoError::Failed(failure) => failure,
        HostIoError::TimedOut(_) | HostIoError::Cancelled => {
            ParseFailure::Refused(ParseError::unavailable())
        }
    }) {
        Ok(mut answer) => {
            answer.extend_from_slice(AROUND.1.as_bytes());
            match String::from_utf8(answer) {
                Ok(text) => {
                    state.set_charged_response(text, hold);
                    0
                }
                Err(_) => {
                    tracing::warn!(
                        "[wasm] {HOST_FN}: the parser process answered a tree that is not UTF-8"
                    );
                    state.respond_json(HOST_FN, &ParseResponse::Error(ParseError::unavailable()))
                }
            }
        }
        Err(ParseFailure::Refused(error)) => {
            drop(hold);
            state.respond_json(HOST_FN, &ParseResponse::Error(error))
        }
        Err(ParseFailure::Unheld(refused)) => state.refused(refused),
    }
}

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        HOST_FN,
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            parse(&mut caller, ptr, len)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::PolicyMode;

    #[test]
    fn a_tree_fits_the_response_cap_with_its_answer_around_it() {
        let caps = HostCaps {
            mode: PolicyMode::Enforce,
            ..HostCaps::default()
        };
        let (before, after) = &*AROUND;
        assert_eq!(max_tree(&caps) + before.len() + after.len(), caps.response);
        let mut hold = Hold::on(None);
        let answer = charged(&caps, &mut hold, max_tree(&caps)).expect("fits");
        assert_eq!(answer, before.as_bytes());
        assert_eq!(answer.capacity(), caps.response);
        assert!(matches!(
            charged(&caps, &mut hold, max_tree(&caps) + 1),
            Err(Refused::Cap(_))
        ));
    }
}
