use super::{read_guest_string, wrap, HostState};
use std::collections::HashSet;
use wasmtime::{Caller, Linker};

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "cufflink_log",
        |mut caller: Caller<'_, HostState>, level: i32, msg_ptr: i32, msg_len: i32| {
            if let Ok(msg) = read_guest_string(&mut caller, "cufflink_log", msg_ptr, msg_len) {
                match level {
                    0 => tracing::error!("[wasm] {}", msg),
                    1 => tracing::warn!("[wasm] {}", msg),
                    2 => tracing::info!("[wasm] {}", msg),
                    _ => tracing::debug!("[wasm] {}", msg),
                }
            }
        },
    )?;

    Ok(())
}
