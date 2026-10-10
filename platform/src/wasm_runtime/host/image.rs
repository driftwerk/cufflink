use super::{block_on_bounded, read_guest_string, wrap, HostState};
use serde_json::{json, Value};
use std::collections::HashSet;
use wasmtime::{Caller, Linker};

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    register_image_transform(
        linker,
        names,
        "image_transform_jpeg",
        crate::wasm_runtime::image::Format::Jpeg,
    )?;
    register_image_transform(
        linker,
        names,
        "image_transform_webp",
        crate::wasm_runtime::image::Format::Webp,
    )?;
    register_image_transform(
        linker,
        names,
        "image_transform_avif",
        crate::wasm_runtime::image::Format::Avif,
    )?;

    wrap(
        linker,
        names,
        "image_transform_multi",
        |mut caller: Caller<'_, HostState>,
         bucket_ptr: i32,
         bucket_len: i32,
         in_key_ptr: i32,
         in_key_len: i32,
         targets_ptr: i32,
         targets_len: i32|
         -> i32 {
            const FN: &str = "image_transform_multi";
            let bucket = host_try!(read_guest_string(&mut caller, FN, bucket_ptr, bucket_len));
            let in_key = host_try!(read_guest_string(&mut caller, FN, in_key_ptr, in_key_len));
            let targets_json =
                host_try!(read_guest_string(&mut caller, FN, targets_ptr, targets_len));
            let Ok(targets) = serde_json::from_str::<Value>(&targets_json) else {
                return -1;
            };

            let parsed: Option<Vec<(String, u32, u8, crate::wasm_runtime::image::Format)>> =
                targets.as_array().map(|items| {
                    items
                        .iter()
                        .filter_map(|t| {
                            let out_key = t.get("out_key")?.as_str()?.to_string();
                            let max_dim = u32::try_from(t.get("max_dim")?.as_u64()?).ok()?;
                            let quality = u8::try_from(t.get("quality")?.as_u64()?).ok()?;
                            if !(1..=100).contains(&quality) {
                                return None;
                            }
                            let format = match t.get("format").and_then(|v| v.as_str()) {
                                Some(name) => crate::wasm_runtime::image::Format::from_name(name)?,
                                None => crate::wasm_runtime::image::Format::Jpeg,
                            };
                            Some((out_key, max_dim, quality, format))
                        })
                        .collect()
                });
            let parsed = match parsed {
                Some(p)
                    if !p.is_empty() && p.len() == targets.as_array().map_or(0, |a| a.len()) =>
                {
                    p
                }
                _ => return -1,
            };
            let keys: Vec<&str> = std::iter::once(in_key.as_str())
                .chain(parsed.iter().map(|(out_key, ..)| out_key.as_str()))
                .collect();
            if !caller.data().authorize_storage(&bucket, &keys) {
                return -1;
            }

            let app_state = caller.data().app_state.clone();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let storage = app_state
                    .storage
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("storage not configured"))?;
                crate::wasm_runtime::image::transform_multi(storage, &bucket, &in_key, parsed).await
            });

            match result {
                Ok(sizes) => {
                    let count = sizes.len() as i32;
                    caller.data_mut().host_response = Some(json!(sizes).to_string());
                    count
                }
                Err(e) => {
                    tracing::error!("[wasm] image_transform_multi error: {}", e);
                    -1
                }
            }
        },
    )?;

    Ok(())
}

fn register_image_transform(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
    name: &'static str,
    format: crate::wasm_runtime::image::Format,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        name,
        move |mut caller: Caller<'_, HostState>,
              bucket_ptr: i32,
              bucket_len: i32,
              in_key_ptr: i32,
              in_key_len: i32,
              out_key_ptr: i32,
              out_key_len: i32,
              max_dim: i32,
              quality: i32|
              -> i32 {
            let bucket = host_try!(read_guest_string(&mut caller, name, bucket_ptr, bucket_len));
            let in_key = host_try!(read_guest_string(&mut caller, name, in_key_ptr, in_key_len));
            let out_key = host_try!(read_guest_string(
                &mut caller,
                name,
                out_key_ptr,
                out_key_len
            ));

            if max_dim < 0 || !(1..=100).contains(&quality) {
                tracing::error!(
                    "[wasm] {}: invalid args (max_dim={}, quality={})",
                    name,
                    max_dim,
                    quality
                );
                return -1;
            }
            if !caller
                .data()
                .authorize_storage(&bucket, &[&in_key, &out_key])
            {
                return -1;
            }

            let app_state = caller.data().app_state.clone();

            let result = block_on_bounded(&caller.data().bounds(), async {
                let storage = app_state
                    .storage
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("storage not configured"))?;
                crate::wasm_runtime::image::transform(
                    storage,
                    &bucket,
                    &in_key,
                    &out_key,
                    max_dim as u32,
                    quality as u8,
                    format,
                )
                .await
            });

            match result {
                Ok(bytes) => bytes as i32,
                Err(e) => {
                    tracing::error!("[wasm] {} error: {}", name, e);
                    -1
                }
            }
        },
    )?;
    Ok(())
}
