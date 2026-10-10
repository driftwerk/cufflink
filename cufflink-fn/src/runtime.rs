//! The guest side of the handler ABI: `alloc(len) -> ptr` for the host to
//! write a request into, and `handler(ptr, len) -> ptr` returning the response
//! as `[u32 LE length][bytes]`.

use crate::{Request, Response};
use std::mem::ManuallyDrop;

/// Internal function used by the `handler!` macro. Do not call directly.
#[doc(hidden)]
pub fn __run_handler<F>(ptr: i32, len: i32, f: F) -> i32
where
    F: FnOnce(Request) -> Response,
{
    let request_json = String::from_utf8_lossy(request_bytes(ptr, len));
    let request = Request::from_json(&request_json).unwrap_or_else(Request::unparsed);
    let framed = frame(f(request).into_data().as_bytes());
    Box::leak(framed.into_boxed_slice()).as_ptr() as i32
}

/// Backs the `alloc` export of [`init!`](crate::init). Never returns 0: that
/// is valid linear memory, and the host would write the request through it.
/// A negative size is a host bug and traps.
#[doc(hidden)]
pub fn __alloc(size: i32) -> i32 {
    crate::features::anchor();
    match usize::try_from(size) {
        Ok(size) => leak_buffer(size) as i32,
        Err(_) => std::process::abort(),
    }
}

fn leak_buffer(size: usize) -> *mut u8 {
    ManuallyDrop::new(Vec::<u8>::with_capacity(size.max(1))).as_mut_ptr()
}

/// The request the host wrote at `ptr`; empty when it passed none.
fn request_bytes<'a>(ptr: i32, len: i32) -> &'a [u8] {
    match usize::try_from(len) {
        // SAFETY: the host wrote `len` bytes at `ptr`, inside the buffer it
        Ok(len) if len > 0 && ptr != 0 => unsafe {
            std::slice::from_raw_parts(ptr as *const u8, len)
        },
        _ => &[],
    }
}

/// `body` prefixed with its length as a little-endian `u32`.
fn frame(body: &[u8]) -> Vec<u8> {
    [&(body.len() as u32).to_le_bytes()[..], body].concat()
}

/// Initialize the cufflink-fn runtime. Call this once at the top of your `lib.rs`.
///
/// Exports the `alloc` function that the platform needs to pass data into
/// your WASM module.
///
/// ```rust,ignore
/// use cufflink_fn::prelude::*;
///
/// cufflink_fn::init!();
/// ```
#[macro_export]
macro_rules! init {
    () => {
        #[no_mangle]
        pub extern "C" fn alloc(size: i32) -> i32 {
            $crate::__alloc(size)
        }
    };
}

/// Define a handler function.
///
/// This macro generates the `#[no_mangle] extern "C"` boilerplate so your
/// handler is a plain Rust closure that receives a [`Request`] and returns
/// a [`Response`].
///
/// ```rust,ignore
/// use cufflink_fn::prelude::*;
///
/// cufflink_fn::init!();
///
/// handler!(get_stats, |req: Request| {
///     let rows = db::query("SELECT COUNT(*) as total FROM orders");
///     Response::json(&json!({"total": rows[0]["total"]}))
/// });
///
/// handler!(create_order, |req: Request| {
///     let body = req.body();
///     let customer = body["customer_id"].as_str().unwrap_or("unknown");
///     db::execute(&format!(
///         "INSERT INTO orders (customer_id, status) VALUES ('{}', 'pending')",
///         customer
///     ));
///     Response::json(&json!({"status": "created"}))
/// });
/// ```
///
/// A handler that is a function of its own is exported by its path, and
/// stays callable from tests:
///
/// ```rust,ignore
/// pub fn get_stats(req: Request) -> Response {
///     Response::json(&json!({"tenant": req.tenant()}))
/// }
///
/// handler!(stats, get_stats);
/// ```
#[macro_export]
macro_rules! handler {
    ($name:ident, |$req:ident : Request| $body:expr) => {
        #[no_mangle]
        pub extern "C" fn $name(ptr: i32, len: i32) -> i32 {
            $crate::__run_handler(ptr, len, |$req: $crate::Request| $body)
        }
    };
    ($name:ident, $handler:path) => {
        #[no_mangle]
        pub extern "C" fn $name(ptr: i32, len: i32) -> i32 {
            $crate::__run_handler(ptr, len, $handler)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_prefixes_the_little_endian_length() {
        assert_eq!(frame(b"ok"), vec![2, 0, 0, 0, b'o', b'k']);
        assert_eq!(frame(b""), vec![0, 0, 0, 0]);
    }

    #[test]
    fn request_bytes_are_empty_without_a_request() {
        let body = b"{}";
        let ptr = body.as_ptr() as i32;
        assert!(request_bytes(0, 2).is_empty());
        assert!(request_bytes(ptr, 0).is_empty());
        assert!(request_bytes(ptr, -1).is_empty());
    }

    fn named(req: Request) -> Response {
        Response::text(req.method())
    }

    handler!(exported_by_path, named);

    #[test]
    fn a_function_is_exported_by_its_path() {
        let exported: extern "C" fn(i32, i32) -> i32 = exported_by_path;
        assert_ne!(exported as usize, 0);
        assert_eq!(
            named(Request::unparsed()).into_data(),
            Response::text("GET").into_data()
        );
    }

    #[test]
    fn buffers_are_never_null_even_when_empty() {
        assert!(!leak_buffer(0).is_null());
        assert!(!leak_buffer(4096).is_null());
    }
}
