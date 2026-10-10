//! Structured logging from inside your WASM handler.
//!
//! Messages appear in the platform's log output prefixed with `[wasm]`.

#[cfg(target_arch = "wasm32")]
use crate::ffi;

/// Log an error message (level 0).
pub fn error(msg: &str) {
    write(0, msg);
}

/// Log a warning message (level 1).
pub fn warn(msg: &str) {
    write(1, msg);
}

/// Log an info message (level 2).
pub fn info(msg: &str) {
    write(2, msg);
}

/// Log a debug message (level 3).
pub fn debug(msg: &str) {
    write(3, msg);
}

fn write(level: i32, msg: &str) {
    #[cfg(target_arch = "wasm32")]
    {
        let bytes = msg.as_bytes();
        unsafe {
            ffi::cufflink_log(level, bytes.as_ptr() as i32, bytes.len() as i32);
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (level, msg);
    }
}
