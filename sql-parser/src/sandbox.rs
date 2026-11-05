//! What confines a parser process on Linux: its address space, its place
//! in the out-of-memory killer's order, no inspection by other processes,
//! and, once it has greeted, a system call allowlist with no files,
//! sockets or programs in it. Elsewhere only the memory setting is read.

use std::io;

#[cfg(target_os = "linux")]
pub fn confine() -> io::Result<()> {
    if let Some(mb) = memory_limit_mb()? {
        let bytes: libc::rlim_t = mb.saturating_mul(1024 * 1024);
        let limit = libc::rlimit {
            rlim_cur: bytes,
            rlim_max: bytes,
        };
        // SAFETY: setrlimit reads the struct it is given and nothing else.
        if unsafe { libc::setrlimit(libc::RLIMIT_AS, &limit) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    // An undumpable process's /proc entries belong to root, so the score
    // is raised while they are still its own.
    std::fs::write("/proc/self/oom_score_adj", "1000")
        .map_err(|e| io::Error::new(e.kind(), format!("cannot raise oom_score_adj: {e}")))?;
    // SAFETY: PR_SET_DUMPABLE takes plain integers and touches no memory.
    if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn confine() -> io::Result<()> {
    memory_limit_mb().map(drop)
}

/// Every thread of the process may from now on make only the system calls
/// a parse and its answer need; any other ends the process.
#[cfg(target_os = "linux")]
pub fn enter() -> io::Result<()> {
    use seccompiler::{BpfProgram, SeccompAction, SeccompFilter, TargetArch};
    let arch = TargetArch::try_from(std::env::consts::ARCH).map_err(io::Error::other)?;
    let rules = ALLOWED
        .iter()
        .chain(ALLOWED_ON_ARCH)
        .map(|&call| (call, Vec::new()))
        .collect();
    let filter = SeccompFilter::new(
        rules,
        SeccompAction::KillProcess,
        SeccompAction::Allow,
        arch,
    )
    .map_err(io::Error::other)?;
    let program = BpfProgram::try_from(filter).map_err(io::Error::other)?;
    seccompiler::apply_filter_all_threads(&program).map_err(io::Error::other)
}

#[cfg(not(target_os = "linux"))]
pub fn enter() -> io::Result<()> {
    Ok(())
}

/// The stdin and stdout of the wire, the parser's capture of its own
/// stderr through a pipe, the allocator, the parse thread's hand-offs, and
/// signal returns and exit.
#[cfg(target_os = "linux")]
const ALLOWED: &[libc::c_long] = &[
    libc::SYS_read,
    libc::SYS_write,
    libc::SYS_close,
    libc::SYS_dup,
    libc::SYS_dup3,
    libc::SYS_pipe2,
    libc::SYS_fcntl,
    libc::SYS_fstat,
    libc::SYS_newfstatat,
    libc::SYS_brk,
    libc::SYS_mmap,
    libc::SYS_munmap,
    libc::SYS_mremap,
    libc::SYS_mprotect,
    libc::SYS_madvise,
    libc::SYS_futex,
    libc::SYS_sched_yield,
    libc::SYS_clock_gettime,
    libc::SYS_getrandom,
    libc::SYS_getpid,
    libc::SYS_gettid,
    libc::SYS_rt_sigreturn,
    libc::SYS_rt_sigprocmask,
    libc::SYS_sigaltstack,
    libc::SYS_restart_syscall,
    libc::SYS_exit,
    libc::SYS_exit_group,
];

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const ALLOWED_ON_ARCH: &[libc::c_long] = &[libc::SYS_pipe, libc::SYS_dup2];

#[cfg(all(target_os = "linux", not(target_arch = "x86_64")))]
const ALLOWED_ON_ARCH: &[libc::c_long] = &[];

/// Hands the memory a parse freed back to the system, so a large parse
/// does not leave the process holding it.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn release_memory() {
    // SAFETY: malloc_trim only walks the allocator's own free lists.
    unsafe { libc::malloc_trim(0) };
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn release_memory() {}

fn memory_limit_mb() -> io::Result<Option<u64>> {
    match std::env::var("SQL_PARSE_MEMORY_MB") {
        Ok(mb) => mb
            .parse::<u64>()
            .ok()
            .filter(|mb| *mb > 0)
            .map(Some)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SQL_PARSE_MEMORY_MB is not a positive number",
                )
            }),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
    }
}
