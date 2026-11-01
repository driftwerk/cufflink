use super::ledger::{Charge, Ledger, ThreadStart};
use std::cell::RefCell;
use std::sync::Arc;

struct ThreadCharge(Arc<Ledger>);

impl Drop for ThreadCharge {
    fn drop(&mut self) {
        self.0.thread_retired();
    }
}

thread_local! {
    static THREAD: RefCell<Option<ThreadCharge>> = const { RefCell::new(None) };
}

pub(crate) struct OnThread(Arc<Ledger>);

impl Drop for OnThread {
    fn drop(&mut self) {
        self.0.thread_idle();
    }
}

pub(crate) fn enter(charge: &mut Charge) -> OnThread {
    let ledger = Arc::clone(charge.ledger());
    let charged = THREAD.with(|thread| {
        thread
            .borrow()
            .as_ref()
            .is_some_and(|held| Arc::ptr_eq(&held.0, &ledger))
    });
    match ledger.thread_started(charge.take_stack(), charged) {
        ThreadStart::Charged => {}
        ThreadStart::ReleaseReserved => charge.release_stack(),
        ThreadStart::MoveReserved => {
            ledger.move_to_residue(charge.class(), charge.service());
            charge.stack_moved();
        }
    }
    if !charged {
        let previous =
            THREAD.with(|thread| thread.replace(Some(ThreadCharge(Arc::clone(&ledger)))));
        drop(previous);
    }
    OnThread(ledger)
}
