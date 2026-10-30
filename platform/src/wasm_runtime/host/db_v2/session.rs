use crate::tenant_sessions::Session;
use crate::wasm_runtime::ExecutionError;
use cufflink_pgcodec::tx::{transaction_limit, transaction_not_found, Phase};
use cufflink_types::db::{DbError, DbErrorKind};
use std::collections::BTreeMap;
use std::time::Instant;
use tokio_postgres::Statement;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbandonReason {
    Trap,
    Deadline,
    Shutdown,
    Leaked,
}

impl AbandonReason {
    fn label(self) -> &'static str {
        match self {
            AbandonReason::Trap => "trap",
            AbandonReason::Deadline => "deadline",
            AbandonReason::Shutdown => "shutdown",
            AbandonReason::Leaked => "leaked",
        }
    }

    pub(crate) fn of<T>(result: &Result<T, ExecutionError>) -> Self {
        match result {
            Ok(_) => AbandonReason::Leaked,
            Err(ExecutionError::DeadlineExceeded { .. }) => AbandonReason::Deadline,
            Err(ExecutionError::ShuttingDown) => AbandonReason::Shutdown,
            Err(_) => AbandonReason::Trap,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxEnd {
    Commit,
    Rollback,
    Abandoned,
}

impl TxEnd {
    fn label(self) -> &'static str {
        match self {
            TxEnd::Commit => "commit",
            TxEnd::Rollback => "rollback",
            TxEnd::Abandoned => "abandoned",
        }
    }
}

pub(crate) struct TxSlot {
    client: Option<Session>,
    pub(crate) set_timeout: Statement,
    pub(crate) phase: Phase,
    pub(crate) savepoints: Vec<String>,
    opened: Instant,
}

impl TxSlot {
    fn new(client: Session, set_timeout: Statement) -> Self {
        metrics::gauge!("cufflink_db_tx_open").increment(1.0);
        Self {
            client: Some(client),
            set_timeout,
            phase: Phase::Open,
            savepoints: Vec::new(),
            opened: Instant::now(),
        }
    }

    pub(crate) fn client(&self) -> Result<&Session, DbError> {
        self.client.as_ref().ok_or_else(|| {
            DbError::new(
                DbErrorKind::TransactionNotFound,
                "the transaction's connection is gone",
            )
        })
    }

    pub(crate) fn finish(mut self, end: TxEnd, reusable: bool) {
        if let Some(client) = self.client.take() {
            if !reusable {
                client.discard();
            }
            self.record(end);
        }
    }

    pub(crate) fn abandon(mut self, reason: AbandonReason) {
        self.discard(reason);
    }

    fn discard(&mut self, reason: AbandonReason) {
        if let Some(client) = self.client.take() {
            metrics::counter!("cufflink_db_tx_abandoned_total", "reason" => reason.label())
                .increment(1);
            self.record(TxEnd::Abandoned);
            client.discard();
        }
    }

    fn record(&self, end: TxEnd) {
        metrics::gauge!("cufflink_db_tx_open").decrement(1.0);
        metrics::histogram!("cufflink_db_tx_duration_seconds", "outcome" => end.label())
            .record(self.opened.elapsed().as_secs_f64());
    }
}

impl Drop for TxSlot {
    fn drop(&mut self) {
        self.discard(AbandonReason::Leaked);
    }
}

pub struct DbSession {
    slots: BTreeMap<u32, TxSlot>,
    next_id: u32,
    max_open: usize,
}

impl DbSession {
    pub fn new(max_open: usize) -> Self {
        Self {
            slots: BTreeMap::new(),
            next_id: 1,
            max_open,
        }
    }

    pub(crate) fn has_capacity(&self) -> Result<(), DbError> {
        match self.slots.len() < self.max_open {
            true => Ok(()),
            false => Err(transaction_limit(self.max_open)),
        }
    }

    pub(crate) fn open(&mut self, client: Session, set_timeout: Statement) -> u32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        self.slots.insert(id, TxSlot::new(client, set_timeout));
        id
    }

    pub(crate) fn slot(&mut self, tx: u32) -> Result<&mut TxSlot, DbError> {
        self.slots
            .get_mut(&tx)
            .ok_or_else(|| transaction_not_found(tx))
    }

    pub(crate) fn remove(&mut self, tx: u32) -> Result<TxSlot, DbError> {
        self.slots
            .remove(&tx)
            .ok_or_else(|| transaction_not_found(tx))
    }

    pub fn open_count(&self) -> usize {
        self.slots.len()
    }

    pub fn abandon_all(&mut self, reason: AbandonReason) {
        std::mem::take(&mut self.slots)
            .into_values()
            .for_each(|slot| slot.abandon(reason));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_session_bounds_open_transactions_and_names_unknown_ones() {
        let mut session = DbSession::new(2);
        assert!(session.has_capacity().is_ok());
        assert_eq!(
            session.slot(7).err().map(|e| e.kind),
            Some(DbErrorKind::TransactionNotFound)
        );
        assert_eq!(
            session.remove(7).err().map(|e| e.kind),
            Some(DbErrorKind::TransactionNotFound)
        );
        let full = DbSession::new(0);
        assert_eq!(
            full.has_capacity().err().map(|e| e.kind),
            Some(DbErrorKind::TransactionLimit)
        );
        assert_eq!(session.open_count(), 0);
    }

    #[test]
    fn abandon_reasons_follow_how_the_execution_ended() {
        use std::time::Duration;
        assert_eq!(AbandonReason::of(&Ok(String::new())), AbandonReason::Leaked);
        assert_eq!(
            AbandonReason::of::<()>(&Err(ExecutionError::DeadlineExceeded {
                budget: Duration::from_secs(1)
            })),
            AbandonReason::Deadline
        );
        assert_eq!(
            AbandonReason::of::<()>(&Err(ExecutionError::ShuttingDown)),
            AbandonReason::Shutdown
        );
        assert_eq!(
            AbandonReason::of::<()>(&Err(ExecutionError::Trap("unreachable".into()))),
            AbandonReason::Trap
        );
        assert_eq!(
            AbandonReason::of::<()>(&Err(ExecutionError::Cancelled)),
            AbandonReason::Trap,
            "a cancelled guest was stopped where it stood, as a trapped one is"
        );
        assert_eq!(AbandonReason::Trap.label(), "trap");
    }
}
