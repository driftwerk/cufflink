//! The rules of an interactive transaction, as a pure state machine: what
//! an open or failed transaction admits, where it stands afterwards, and
//! which savepoints it holds. Whatever holds the connection applies them.

use cufflink_types::db::{DbError, DbErrorKind, Isolation};

/// Where an open transaction stands. After a statement error Postgres has
/// aborted the transaction and only a rollback (to a savepoint or whole)
/// is accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Open,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotOp {
    Statement,
    Savepoint,
    Release,
    RollbackTo,
    Commit,
    Rollback,
}

/// How an operation that ran came back: fine, refused before reaching the
/// server (the transaction is untouched), or failed on the server (which
/// aborts the transaction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Refused,
    ServerError,
}

/// Where the slot goes after an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    Stay(Phase),
    Close,
}

/// Whether `op` may run in `phase`; a failed transaction answers every
/// statement, savepoint and release with the error Postgres would give.
pub fn admit(phase: Phase, op: SlotOp) -> Result<(), DbErrorKind> {
    match (phase, op) {
        (Phase::Open, _)
        | (Phase::Failed, SlotOp::RollbackTo | SlotOp::Rollback | SlotOp::Commit) => Ok(()),
        (Phase::Failed, SlotOp::Statement | SlotOp::Savepoint | SlotOp::Release) => {
            Err(DbErrorKind::InFailedTransaction)
        }
    }
}

/// The slot's phase after an admitted `op` ran with `outcome`. Commit and
/// rollback end the transaction whatever happened; a server error inside
/// it aborts it; a successful rollback to a savepoint recovers it.
pub fn next(phase: Phase, op: SlotOp, outcome: Outcome) -> Next {
    match (op, outcome) {
        (SlotOp::Commit | SlotOp::Rollback, _) => Next::Close,
        (_, Outcome::Refused) => Next::Stay(phase),
        (_, Outcome::ServerError) => Next::Stay(Phase::Failed),
        (SlotOp::RollbackTo, Outcome::Ok) => Next::Stay(Phase::Open),
        (_, Outcome::Ok) => Next::Stay(phase),
    }
}

/// Savepoint names are interpolated into SQL, so only the SDK's own
/// `sp_{depth}` shape is accepted.
pub fn valid_savepoint(name: &str) -> bool {
    name.strip_prefix("sp_")
        .is_some_and(|n| (1..=3).contains(&n.len()) && n.bytes().all(|b| b.is_ascii_digit()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SavepointOp {
    Create,
    Release,
    RollbackTo,
}

impl SavepointOp {
    pub fn slot_op(self) -> SlotOp {
        match self {
            SavepointOp::Create => SlotOp::Savepoint,
            SavepointOp::Release => SlotOp::Release,
            SavepointOp::RollbackTo => SlotOp::RollbackTo,
        }
    }
}

/// The SQL for a savepoint operation, when the name it refers to exists:
/// `RELEASE` and `ROLLBACK TO` an unknown savepoint would abort the whole
/// transaction on the server, so they are refused first.
pub fn savepoint_sql(op: SavepointOp, name: &str, known: Option<usize>) -> Option<String> {
    match (op, known) {
        (SavepointOp::Create, _) => Some(format!("SAVEPOINT {name}")),
        (SavepointOp::Release, Some(_)) => Some(format!("RELEASE SAVEPOINT {name}")),
        (SavepointOp::RollbackTo, Some(_)) => Some(format!("ROLLBACK TO SAVEPOINT {name}")),
        (_, None) => None,
    }
}

/// The savepoint stack after a successful operation: `RELEASE` drops the
/// savepoint and every later one, `ROLLBACK TO` keeps it and drops the later
/// ones.
pub fn savepoints_after(mut stack: Vec<String>, op: SavepointOp, name: &str) -> Vec<String> {
    let known = stack.iter().rposition(|s| s == name);
    match (op, known) {
        (SavepointOp::Create, _) => stack.push(name.to_string()),
        (SavepointOp::Release, Some(at)) => stack.truncate(at),
        (SavepointOp::RollbackTo, Some(at)) => stack.truncate(at + 1),
        (_, None) => {}
    }
    stack
}

pub fn isolation_sql(isolation: Isolation) -> &'static str {
    match isolation {
        Isolation::ReadCommitted => "READ COMMITTED",
        Isolation::RepeatableRead => "REPEATABLE READ",
        Isolation::Serializable => "SERIALIZABLE",
    }
}

/// Transaction control inside a statement, which the host owns.
pub fn transaction_control_refused() -> DbError {
    DbError::new(
        DbErrorKind::NotSupported,
        "transaction control is refused in db_call statements; use begin, commit, \
         rollback and the savepoint operations",
    )
}

/// Parameters on a statement that cannot be prepared inside a transaction.
pub fn parameters_outside_transaction() -> DbError {
    DbError::new(
        DbErrorKind::NotSupported,
        "a statement that cannot run inside a transaction takes no parameters",
    )
}

/// What a failed transaction answers until it is rolled back.
pub fn in_failed_transaction(kind: DbErrorKind) -> DbError {
    DbError::new(
        kind,
        "the transaction was aborted by an earlier error; roll back, or roll back to a savepoint, first",
    )
}

/// A `COMMIT` of a failed transaction, which the server answers by
/// rolling it back.
pub fn rolled_back_by_commit() -> DbError {
    DbError::new(
        DbErrorKind::InFailedTransaction,
        "the transaction was aborted by an earlier error; COMMIT rolled it back",
    )
}

pub fn transaction_not_found(tx: u32) -> DbError {
    DbError::new(
        DbErrorKind::TransactionNotFound,
        format!("no open transaction {tx}"),
    )
}

pub fn transaction_limit(max_open: usize) -> DbError {
    DbError::new(
        DbErrorKind::TransactionLimit,
        format!("at most {max_open} transactions may be open at once"),
    )
}

pub fn invalid_savepoint(name: &str) -> DbError {
    DbError::new(
        DbErrorKind::InvalidParameter,
        format!("invalid savepoint name {name:?}"),
    )
}

pub fn unknown_savepoint(name: &str, tx: u32) -> DbError {
    DbError::new(
        DbErrorKind::InvalidParameter,
        format!("no savepoint {name} in transaction {tx}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusals_name_their_kind() {
        for (refusal, kind) in [
            (transaction_control_refused(), DbErrorKind::NotSupported),
            (parameters_outside_transaction(), DbErrorKind::NotSupported),
            (
                in_failed_transaction(DbErrorKind::InFailedTransaction),
                DbErrorKind::InFailedTransaction,
            ),
            (rolled_back_by_commit(), DbErrorKind::InFailedTransaction),
            (transaction_not_found(7), DbErrorKind::TransactionNotFound),
            (transaction_limit(2), DbErrorKind::TransactionLimit),
            (invalid_savepoint("x"), DbErrorKind::InvalidParameter),
            (unknown_savepoint("sp_1", 7), DbErrorKind::InvalidParameter),
        ] {
            assert_eq!(refusal.kind, kind, "{refusal}");
            assert!(refusal.code.is_none(), "{refusal}");
        }
        assert_eq!(transaction_not_found(7).message, "no open transaction 7");
        assert_eq!(
            transaction_limit(2).message,
            "at most 2 transactions may be open at once"
        );
        assert_eq!(
            unknown_savepoint("sp_1", 7).message,
            "no savepoint sp_1 in transaction 7"
        );
    }

    const PHASES: [Phase; 2] = [Phase::Open, Phase::Failed];
    const OPS: [SlotOp; 6] = [
        SlotOp::Statement,
        SlotOp::Savepoint,
        SlotOp::Release,
        SlotOp::RollbackTo,
        SlotOp::Commit,
        SlotOp::Rollback,
    ];
    const OUTCOMES: [Outcome; 3] = [Outcome::Ok, Outcome::Refused, Outcome::ServerError];

    #[test]
    fn an_open_transaction_admits_everything() {
        for op in OPS {
            assert_eq!(admit(Phase::Open, op), Ok(()), "{op:?}");
        }
    }

    #[test]
    fn a_failed_transaction_admits_only_the_ways_out() {
        for (op, admitted) in [
            (SlotOp::Statement, false),
            (SlotOp::Savepoint, false),
            (SlotOp::Release, false),
            (SlotOp::RollbackTo, true),
            (SlotOp::Commit, true),
            (SlotOp::Rollback, true),
        ] {
            assert_eq!(
                admit(Phase::Failed, op),
                if admitted {
                    Ok(())
                } else {
                    Err(DbErrorKind::InFailedTransaction)
                },
                "{op:?}"
            );
        }
    }

    #[test]
    fn transitions_cover_every_phase_op_and_outcome() {
        for phase in PHASES {
            for op in OPS {
                for outcome in OUTCOMES {
                    let expected = match (op, outcome) {
                        (SlotOp::Commit | SlotOp::Rollback, _) => Next::Close,
                        (_, Outcome::Refused) => Next::Stay(phase),
                        (_, Outcome::ServerError) => Next::Stay(Phase::Failed),
                        (SlotOp::RollbackTo, Outcome::Ok) => Next::Stay(Phase::Open),
                        (_, Outcome::Ok) => Next::Stay(phase),
                    };
                    assert_eq!(
                        next(phase, op, outcome),
                        expected,
                        "{phase:?} {op:?} {outcome:?}"
                    );
                }
            }
        }
        assert_eq!(
            next(Phase::Failed, SlotOp::RollbackTo, Outcome::Ok),
            Next::Stay(Phase::Open),
            "savepoint recovery"
        );
        assert_eq!(
            next(Phase::Open, SlotOp::Statement, Outcome::ServerError),
            Next::Stay(Phase::Failed)
        );
        assert_eq!(
            next(Phase::Open, SlotOp::Statement, Outcome::Refused),
            Next::Stay(Phase::Open),
            "a parameter the host refused never reached the server"
        );
    }

    #[test]
    fn savepoint_names_are_the_sdk_shape_only() {
        for name in ["sp_0", "sp_1", "sp_12", "sp_999"] {
            assert!(valid_savepoint(name), "{name}");
        }
        for name in [
            "sp_",
            "sp_1000",
            "sp_a",
            "sp_1; DROP TABLE x",
            "SP_1",
            "x_1",
            "",
        ] {
            assert!(!valid_savepoint(name), "{name}");
        }
    }

    #[test]
    fn savepoint_sql_refuses_unknown_names_before_the_server_sees_them() {
        assert_eq!(
            savepoint_sql(SavepointOp::Create, "sp_1", None).as_deref(),
            Some("SAVEPOINT sp_1")
        );
        assert_eq!(
            savepoint_sql(SavepointOp::Release, "sp_1", Some(0)).as_deref(),
            Some("RELEASE SAVEPOINT sp_1")
        );
        assert_eq!(
            savepoint_sql(SavepointOp::RollbackTo, "sp_1", Some(0)).as_deref(),
            Some("ROLLBACK TO SAVEPOINT sp_1")
        );
        assert_eq!(savepoint_sql(SavepointOp::Release, "sp_1", None), None);
        assert_eq!(savepoint_sql(SavepointOp::RollbackTo, "sp_1", None), None);
    }

    #[test]
    fn the_savepoint_stack_follows_postgres() {
        let stack = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let three = stack(&["sp_0", "sp_1", "sp_2"]);
        assert_eq!(
            savepoints_after(stack(&["sp_0"]), SavepointOp::Create, "sp_1"),
            stack(&["sp_0", "sp_1"])
        );
        assert_eq!(
            savepoints_after(three.clone(), SavepointOp::Release, "sp_1"),
            stack(&["sp_0"])
        );
        assert_eq!(
            savepoints_after(three.clone(), SavepointOp::RollbackTo, "sp_1"),
            stack(&["sp_0", "sp_1"])
        );
        assert_eq!(
            savepoints_after(stack(&["sp_0", "sp_0"]), SavepointOp::Release, "sp_0"),
            stack(&["sp_0"]),
            "the most recent of two same-named savepoints"
        );
    }

    #[test]
    fn isolation_levels_render_as_sql() {
        assert_eq!(isolation_sql(Isolation::ReadCommitted), "READ COMMITTED");
        assert_eq!(isolation_sql(Isolation::RepeatableRead), "REPEATABLE READ");
        assert_eq!(isolation_sql(Isolation::Serializable), "SERIALIZABLE");
    }
}
