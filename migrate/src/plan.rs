//! What a history leaves to apply.

use crate::history::{history_version, Applied};
use cufflink_types::manifest::validate::MAX_MIGRATION_VERSION;
use cufflink_types::SqlMigration;
use siphasher::sip::SipHasher13;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

/// refinery's checksum of a migration: SipHash-1-3 over its name, its
/// version as an `i32` and its SQL, so a history either runner wrote
/// verifies under the other.
pub fn checksum(migration: &SqlMigration) -> u64 {
    let mut hasher = SipHasher13::new();
    migration.name.hash(&mut hasher);
    history_version(migration.version).hash(&mut hasher);
    migration.sql.as_str().hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// To apply, ascending.
    pub pending: Vec<SqlMigration>,
    /// How many applied migrations match their local file.
    pub verified: usize,
    /// Applied versions accepted under the baseline without a matching
    /// local file.
    pub baselined: Vec<i32>,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum PlanError {
    #[error(
        "migration V{version} diverges from the one applied: applied '{applied_name}' \
         (checksum {applied_checksum}), local '{local_name}' (checksum {local_checksum})"
    )]
    Divergent {
        version: i32,
        applied_name: String,
        local_name: String,
        applied_checksum: u64,
        local_checksum: u64,
    },
    #[error("applied migration V{version}__{name} has no local file")]
    Missing { version: i32, name: String },
    #[error(
        "migration V{version} is not applied and precedes V{max_applied}, which is; \
         a new migration takes a version above every applied one"
    )]
    OutOfOrder { version: u32, max_applied: i32 },
    #[error("migration version {version} is declared twice")]
    Duplicate { version: u32 },
    #[error("migration version {version} is above {MAX_MIGRATION_VERSION}")]
    VersionOutOfRange { version: u32 },
    #[error("history row of version {version} is unreadable: {reason}")]
    BadHistoryRow { version: i32, reason: String },
}

fn by_version(local: &[SqlMigration]) -> Result<BTreeMap<i64, &SqlMigration>, PlanError> {
    local.iter().try_fold(BTreeMap::new(), |mut found, m| {
        if m.version > MAX_MIGRATION_VERSION {
            return Err(PlanError::VersionOutOfRange { version: m.version });
        }
        match found.insert(i64::from(m.version), m) {
            Some(_) => Err(PlanError::Duplicate { version: m.version }),
            None => Ok(found),
        }
    })
}

enum Standing {
    Verified,
    Baselined,
}

fn standing(
    applied: &Applied,
    local: Option<&SqlMigration>,
    baseline: Option<u32>,
) -> Result<Standing, PlanError> {
    let under_baseline =
        baseline.is_some_and(|baseline| i64::from(applied.version) <= i64::from(baseline));
    match local.map(|local| (local, checksum(local))) {
        Some((local, sum)) if local.name == applied.name && sum == applied.checksum => {
            Ok(Standing::Verified)
        }
        _ if under_baseline => Ok(Standing::Baselined),
        Some((local, sum)) => Err(PlanError::Divergent {
            version: applied.version,
            applied_name: applied.name.clone(),
            local_name: local.name.clone(),
            applied_checksum: applied.checksum,
            local_checksum: sum,
        }),
        None => Err(PlanError::Missing {
            version: applied.version,
            name: applied.name.clone(),
        }),
    }
}

/// Check `applied` against the `local` files and list what is left to
/// apply. An applied version above `baseline` must have its local file,
/// unchanged; one at or under it is accepted as it is.
pub fn plan(
    applied: &[Applied],
    local: &[SqlMigration],
    baseline: Option<u32>,
) -> Result<Plan, PlanError> {
    let files = by_version(local)?;
    let (verified, baselined) =
        applied
            .iter()
            .try_fold((0usize, Vec::new()), |(verified, mut baselined), row| {
                let file = files.get(&i64::from(row.version)).copied();
                standing(row, file, baseline).map(|standing| match standing {
                    Standing::Verified => (verified + 1, baselined),
                    Standing::Baselined => {
                        baselined.push(row.version);
                        (verified, baselined)
                    }
                })
            })?;
    let max_applied = applied.iter().map(|row| row.version).max();
    let is_applied = |version: i64| applied.iter().any(|row| i64::from(row.version) == version);
    let pending: Vec<SqlMigration> = files
        .iter()
        .filter(|(version, _)| !is_applied(**version))
        .map(|(version, file)| match max_applied {
            Some(max_applied) if *version < i64::from(max_applied) => Err(PlanError::OutOfOrder {
                version: file.version,
                max_applied,
            }),
            _ => Ok((*file).clone()),
        })
        .collect::<Result<_, _>>()?;
    Ok(Plan {
        pending,
        verified,
        baselined,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(version: u32, name: &str, sql: &str) -> SqlMigration {
        SqlMigration {
            version,
            name: name.into(),
            sql: sql.into(),
            no_transaction: false,
        }
    }

    fn row(file: &SqlMigration) -> Applied {
        Applied {
            version: file.version as i32,
            name: file.name.clone(),
            checksum: checksum(file),
            applied_on: "2024-05-01T09:30:00Z".into(),
        }
    }

    fn foreign(version: i32) -> Applied {
        Applied {
            version,
            name: format!("deferred_{version}"),
            checksum: 7,
            applied_on: "2024-05-01T09:30:00Z".into(),
        }
    }

    fn loan_service() -> Vec<SqlMigration> {
        vec![
            file(
                1,
                "initial_tables",
                "CREATE TABLE loans (id UUID PRIMARY KEY);\n",
            ),
            file(
                6,
                "add_loan_kind",
                "ALTER TABLE loans ADD COLUMN kind TEXT;\r\n",
            ),
            file(
                7,
                "renewal_index",
                "CREATE INDEX loans_renewal ON loans (id);",
            ),
        ]
    }

    fn versions(plan: &Plan) -> Vec<u32> {
        plan.pending.iter().map(|m| m.version).collect()
    }

    #[test]
    fn the_checksum_is_refinerys() {
        for migration in loan_service().into_iter().chain([
            file(2_147_483_647, "last", "SELECT 1;"),
            file(0, "zero", "\u{feff}-- cufflink:no-transaction\nSELECT 'é';"),
            file(
                8,
                "windows_line_endings",
                "CREATE TABLE loans (\r\n  id uuid PRIMARY KEY\r\n);\r\n",
            ),
        ]) {
            let refinery =
                refinery_core::Migration::unapplied(&migration.file_stem(), &migration.sql)
                    .expect("a refinery migration");
            assert_eq!(checksum(&migration), refinery.checksum(), "{migration:?}");
        }
    }

    #[test]
    fn a_fresh_history_applies_everything_in_order() {
        let mut local = loan_service();
        local.reverse();
        let plan = plan(&[], &local, None).expect("plan");
        assert_eq!(versions(&plan), [1, 6, 7]);
        assert_eq!((plan.verified, plan.baselined), (0, vec![]));
    }

    #[test]
    fn a_history_is_continued_after_a_gap() {
        let local = loan_service();
        let applied = [row(&local[0]), row(&local[1])];
        let plan = plan(&applied, &local, None).expect("plan");
        assert_eq!(versions(&plan), [7]);
        assert_eq!((plan.verified, plan.baselined), (2, vec![]));
        let done = [row(&local[0]), row(&local[1]), row(&local[2])];
        assert_eq!(
            super::plan(&done, &local, None),
            Ok(Plan {
                pending: vec![],
                verified: 3,
                baselined: vec![]
            })
        );
    }

    #[test]
    fn a_changed_name_or_checksum_diverges() {
        let local = loan_service();
        let renamed = Applied {
            name: "add_kind".into(),
            ..row(&local[1])
        };
        assert_eq!(
            plan(&[row(&local[0]), renamed], &local, None),
            Err(PlanError::Divergent {
                version: 6,
                applied_name: "add_kind".into(),
                local_name: "add_loan_kind".into(),
                applied_checksum: checksum(&local[1]),
                local_checksum: checksum(&local[1]),
            })
        );
        let edited = Applied {
            checksum: 42,
            ..row(&local[1])
        };
        let error = plan(&[row(&local[0]), edited], &local, None).expect_err("diverges");
        assert_eq!(
            error.to_string(),
            format!(
                "migration V6 diverges from the one applied: applied 'add_loan_kind' \
                 (checksum 42), local 'add_loan_kind' (checksum {})",
                checksum(&local[1])
            )
        );
    }

    #[test]
    fn an_applied_migration_needs_its_file_above_the_baseline() {
        let local = loan_service();
        let applied = [row(&local[0]), foreign(3), row(&local[1])];
        assert_eq!(
            plan(&applied, &local, None),
            Err(PlanError::Missing {
                version: 3,
                name: "deferred_3".into()
            })
        );
        assert_eq!(
            plan(&applied, &local, Some(2)),
            Err(PlanError::Missing {
                version: 3,
                name: "deferred_3".into()
            })
        );
        let plan = plan(&applied, &local, Some(3)).expect("plan");
        assert_eq!(versions(&plan), [7]);
        assert_eq!((plan.verified, plan.baselined), (2, vec![3]));
    }

    #[test]
    fn a_history_under_the_baseline_is_accepted_unverified() {
        let applied: Vec<Applied> = (1..=15).map(foreign).collect();
        let local: Vec<SqlMigration> = (14..=16)
            .map(|v| file(v, &format!("report_{v}"), "SELECT 1;"))
            .collect();
        let plan = plan(&applied, &local, Some(15)).expect("plan");
        assert_eq!(versions(&plan), [16]);
        assert_eq!(plan.verified, 0);
        assert_eq!(plan.baselined, (1..=15).collect::<Vec<i32>>());
    }

    #[test]
    fn a_new_file_under_an_applied_version_is_out_of_order() {
        let mut local = loan_service();
        let applied = [row(&local[0]), row(&local[1])];
        local.push(file(4, "late", "SELECT 1;"));
        assert_eq!(
            plan(&applied, &local, None),
            Err(PlanError::OutOfOrder {
                version: 4,
                max_applied: 6
            })
        );
        assert_eq!(
            plan(&applied, &local, Some(6)),
            Err(PlanError::OutOfOrder {
                version: 4,
                max_applied: 6
            })
        );
    }

    #[test]
    fn line_endings_are_part_of_what_is_checksummed() {
        assert_ne!(
            checksum(&file(8, "loans", "SELECT 1;\r\nSELECT 2;\r\n")),
            checksum(&file(8, "loans", "SELECT 1;\nSELECT 2;\n")),
            "a checkout that rewrites line endings diverges, as it does under refinery"
        );
    }

    #[test]
    fn a_version_the_history_cannot_hold_is_refused() {
        let mut local = loan_service();
        local.push(file(2_147_483_648, "too_far", "SELECT 1;"));
        assert_eq!(
            plan(&[], &local, None),
            Err(PlanError::VersionOutOfRange {
                version: 2_147_483_648
            })
        );
    }

    #[test]
    fn a_version_declared_twice_is_refused() {
        let mut local = loan_service();
        local.push(file(6, "again", "SELECT 1;"));
        assert_eq!(
            plan(&[], &local, None),
            Err(PlanError::Duplicate { version: 6 })
        );
    }
}
