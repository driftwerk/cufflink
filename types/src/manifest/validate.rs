//! The rules of the manifest's R3 fields, shared by the macros, the CLI
//! and the platform. Every error names the field it refuses.

use super::{
    BackoffSpec, CheckConstraint, ColumnAccess, ColumnDefinition, ColumnType, ConsumerSpec,
    IndexDefinition, IndexMethod, JobConfig, MigrationHistory, ProvidedExtension,
    ScheduleDefinition, ScheduleSpec, SearchSpec, ServiceMode, SqlMigration, StreamDefinition,
    SubscriptionDefinition, TableDefinition,
};
use crate::ext::{callers, name_violation};
use crate::nats_names::{is_valid_consumer_name, is_valid_stream_name, pattern_violation};
use std::collections::BTreeSet;
use std::fmt::Display;
use std::ops::RangeInclusive;

/// The longest name Postgres keeps without truncating it.
pub const IDENTIFIER_MAX: usize = 63;
const RESERVED_SCHEMAS: [&str; 2] = ["public", "information_schema"];
/// Postgres's own schemas, and those the platform keeps in a tenant
/// database.
const RESERVED_SCHEMA_PREFIXES: [&str; 2] = ["pg_", "_cufflink"];
const DESCRIPTION_MAX: usize = 4096;

fn within<T: PartialOrd + Display>(
    path: &str,
    field: &str,
    value: Option<T>,
    range: &RangeInclusive<T>,
) -> Result<(), String> {
    match value {
        Some(value) if !range.contains(&value) => Err(format!(
            "{path}.{field}: {value} is outside {}..={}",
            range.start(),
            range.end()
        )),
        _ => Ok(()),
    }
}

fn positive(path: &str, field: &str, value: Option<i64>) -> Result<(), String> {
    match value {
        Some(value) if value < 1 => Err(format!("{path}.{field}: {value} is not positive")),
        _ => Ok(()),
    }
}

fn distinct<'a>(
    path: &str,
    what: &str,
    values: impl IntoIterator<Item = &'a str>,
) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    match values.into_iter().find(|value| !seen.insert(*value)) {
        Some(repeated) => Err(listed_twice(path, what, repeated)),
        None => Ok(()),
    }
}

fn listed_twice(path: &str, what: &str, repeated: &str) -> String {
    format!("{path}: {what} '{repeated}' is listed twice")
}

/// `^[a-z_][a-z0-9_]{0,62}$`
pub fn is_lowercase_identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= IDENTIFIER_MAX
        && bytes
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// `^[A-Za-z_][A-Za-z0-9_]{0,62}$`
pub fn is_identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    name.len() <= IDENTIFIER_MAX
        && bytes
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn identifier(path: &str, name: &str) -> Result<(), String> {
    if is_identifier(name) {
        Ok(())
    } else {
        Err(format!(
            "{path}: '{name}' is not an identifier (ASCII letters, digits and '_', \
             not starting with a digit, at most {IDENTIFIER_MAX} characters)"
        ))
    }
}

/// `name` as Postgres folds an identifier written without quotes.
pub fn folded(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// `name` quoted for a statement, naming what the unquoted name names.
/// Every statement that names a schema, or a table or column of a
/// manifest, writes it this way: one that is an SQL keyword is still a
/// name, and a name with capitals is the object Postgres folded it to when
/// it was created unquoted.
pub fn folded_ident(name: &str) -> String {
    format!("\"{}\"", folded(name).replace('"', "\"\""))
}

/// Whether no service may have the schema `name`.
pub fn is_reserved_schema(name: &str) -> bool {
    let name = folded(name);
    RESERVED_SCHEMAS.contains(&name.as_str())
        || RESERVED_SCHEMA_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

pub fn validate_schema_name(path: &str, name: &str) -> Result<(), String> {
    if !is_lowercase_identifier(name) {
        return Err(format!(
            "{path}: '{name}' is not a schema name (lowercase letters, digits and '_', \
             not starting with a digit, at most {IDENTIFIER_MAX} characters)"
        ));
    }
    if is_reserved_schema(name) {
        return Err(format!("{path}: schema '{name}' is reserved"));
    }
    Ok(())
}

/// A fragment of SQL a manifest places inside a statement the platform
/// writes: it may not end that statement, hide the rest of it, or close a
/// parenthesis it did not open. A parenthesis inside a string literal or
/// a quoted identifier is text. Dollar quoting is refused, and so is a
/// backslash before a quote in a literal, the one place where an `E''`
/// string and a standard one end apart.
pub fn validate_sql_fragment(path: &str, sql: &str) -> Result<(), String> {
    if sql.trim().is_empty() {
        return Err(format!("{path}: is empty"));
    }
    if let Some(found) = [";", "--", "/*", "*/"]
        .into_iter()
        .find(|token| sql.contains(token))
    {
        return Err(format!("{path}: contains '{found}'"));
    }
    match depth(sql) {
        Ok(0) => Ok(()),
        Ok(_) => Err(format!("{path}: has unbalanced parentheses")),
        Err(reason) => Err(format!("{path}: {reason}")),
    }
}

/// The parentheses `sql` leaves open, read as Postgres reads its quotes.
fn depth(sql: &str) -> Result<u32, &'static str> {
    let mut chars = sql.chars();
    let mut depth = 0u32;
    while let Some(c) = chars.next() {
        depth = match c {
            '(' => depth.checked_add(1).ok_or("has unbalanced parentheses")?,
            ')' => depth.checked_sub(1).ok_or("has unbalanced parentheses")?,
            '$' => return Err("contains '$' outside a quoted string"),
            '\'' | '"' => {
                quoted(&mut chars, c)?;
                depth
            }
            _ => depth,
        };
    }
    Ok(depth)
}

/// Read past the literal or identifier `quote` opened, to the quote that
/// ends it; a doubled quote is one of its characters.
fn quoted(chars: &mut std::str::Chars<'_>, quote: char) -> Result<(), &'static str> {
    let mut previous = None;
    loop {
        match chars.next() {
            None => return Err("has a quote that is not closed"),
            Some(c) if c == quote => {
                if quote == '\'' && previous == Some('\\') {
                    return Err("has a backslash before a quote in a string literal");
                }
                if !chars.as_str().starts_with(quote) {
                    return Ok(());
                }
                chars.next();
                previous = None;
            }
            c => previous = c,
        }
    }
}

pub fn validate_consumer_spec(path: &str, spec: &ConsumerSpec) -> Result<(), String> {
    if let Some(name) = spec.durable_name.as_deref() {
        if !is_valid_consumer_name(name) {
            return Err(format!(
                "{path}.durable_name: '{name}' is not a consumer name \
                 (ASCII letters, digits, '-' and '_', at most 200 characters)"
            ));
        }
    }
    within(
        path,
        "ack_wait_secs",
        spec.ack_wait_secs,
        &ConsumerSpec::ACK_WAIT_SECS,
    )?;
    match spec.max_deliver {
        Some(n) if n != ConsumerSpec::UNLIMITED_DELIVERIES => {
            within(path, "max_deliver", Some(n), &ConsumerSpec::MAX_DELIVER)
                .map_err(|e| format!("{e} (or -1 for no limit)"))?
        }
        _ => {}
    }
    if spec.backoff_secs.len() > ConsumerSpec::MAX_BACKOFF_STEPS {
        return Err(format!(
            "{path}.backoff_secs: {} steps, more than the {} allowed",
            spec.backoff_secs.len(),
            ConsumerSpec::MAX_BACKOFF_STEPS
        ));
    }
    spec.backoff_secs
        .iter()
        .enumerate()
        .try_for_each(|(i, s)| {
            within(
                path,
                &format!("backoff_secs[{i}]"),
                Some(*s),
                &ConsumerSpec::BACKOFF_SECS,
            )
        })?;
    within(
        path,
        "max_ack_pending",
        spec.max_ack_pending,
        &ConsumerSpec::MAX_ACK_PENDING,
    )?;
    within(path, "batch", spec.batch, &ConsumerSpec::BATCH)?;
    within(
        path,
        "concurrency",
        spec.concurrency,
        &ConsumerSpec::CONCURRENCY,
    )?;
    patterns(&format!("{path}.filter_subjects"), &spec.filter_subjects)?;
    distinct(
        &format!("{path}.filter_subjects"),
        "subject",
        spec.filter_subjects.iter().map(String::as_str),
    )
}

/// Every one of `field`'s subject patterns.
fn patterns(field: &str, declared: &[String]) -> Result<(), String> {
    declared
        .iter()
        .enumerate()
        .find_map(|(i, pattern)| {
            pattern_violation(pattern).map(|reason| format!("{field}[{i}]: {reason}"))
        })
        .map_or(Ok(()), Err)
}

/// The subject and delivery fields of a subscription.
pub fn validate_subscription_delivery(
    path: &str,
    subscription: &SubscriptionDefinition,
) -> Result<(), String> {
    pattern_violation(&subscription.subject)
        .map_or(Ok(()), |reason| Err(format!("{path}.subject: {reason}")))?;
    within(
        path,
        "timeout_secs",
        subscription.timeout_secs,
        &SubscriptionDefinition::TIMEOUT_SECS,
    )?;
    subscription.consumer.as_ref().map_or(Ok(()), |spec| {
        validate_consumer_spec(&format!("{path}.consumer"), spec)
    })
}

pub fn validate_stream_definition(path: &str, stream: &StreamDefinition) -> Result<(), String> {
    if !is_valid_stream_name(&stream.name) {
        return Err(format!(
            "{path}.name: '{}' is not a stream name (A-Z, 0-9 and '_', at most 64 characters)",
            stream.name
        ));
    }
    if stream.subjects.is_empty() {
        return Err(format!("{path}.subjects: is empty"));
    }
    patterns(&format!("{path}.subjects"), &stream.subjects)?;
    distinct(
        &format!("{path}.subjects"),
        "subject",
        stream.subjects.iter().map(String::as_str),
    )?;
    within(path, "max_age_secs", stream.max_age_secs, &(1..=u64::MAX))?;
    positive(path, "max_msgs", stream.max_msgs)?;
    positive(path, "max_bytes", stream.max_bytes)?;
    within(
        path,
        "duplicate_window_secs",
        stream.duplicate_window_secs,
        &(1..=u64::MAX),
    )?;
    match (stream.duplicate_window_secs, stream.max_age_secs) {
        (Some(window), Some(age)) if window > age => Err(format!(
            "{path}.duplicate_window_secs: {window} is longer than max_age_secs {age}"
        )),
        _ => Ok(()),
    }?;
    match stream.description.as_deref() {
        Some(text) if text.len() > DESCRIPTION_MAX => Err(format!(
            "{path}.description: is longer than {DESCRIPTION_MAX} bytes"
        )),
        _ => Ok(()),
    }
}

pub fn validate_job_config(path: &str, job: &JobConfig) -> Result<(), String> {
    distinct(
        &format!("{path}.retry_on"),
        "failure",
        job.retry_on.iter().map(|retry| retry.as_str()),
    )?;
    let Some(backoff) = job.backoff else {
        return Ok(());
    };
    let path = format!("{path}.backoff");
    within(
        &path,
        "base_secs",
        Some(backoff.base_secs),
        &BackoffSpec::SECS,
    )?;
    within(
        &path,
        "max_secs",
        Some(backoff.max_secs),
        &BackoffSpec::SECS,
    )?;
    if backoff.max_secs < backoff.base_secs {
        return Err(format!(
            "{path}.max_secs: {} is below base_secs {}",
            backoff.max_secs, backoff.base_secs
        ));
    }
    Ok(())
}

/// Everything about a schedule but whether its cron expression parses,
/// which whoever compiles the expression reports.
pub fn validate_schedule(path: &str, schedule: &ScheduleDefinition) -> Result<(), String> {
    if !ScheduleDefinition::is_valid_name(&schedule.name) {
        return Err(format!(
            "{path}.name: '{}' is not a schedule name (a-z, 0-9 and '_', at most {} characters)",
            schedule.name,
            ScheduleDefinition::NAME_MAX
        ));
    }
    identifier(&format!("{path}.handler"), &schedule.handler)?;
    match &schedule.spec {
        ScheduleSpec::Every { secs } if *secs < ScheduleDefinition::MIN_INTERVAL_SECS => {
            return Err(format!(
                "{path}.spec.secs: {secs} is below the {} second minimum",
                ScheduleDefinition::MIN_INTERVAL_SECS
            ));
        }
        ScheduleSpec::Every { .. } if schedule.timezone.is_some() => {
            return Err(format!(
                "{path}.timezone: applies to a cron schedule, not to every(..)"
            ));
        }
        ScheduleSpec::Cron { expr } if !matches!(expr.split_whitespace().count(), 5 | 6) => {
            return Err(format!(
                "{path}.spec.expr: '{expr}' does not have five fields, or six with seconds"
            ));
        }
        _ => {}
    }
    match schedule.timezone.as_deref() {
        Some(zone) if !is_timezone_name(zone) => {
            return Err(format!(
                "{path}.timezone: '{zone}' is not an IANA time zone name"
            ));
        }
        _ => {}
    }
    within(
        path,
        "timeout_secs",
        schedule.timeout_secs,
        &ScheduleDefinition::TIMEOUT_SECS,
    )?;
    match &schedule.job {
        Some(_) if schedule.timeout_secs.is_some() => Err(format!(
            "{path}.timeout_secs: applies to an inline handler; a job's is job.timeout_secs"
        )),
        Some(job) => validate_job_config(&format!("{path}.job"), job),
        None => Ok(()),
    }
}

fn is_timezone_name(zone: &str) -> bool {
    (1..=64).contains(&zone.len())
        && zone.split('/').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'+'))
        })
}

/// The schedules of one service: each valid, their names distinct.
pub fn validate_schedules(path: &str, schedules: &[ScheduleDefinition]) -> Result<(), String> {
    schedules
        .iter()
        .enumerate()
        .try_for_each(|(i, schedule)| validate_schedule(&format!("{path}[{i}]"), schedule))?;
    distinct(path, "schedule", schedules.iter().map(|s| s.name.as_str()))
}

/// The streams of one service: each valid, their names distinct.
pub fn validate_streams(path: &str, streams: &[StreamDefinition]) -> Result<(), String> {
    streams
        .iter()
        .enumerate()
        .try_for_each(|(i, stream)| validate_stream_definition(&format!("{path}[{i}]"), stream))?;
    distinct(path, "stream", streams.iter().map(|s| s.name.as_str()))
}

/// ASCII letters, digits and `_`, at most 255 characters.
pub fn is_migration_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// The largest version a migration may have: the history table keeps it in
/// an `INT4`.
pub const MAX_MIGRATION_VERSION: u32 = i32::MAX as u32;

pub fn validate_sql_migrations(
    path: &str,
    migrations: &[SqlMigration],
    history: Option<&MigrationHistory>,
) -> Result<(), String> {
    migrations.iter().enumerate().try_for_each(|(i, m)| {
        let path = format!("{path}[{i}]");
        if m.version > MAX_MIGRATION_VERSION {
            return Err(format!(
                "{path}.version: {} is above {MAX_MIGRATION_VERSION}",
                m.version
            ));
        }
        if !is_migration_name(&m.name) {
            return Err(format!(
                "{path}.name: '{}' is not a migration name (ASCII letters, digits and '_')",
                m.name
            ));
        }
        if m.sql.trim().is_empty() {
            return Err(format!("{path}.sql: is empty"));
        }
        if m.no_transaction != SqlMigration::declares_no_transaction(&m.sql) {
            return Err(format!(
                "{path}.no_transaction: disagrees with the first line of the SQL \
                 ('{}' opts out of the transaction)",
                SqlMigration::NO_TRANSACTION_MARKER
            ));
        }
        Ok(())
    })?;
    let ascending = migrations
        .windows(2)
        .find(|pair| pair[0].version >= pair[1].version);
    if let Some(pair) = ascending {
        return Err(format!(
            "{path}: version {} follows version {}; versions are distinct and ascending",
            pair[1].version, pair[0].version
        ));
    }
    history.map_or(Ok(()), |history| {
        validate_migration_history("migration_history", history)
    })
}

pub fn validate_migration_history(path: &str, history: &MigrationHistory) -> Result<(), String> {
    let parts: Vec<&str> = history.table.split('.').collect();
    if parts.len() > 2 || !parts.iter().all(|part| is_lowercase_identifier(part)) {
        return Err(format!(
            "{path}.table: '{}' is not 'table' or 'schema.table' in lowercase",
            history.table
        ));
    }
    within(
        path,
        "baseline",
        history.baseline,
        &(0..=MAX_MIGRATION_VERSION),
    )
}

pub fn validate_column_type(path: &str, column_type: &ColumnType) -> Result<(), String> {
    match column_type {
        ColumnType::Numeric {
            precision: None,
            scale: Some(_),
        } => Err(format!("{path}: a numeric scale needs a precision")),
        ColumnType::Numeric {
            precision: Some(precision),
            scale,
        } => {
            within(
                path,
                "precision",
                Some(*precision),
                &(1..=ColumnType::MAX_NUMERIC_PRECISION),
            )?;
            within(path, "scale", *scale, &(0..=*precision))
        }
        ColumnType::Varchar { length } => within(
            path,
            "length",
            Some(*length),
            &(1..=ColumnType::MAX_VARCHAR_LENGTH),
        ),
        _ => Ok(()),
    }
}

pub fn validate_check_constraint(path: &str, check: &CheckConstraint) -> Result<(), String> {
    identifier(&format!("{path}.name"), &check.name)?;
    validate_sql_fragment(&format!("{path}.expr"), &check.expr)
}

/// The checks of one table: each valid, their names distinct.
pub fn validate_check_constraints(path: &str, checks: &[CheckConstraint]) -> Result<(), String> {
    checks
        .iter()
        .enumerate()
        .try_for_each(|(i, check)| validate_check_constraint(&format!("{path}[{i}]"), check))?;
    distinct(path, "check", checks.iter().map(|c| c.name.as_str()))
}

pub fn validate_index(path: &str, index: &IndexDefinition) -> Result<(), String> {
    identifier(&format!("{path}.name"), &index.name)?;
    if index.columns.is_empty() {
        return Err(format!("{path}.columns: is empty"));
    }
    index
        .columns
        .iter()
        .enumerate()
        .try_for_each(|(i, column)| identifier(&format!("{path}.columns[{i}]"), column))?;
    distinct(
        &format!("{path}.columns"),
        "column",
        index.columns.iter().map(String::as_str),
    )?;
    if index.unique && index.method != IndexMethod::Btree {
        return Err(format!(
            "{path}.method: a unique index is a btree index, not {}",
            index.method.as_sql()
        ));
    }
    if index.columns.len() > 1 && matches!(index.method, IndexMethod::Hash) {
        return Err(format!("{path}.method: a hash index has one column"));
    }
    index.predicate.as_deref().map_or(Ok(()), |predicate| {
        validate_sql_fragment(&format!("{path}.predicate"), predicate)
    })
}

pub fn validate_search_spec(path: &str, search: &SearchSpec) -> Result<(), String> {
    if search.columns.is_empty() && search.vector_column.is_none() {
        return Err(format!("{path}: names neither columns nor a vector_column"));
    }
    search
        .columns
        .iter()
        .enumerate()
        .try_for_each(|(i, column)| identifier(&format!("{path}.columns[{i}]"), column))?;
    distinct(
        &format!("{path}.columns"),
        "column",
        search.columns.iter().map(String::as_str),
    )?;
    if !is_lowercase_identifier(&search.config) {
        return Err(format!(
            "{path}.config: '{}' is not a text search configuration name",
            search.config
        ));
    }
    search.vector_column.as_deref().map_or(Ok(()), |column| {
        identifier(&format!("{path}.vector_column"), column)
    })
}

/// Audit redaction rules: a path of keys joined by `.`, each a name or a
/// prefix ending in `*`, and followed by `[]` where its value is an array
/// whose elements the rest of the path is of.
pub fn validate_audit_redact(path: &str, rules: &[String]) -> Result<(), String> {
    rules.iter().enumerate().try_for_each(|(i, rule)| {
        rule.split('.')
            .try_for_each(|key| {
                let name = key.strip_suffix("[]").unwrap_or(key);
                identifier(
                    &format!("{path}[{i}]"),
                    name.strip_suffix('*').unwrap_or(name),
                )
            })
            .map_err(|e| {
                format!(
                    "{e}; a rule is a column name or 'prefix*', and the keys inside it joined by \
                 '.', a key whose value is an array followed by '[]'"
                )
            })
    })?;
    distinct(path, "rule", rules.iter().map(String::as_str))
}

/// The column the offset listings of every table are ordered by when a
/// request names no sort.
const DEFAULT_SORT_COLUMN: &str = "created_at";

/// Why `column` of `table` cannot ask a grant at all, where it cannot: the
/// data routes address, scope and order rows by it whoever asks.
fn unrestrictable(table: &TableDefinition, column: &ColumnDefinition) -> Option<&'static str> {
    if column.primary_key {
        Some("the primary key is how every caller addresses a row")
    } else if table.owner_field.as_deref() == Some(column.name.as_str()) {
        Some("the owner column is what the table's rows are scoped by")
    } else if column.auto_generate {
        Some("the platform sets the column itself, and orders listings by it")
    } else {
        None
    }
}

fn validate_access(
    path: &str,
    table: &TableDefinition,
    column: &ColumnDefinition,
    access: &ColumnAccess,
) -> Result<(), String> {
    let operations = [("read", &access.read), ("write", &access.write)];
    if operations.iter().all(|(_, operation)| operation.is_none()) {
        return Err(format!("{path}: names neither read nor write"));
    }
    operations
        .iter()
        .filter_map(|(field, operation)| operation.as_deref().map(|operation| (field, operation)))
        .try_for_each(|(field, operation)| identifier(&format!("{path}.{field}"), operation))?;
    if table.access_area().is_none() {
        return Err(format!(
            "{path}: a column permission names an operation of the table's permission_area, \
             and the table declares none"
        ));
    }
    if let Some(reason) = unrestrictable(table, column) {
        return Err(format!("{path}: {reason}; it cannot be restricted"));
    }
    if access.read.is_some() && column.name == DEFAULT_SORT_COLUMN {
        return Err(format!(
            "{path}.read: listings are ordered by '{DEFAULT_SORT_COLUMN}' for every caller"
        ));
    }
    if access.write.is_some() && !column.nullable && column.default_value.is_none() {
        return Err(format!(
            "{path}.write: every create sets the column (it is neither nullable nor has a \
             default), so a caller without the grant could create no row"
        ));
    }
    let unique = column.unique
        || table
            .indexes
            .iter()
            .any(|index| index.unique && index.columns.contains(&column.name));
    if unique && access.read.is_some() && access.write.is_none() {
        return Err(format!(
            "{path}: the column is unique, so a caller that may write it learns the values \
             other rows hold from the conflicts; restrict write as well"
        ));
    }
    Ok(())
}

/// The column permissions of `table`: each names an operation of the
/// table's permission area to read the column, to write it or both, on a
/// column the data routes do not themselves depend on, and none hides a
/// column the table's declared search matches for every caller.
pub fn validate_column_access(path: &str, table: &TableDefinition) -> Result<(), String> {
    table
        .restricted_columns()
        .try_for_each(|(column, access)| {
            validate_access(
                &format!("{path}.columns.{}.access", column.name),
                table,
                column,
                access,
            )
        })?;
    let hidden = |name: &String| {
        table
            .restricted_columns()
            .any(|(column, access)| column.name == *name && access.read.is_some())
    };
    let Some(search) = &table.search else {
        return Ok(());
    };
    if let Some(name) = search.columns.iter().find(|name| hidden(name)) {
        return Err(format!(
            "{path}.search.columns: '{name}' is read only with a grant, and the declared \
             search matches it for every caller"
        ));
    }
    let withheld = table.columns.iter().find(|column| hidden(&column.name));
    match (&search.vector_column, withheld) {
        (Some(vector), Some(column)) => Err(format!(
            "{path}.search.vector_column: '{vector}' is a vector the platform does not keep, \
             which may hold '{}', read only with a grant, and the search matches it for every \
             caller",
            column.name
        )),
        _ => Ok(()),
    }
}

/// The extensions a service calls: each an extension name, none listed
/// twice.
pub fn validate_extensions(path: &str, names: &[String]) -> Result<(), String> {
    refused_extension(path, names).map_or(Ok(()), |(_, reason)| Err(reason))
}

/// The first of `names` that is refused, by its index, and why: one that
/// is not an extension name, or one listed before it.
pub fn refused_extension(path: &str, names: &[String]) -> Option<(usize, String)> {
    let mut seen = BTreeSet::new();
    names.iter().enumerate().find_map(|(i, name)| {
        name_violation(name)
            .map(|violation| format!("{path}[{i}]: {violation}"))
            .or_else(|| {
                (!seen.insert(name.as_str())).then(|| listed_twice(path, "extension", name))
            })
            .map(|reason| (i, reason))
    })
}

/// The extensions a native service provides: at least one, each an
/// extension name listed once, each with the callers [`callers`] stores.
pub fn validate_provides(path: &str, provides: &[ProvidedExtension]) -> Result<(), String> {
    if provides.is_empty() {
        return Err(format!(
            "{path}: a native service provides at least one extension"
        ));
    }
    let names: Vec<String> = provides.iter().map(|p| p.name.clone()).collect();
    validate_extensions(path, &names)?;
    provides.iter().enumerate().try_for_each(|(i, provided)| {
        callers(provided.callers.clone())
            .map(drop)
            .map_err(|e| format!("{path}[{i}].{e}"))
    })
}

/// The declarations of `table` that only data routes read. `scoped_by`
/// with its owner column and `soft_delete` are not among them: scoped
/// custom routes and the adoption check read those.
pub fn served_only_declarations(table: &TableDefinition) -> impl Iterator<Item = &'static str> {
    let area = table.permission_area.as_ref();
    [
        table.hooks.is_some().then_some("hooks"),
        table.auth_required.then_some("auth_required"),
        area.is_some_and(|area| table.scoped_by.as_ref() != Some(area))
            .then_some("permission_area"),
        (table.owner_field.is_some() && table.scoped_by.is_none()).then_some("owner_field"),
        table.owner_self_service.then_some("owner_self_service"),
        table.search.is_some().then_some("search"),
        (!table.pagination.is_offset()).then_some("pagination"),
        table.sensitive.then_some("sensitive"),
        (!table.audit_redact.is_empty()).then_some("audit_redact"),
        table
            .columns
            .iter()
            .any(|column| !column.validations.is_empty())
            .then_some("validations"),
        table
            .restricted_columns()
            .next()
            .map(|_| "column permissions"),
    ]
    .into_iter()
    .flatten()
}

/// A private table declares nothing that only data routes read.
pub fn validate_private_table(table: &TableDefinition) -> Result<(), String> {
    served_only_declarations(table)
        .next()
        .map_or(Ok(()), |declaration| {
            Err(format!(
                "Private table '{}' declares {declaration}, which only data routes read",
                table.name
            ))
        })
}

/// Why a service of `mode` cannot be deployed inactive, where it cannot:
/// the flag would promise what a service's status does not gate.
pub fn inactive_refusal(mode: &ServiceMode) -> Option<&'static str> {
    match mode {
        ServiceMode::Web => Some(
            "a web app is started by its runner whatever its service's status; \
             deploy it without --inactive",
        ),
        ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Native => {
            None
        }
    }
}

/// Why a deploy asked not to start the service `name` is refused while it
/// runs.
pub fn active_refusal(name: &str) -> String {
    format!(
        "Service '{name}' is active; a deploy never stops a running service. \
         Disable it first, or deploy without --inactive"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_names::SUBJECT_MAX;
    use crate::{
        CatchUp, DiscardSpec, HandlerType, Overlap, RetryOn, StreamRetention, StreamStorage,
    };

    #[test]
    fn only_a_web_app_is_refused_inactive_and_a_running_service_is_named() {
        use ServiceMode::{Container, Crud, Native, Wasm, Web};
        assert_eq!(
            inactive_refusal(&Web),
            Some(
                "a web app is started by its runner whatever its service's status; \
                 deploy it without --inactive"
            )
        );
        for mode in [Crud, Wasm, Container, Native] {
            assert_eq!(inactive_refusal(&mode), None, "{mode:?}");
        }
        assert_eq!(
            active_refusal("loans"),
            "Service 'loans' is active; a deploy never stops a running service. \
             Disable it first, or deploy without --inactive"
        );
    }

    fn refused(result: Result<(), String>, path: &str, reason: &str) {
        let error = result.expect_err(reason);
        assert!(error.starts_with(path), "'{error}' does not name {path}");
        assert!(error.contains(reason), "'{error}' lacks '{reason}'");
    }

    #[test]
    fn schema_names() {
        for valid in ["loan_service", "_private", "a", "books2", &"s".repeat(63)] {
            assert_eq!(
                validate_schema_name("schema_name", valid),
                Ok(()),
                "{valid}"
            );
        }
        for (invalid, reason) in [
            ("", "is not a schema name"),
            ("Loan_Service", "is not a schema name"),
            ("loan-service", "is not a schema name"),
            ("1loans", "is not a schema name"),
            ("loans\"; DROP", "is not a schema name"),
            (&*"s".repeat(64), "is not a schema name"),
            ("public", "is reserved"),
            ("information_schema", "is reserved"),
            ("_cufflink", "is reserved"),
            ("_cufflink_outbox", "is reserved"),
            ("pg_catalog", "is reserved"),
            ("pg_anything", "is reserved"),
        ] {
            refused(
                validate_schema_name("schema_name", invalid),
                "schema_name",
                reason,
            );
        }
    }

    #[test]
    fn extensions() {
        let names = |names: &[&str]| {
            names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(validate_extensions("extensions", &[]), Ok(()));
        assert_eq!(
            validate_extensions("extensions", &names(&["google.token", "pdf.render-v2"])),
            Ok(())
        );
        assert_eq!(
            validate_extensions("extensions", &names(&["google.token", "Pdf"])),
            Err(
                "extensions[1]: 'Pdf' is not an extension name (segments of a-z, 0-9 and '-' \
                 joined by '.', at most 128 bytes)"
                    .into()
            )
        );
        assert_eq!(
            validate_extensions("extensions", &names(&["a.b", "c", "a.b"])),
            Err("extensions: extension 'a.b' is listed twice".into())
        );
        assert_eq!(
            refused_extension("extensions", &names(&["a.b", "a.b", "Bad"])),
            Some((1, "extensions: extension 'a.b' is listed twice".into())),
            "the first refused is named, where it is"
        );
        assert_eq!(
            refused_extension("extensions", &names(&["a.b", "Bad", "a.b"])).map(|(i, _)| i),
            Some(1)
        );
    }

    #[test]
    fn provided_extensions() {
        let provided = |name: &str, callers: &[&str]| ProvidedExtension {
            name: name.into(),
            callers: callers.iter().map(|c| c.to_string()).collect(),
        };
        assert_eq!(
            validate_provides(
                "provides",
                &[
                    provided("google.token", &["reports-service", "reports-service"]),
                    provided("pdf.render", &["*"]),
                ]
            ),
            Ok(())
        );
        for (provides, reason) in [
            (
                vec![],
                "provides: a native service provides at least one extension",
            ),
            (
                vec![provided("google.token", &["*"]), provided("Pdf", &["*"])],
                "provides[1]: 'Pdf' is not an extension name (segments of a-z, 0-9 and '-' \
                 joined by '.', at most 128 bytes)",
            ),
            (
                vec![provided("a.b", &["*"]), provided("a.b", &["x"])],
                "provides: extension 'a.b' is listed twice",
            ),
            (
                vec![provided("a.b", &["*"]), provided("c", &[])],
                "provides[1].callers: at least one caller is required",
            ),
            (
                vec![provided("a.b", &["reports", "a b"])],
                "provides[0].callers: 'a b' is neither a service name nor '*'",
            ),
        ] {
            assert_eq!(validate_provides("provides", &provides), Err(reason.into()));
        }
    }

    #[test]
    fn a_schema_is_reserved_however_it_is_capitalised() {
        for reserved in [
            "public",
            "Public",
            "PG_CATALOG",
            "pg_toast",
            "Information_Schema",
            "_cufflink",
            "_Cufflink_audit",
        ] {
            assert!(is_reserved_schema(reserved), "{reserved}");
        }
        for free in ["order", "user", "publics", "cufflink", "pg", "loan_service"] {
            assert!(!is_reserved_schema(free), "{free}");
        }
    }

    #[test]
    fn a_folded_identifier_names_what_the_unquoted_name_does() {
        for (name, quoted) in [
            ("loan_service", "\"loan_service\""),
            ("Order_Desk_2", "\"order_desk_2\""),
            ("order", "\"order\""),
            ("USER", "\"user\""),
            ("2fa", "\"2fa\""),
            ("a\"b", "\"a\"\"b\""),
        ] {
            assert_eq!(folded_ident(name), quoted);
        }
        assert_eq!(folded("Order_Desk_2"), "order_desk_2");
    }

    #[test]
    fn sql_fragments() {
        for valid in [
            "price_cents >= 0",
            "deleted_at IS NULL",
            "(a > 0) AND (b IN ('x', 'y'))",
            "lower(email) = email",
            "loan <> ')'",
            "label IN ('(', 'it''s (')",
            "\"odd)name\" > 0 AND \"say \"\"(\"\"\" IS NULL",
            "email ~ '^\\S+@\\S+$'",
            "code = E'\\\\n'",
        ] {
            assert_eq!(validate_sql_fragment("expr", valid), Ok(()), "{valid}");
        }
        for (invalid, reason) in [
            ("", "is empty"),
            ("   ", "is empty"),
            ("a > 0; DROP TABLE t", "contains ';'"),
            ("a > 0 -- always", "contains '--'"),
            ("a > 0 /* always */", "contains '/*'"),
            ("a > 0 */", "contains '*/'"),
            ("(a > 0", "unbalanced"),
            ("a > 0)", "unbalanced"),
            (")a > 0(", "unbalanced"),
            (
                "'(' = '(' ) , DROP COLUMN b CASCADE , ADD CONSTRAINT z CHECK ( ')' = ')'",
                "unbalanced",
            ),
            (
                "\"(\" = 1 ) , DROP COLUMN b , ADD CHECK ( \")\" = 1",
                "unbalanced",
            ),
            (
                "a = E'\\' ) , DROP COLUMN b , ADD CHECK ( '' = ''",
                "a backslash before a quote",
            ),
            ("a = E'\\'')'", "a backslash before a quote"),
            ("a = $$x$$", "contains '$'"),
            ("a = $1", "contains '$'"),
            ("a = 'open", "not closed"),
            ("\"open = 1", "not closed"),
        ] {
            refused(validate_sql_fragment("expr", invalid), "expr", reason);
        }
    }

    #[test]
    fn a_valid_consumer_spec_passes() {
        let spec = ConsumerSpec {
            durable_name: Some("loans-service-book-consumer".into()),
            ack_wait_secs: Some(30),
            max_deliver: Some(3),
            backoff_secs: vec![10, 60, 300],
            max_ack_pending: Some(1000),
            batch: Some(32),
            deliver_policy: None,
            filter_subjects: vec!["books.deleted.loans".into(), "books.*.set".into()],
            concurrency: Some(4),
            dead_letter: Some(true),
        };
        assert_eq!(validate_consumer_spec("c", &spec), Ok(()));
        assert_eq!(
            validate_consumer_spec("c", &ConsumerSpec::default()),
            Ok(())
        );
        let unlimited = ConsumerSpec {
            max_deliver: Some(-1),
            ..Default::default()
        };
        assert_eq!(validate_consumer_spec("c", &unlimited), Ok(()));
    }

    #[test]
    fn each_consumer_field_is_bounded() {
        let cases: Vec<(ConsumerSpec, &str)> = vec![
            (
                ConsumerSpec {
                    durable_name: Some("a.b".into()),
                    ..Default::default()
                },
                "c.durable_name",
            ),
            (
                ConsumerSpec {
                    durable_name: Some(String::new()),
                    ..Default::default()
                },
                "c.durable_name",
            ),
            (
                ConsumerSpec {
                    ack_wait_secs: Some(4),
                    ..Default::default()
                },
                "c.ack_wait_secs",
            ),
            (
                ConsumerSpec {
                    ack_wait_secs: Some(3601),
                    ..Default::default()
                },
                "c.ack_wait_secs",
            ),
            (
                ConsumerSpec {
                    max_deliver: Some(0),
                    ..Default::default()
                },
                "c.max_deliver",
            ),
            (
                ConsumerSpec {
                    max_deliver: Some(-2),
                    ..Default::default()
                },
                "c.max_deliver",
            ),
            (
                ConsumerSpec {
                    max_deliver: Some(101),
                    ..Default::default()
                },
                "c.max_deliver",
            ),
            (
                ConsumerSpec {
                    backoff_secs: vec![1; 11],
                    ..Default::default()
                },
                "c.backoff_secs",
            ),
            (
                ConsumerSpec {
                    backoff_secs: vec![5, 0],
                    ..Default::default()
                },
                "c.backoff_secs[1]",
            ),
            (
                ConsumerSpec {
                    backoff_secs: vec![86_401],
                    ..Default::default()
                },
                "c.backoff_secs[0]",
            ),
            (
                ConsumerSpec {
                    max_ack_pending: Some(0),
                    ..Default::default()
                },
                "c.max_ack_pending",
            ),
            (
                ConsumerSpec {
                    max_ack_pending: Some(100_001),
                    ..Default::default()
                },
                "c.max_ack_pending",
            ),
            (
                ConsumerSpec {
                    batch: Some(0),
                    ..Default::default()
                },
                "c.batch",
            ),
            (
                ConsumerSpec {
                    batch: Some(257),
                    ..Default::default()
                },
                "c.batch",
            ),
            (
                ConsumerSpec {
                    concurrency: Some(0),
                    ..Default::default()
                },
                "c.concurrency",
            ),
            (
                ConsumerSpec {
                    concurrency: Some(65),
                    ..Default::default()
                },
                "c.concurrency",
            ),
            (
                ConsumerSpec {
                    filter_subjects: vec!["books.ok".into(), "books..bad".into()],
                    ..Default::default()
                },
                "c.filter_subjects[1]",
            ),
            (
                ConsumerSpec {
                    filter_subjects: vec![format!("books.{}", "x".repeat(SUBJECT_MAX))],
                    ..Default::default()
                },
                "c.filter_subjects[0]",
            ),
            (
                ConsumerSpec {
                    filter_subjects: vec!["books.a".into(), "books.a".into()],
                    ..Default::default()
                },
                "c.filter_subjects",
            ),
        ];
        for (spec, path) in cases {
            refused(validate_consumer_spec("c", &spec), path, "");
        }
    }

    #[test]
    fn subscription_delivery_checks_the_subject_the_timeout_and_the_consumer() {
        let plain = || SubscriptionDefinition::new("x", "h", HandlerType::Wasm);
        assert_eq!(validate_subscription_delivery("s", &plain()), Ok(()));
        for subject in ["dw.{tenant}.books.*", "cufflink.tick.*", "dw.{tenant}.>"] {
            let sub = SubscriptionDefinition::new(subject, "h", HandlerType::Wasm);
            assert_eq!(
                validate_subscription_delivery("s", &sub),
                Ok(()),
                "{subject}"
            );
        }
        for (subject, reason) in [
            (String::new(), "'' is not a subject pattern".to_string()),
            (
                "books..created".into(),
                "'books..created' is not a subject pattern".into(),
            ),
            (
                "books.>.created".into(),
                "'books.>.created' is not a subject pattern".into(),
            ),
            (
                format!("dw.{{tenant}}.{}", "x".repeat(SUBJECT_MAX)),
                "a subject pattern of 1036 bytes is longer".into(),
            ),
        ] {
            refused(
                validate_subscription_delivery(
                    "s",
                    &SubscriptionDefinition::new(subject, "h", HandlerType::Wasm),
                ),
                "s.subject",
                &reason,
            );
        }
        for timeout in [1, 60, 300] {
            let sub = SubscriptionDefinition {
                timeout_secs: Some(timeout),
                ..plain()
            };
            assert_eq!(validate_subscription_delivery("s", &sub), Ok(()));
        }
        for timeout in [0, 301] {
            let sub = SubscriptionDefinition {
                timeout_secs: Some(timeout),
                ..plain()
            };
            refused(
                validate_subscription_delivery("s", &sub),
                "s.timeout_secs",
                "outside 1..=300",
            );
        }
        let sub = SubscriptionDefinition {
            consumer: Some(ConsumerSpec {
                batch: Some(0),
                ..Default::default()
            }),
            ..plain()
        };
        refused(
            validate_subscription_delivery("s", &sub),
            "s.consumer.batch",
            "outside",
        );
    }

    fn stream() -> StreamDefinition {
        StreamDefinition {
            name: "LOANS".into(),
            subjects: vec!["loans.created".into(), "loans.renewal.>".into()],
            retention: StreamRetention::Limits,
            max_age_secs: Some(2_592_000),
            max_msgs: Some(100_000),
            max_bytes: Some(268_435_456),
            storage: StreamStorage::File,
            duplicate_window_secs: Some(120),
            discard: DiscardSpec::Old,
            description: Some("loans".into()),
        }
    }

    #[test]
    fn streams() {
        assert_eq!(validate_stream_definition("st", &stream()), Ok(()));
        let cases: Vec<(StreamDefinition, &str, &str)> = vec![
            (
                StreamDefinition {
                    name: "loans".into(),
                    ..stream()
                },
                "st.name",
                "is not a stream name",
            ),
            (
                StreamDefinition {
                    subjects: vec![],
                    ..stream()
                },
                "st.subjects",
                "is empty",
            ),
            (
                StreamDefinition {
                    subjects: vec!["loans.>.x".into()],
                    ..stream()
                },
                "st.subjects[0]",
                "is not a subject pattern",
            ),
            (
                StreamDefinition {
                    subjects: vec![
                        "loans.created".into(),
                        format!("loans.{}", "x".repeat(SUBJECT_MAX)),
                    ],
                    ..stream()
                },
                "st.subjects[1]",
                "a subject pattern of 1030 bytes is longer than the 1024 bytes",
            ),
            (
                StreamDefinition {
                    subjects: vec!["a.b".into(), "a.b".into()],
                    ..stream()
                },
                "st.subjects",
                "listed twice",
            ),
            (
                StreamDefinition {
                    max_age_secs: Some(0),
                    ..stream()
                },
                "st.max_age_secs",
                "outside",
            ),
            (
                StreamDefinition {
                    max_msgs: Some(0),
                    ..stream()
                },
                "st.max_msgs",
                "not positive",
            ),
            (
                StreamDefinition {
                    max_bytes: Some(-1),
                    ..stream()
                },
                "st.max_bytes",
                "not positive",
            ),
            (
                StreamDefinition {
                    duplicate_window_secs: Some(0),
                    ..stream()
                },
                "st.duplicate_window_secs",
                "outside",
            ),
            (
                StreamDefinition {
                    max_age_secs: Some(60),
                    duplicate_window_secs: Some(120),
                    ..stream()
                },
                "st.duplicate_window_secs",
                "longer than max_age_secs",
            ),
            (
                StreamDefinition {
                    description: Some("d".repeat(4097)),
                    ..stream()
                },
                "st.description",
                "longer than",
            ),
        ];
        for (invalid, path, reason) in cases {
            refused(validate_stream_definition("st", &invalid), path, reason);
        }
        refused(
            validate_streams("streams", &[stream(), stream()]),
            "streams",
            "stream 'LOANS' is listed twice",
        );
        refused(
            validate_streams(
                "streams",
                &[
                    stream(),
                    StreamDefinition {
                        name: "bad".into(),
                        ..stream()
                    },
                ],
            ),
            "streams[1].name",
            "is not a stream name",
        );
    }

    fn schedule(spec: ScheduleSpec) -> ScheduleDefinition {
        ScheduleDefinition {
            name: "order_retry".into(),
            handler: "retry_orders".into(),
            spec,
            timezone: None,
            overlap: Overlap::Skip,
            catch_up: CatchUp::Latest,
            job: None,
            timeout_secs: None,
        }
    }

    fn cron(expr: &str) -> ScheduleSpec {
        ScheduleSpec::Cron { expr: expr.into() }
    }

    #[test]
    fn valid_schedules_pass() {
        for valid in [
            schedule(ScheduleSpec::Every { secs: 10 }),
            schedule(cron("0 * * * *")),
            schedule(cron("*/30 0 * * * *")),
            ScheduleDefinition {
                timezone: Some("Europe/London".into()),
                timeout_secs: Some(300),
                ..schedule(cron("0 9 * * 1-5"))
            },
            ScheduleDefinition {
                timezone: Some("Etc/GMT+5".into()),
                ..schedule(cron("0 9 * * *"))
            },
            ScheduleDefinition {
                job: Some(JobConfig {
                    retry_on: vec![RetryOn::Error],
                    backoff: Some(BackoffSpec::default()),
                    ..Default::default()
                }),
                ..schedule(ScheduleSpec::Every { secs: 300 })
            },
        ] {
            assert_eq!(validate_schedule("sc", &valid), Ok(()), "{valid:?}");
        }
    }

    #[test]
    fn invalid_schedules_name_their_field() {
        let cases: Vec<(ScheduleDefinition, &str, &str)> = vec![
            (
                ScheduleDefinition {
                    name: "Order-Retry".into(),
                    ..schedule(ScheduleSpec::Every { secs: 30 })
                },
                "sc.name",
                "is not a schedule name",
            ),
            (
                ScheduleDefinition {
                    handler: "retry orders".into(),
                    ..schedule(ScheduleSpec::Every { secs: 30 })
                },
                "sc.handler",
                "is not an identifier",
            ),
            (
                schedule(ScheduleSpec::Every { secs: 9 }),
                "sc.spec.secs",
                "below the 10 second minimum",
            ),
            (
                ScheduleDefinition {
                    timezone: Some("Europe/London".into()),
                    ..schedule(ScheduleSpec::Every { secs: 30 })
                },
                "sc.timezone",
                "applies to a cron schedule",
            ),
            (schedule(cron("")), "sc.spec.expr", "five fields"),
            (schedule(cron("* * * *")), "sc.spec.expr", "five fields"),
            (
                schedule(cron("* * * * * * *")),
                "sc.spec.expr",
                "five fields",
            ),
            (
                ScheduleDefinition {
                    timezone: Some("Europe//London".into()),
                    ..schedule(cron("0 * * * *"))
                },
                "sc.timezone",
                "is not an IANA time zone name",
            ),
            (
                ScheduleDefinition {
                    timezone: Some("Europe/Lon don".into()),
                    ..schedule(cron("0 * * * *"))
                },
                "sc.timezone",
                "is not an IANA time zone name",
            ),
            (
                ScheduleDefinition {
                    timeout_secs: Some(0),
                    ..schedule(cron("0 * * * *"))
                },
                "sc.timeout_secs",
                "outside 1..=300",
            ),
            (
                ScheduleDefinition {
                    timeout_secs: Some(301),
                    ..schedule(cron("0 * * * *"))
                },
                "sc.timeout_secs",
                "outside 1..=300",
            ),
            (
                ScheduleDefinition {
                    timeout_secs: Some(30),
                    job: Some(JobConfig::default()),
                    ..schedule(cron("0 * * * *"))
                },
                "sc.timeout_secs",
                "applies to an inline handler",
            ),
            (
                ScheduleDefinition {
                    job: Some(JobConfig {
                        retry_on: vec![RetryOn::Error, RetryOn::Error],
                        ..Default::default()
                    }),
                    ..schedule(cron("0 * * * *"))
                },
                "sc.job.retry_on",
                "listed twice",
            ),
        ];
        for (invalid, path, reason) in cases {
            refused(validate_schedule("sc", &invalid), path, reason);
        }
        let twice = [
            schedule(ScheduleSpec::Every { secs: 30 }),
            schedule(cron("0 * * * *")),
        ];
        refused(
            validate_schedules("schedules", &twice),
            "schedules",
            "schedule 'order_retry' is listed twice",
        );
    }

    #[test]
    fn job_backoff_is_bounded_and_ordered() {
        let job = |base_secs, max_secs| JobConfig {
            backoff: Some(BackoffSpec {
                base_secs,
                max_secs,
            }),
            ..Default::default()
        };
        assert_eq!(validate_job_config("job", &JobConfig::default()), Ok(()));
        assert_eq!(validate_job_config("job", &job(30, 3600)), Ok(()));
        assert_eq!(validate_job_config("job", &job(60, 60)), Ok(()));
        refused(
            validate_job_config("job", &job(0, 60)),
            "job.backoff.base_secs",
            "outside",
        );
        refused(
            validate_job_config("job", &job(1, 86_401)),
            "job.backoff.max_secs",
            "outside",
        );
        refused(
            validate_job_config("job", &job(120, 60)),
            "job.backoff.max_secs",
            "below base_secs",
        );
    }

    fn migration(version: u32, name: &str, sql: &str) -> SqlMigration {
        SqlMigration {
            version,
            name: name.into(),
            sql: sql.into(),
            no_transaction: SqlMigration::declares_no_transaction(sql),
        }
    }

    #[test]
    fn sql_migrations() {
        let valid = [
            migration(1, "initial_tables", "CREATE TABLE loans ();"),
            migration(6, "add_loan_kind", "ALTER TABLE loans ADD kind TEXT;"),
            migration(
                7,
                "renewal_index",
                "-- cufflink:no-transaction\nCREATE INDEX CONCURRENTLY i ON loans (kind);",
            ),
        ];
        assert_eq!(validate_sql_migrations("migrations", &valid, None), Ok(()));
        assert_eq!(validate_sql_migrations("migrations", &[], None), Ok(()));
        assert_eq!(
            validate_sql_migrations("migrations", &[migration(0, "zero", "SELECT 1;")], None),
            Ok(())
        );

        let cases: Vec<(Vec<SqlMigration>, &str, &str)> = vec![
            (
                vec![
                    migration(1, "a", "SELECT 1;"),
                    migration(1, "b", "SELECT 1;"),
                ],
                "migrations",
                "version 1 follows version 1",
            ),
            (
                vec![
                    migration(6, "a", "SELECT 1;"),
                    migration(2, "b", "SELECT 1;"),
                ],
                "migrations",
                "version 2 follows version 6",
            ),
            (
                vec![migration(1, "add-kind", "SELECT 1;")],
                "migrations[0].name",
                "is not a migration name",
            ),
            (
                vec![migration(1, "", "SELECT 1;")],
                "migrations[0].name",
                "is not a migration name",
            ),
            (
                vec![migration(1, "a", "  \n")],
                "migrations[0].sql",
                "is empty",
            ),
            (
                vec![migration(2_147_483_648, "a", "SELECT 1;")],
                "migrations[0].version",
                "is above",
            ),
            (
                vec![SqlMigration {
                    no_transaction: true,
                    ..migration(1, "a", "SELECT 1;")
                }],
                "migrations[0].no_transaction",
                "disagrees",
            ),
            (
                vec![SqlMigration {
                    no_transaction: false,
                    ..migration(1, "a", "-- cufflink:no-transaction\nSELECT 1;")
                }],
                "migrations[0].no_transaction",
                "disagrees",
            ),
        ];
        for (invalid, path, reason) in cases {
            refused(
                validate_sql_migrations("migrations", &invalid, None),
                path,
                reason,
            );
        }
    }

    #[test]
    fn migration_history() {
        let history = |table: &str, baseline| MigrationHistory {
            table: table.into(),
            baseline,
            ..Default::default()
        };
        for valid in [
            history("schema_history", None),
            history("public.schema_history_deferred", Some(15)),
            history("refinery_schema_history", Some(0)),
        ] {
            assert_eq!(validate_migration_history("h", &valid), Ok(()));
            assert_eq!(
                validate_sql_migrations("migrations", &[], Some(&valid)),
                Ok(())
            );
        }
        for invalid in [
            "",
            "a.b.c",
            "schema.",
            ".table",
            "sche ma.t",
            "t\"; DROP",
            "Schema_History",
            "Public.schema_history",
        ] {
            refused(
                validate_migration_history("h", &history(invalid, None)),
                "h.table",
                "is not 'table' or 'schema.table'",
            );
        }
        refused(
            validate_migration_history("h", &history("t", Some(2_147_483_648))),
            "h.baseline",
            "outside",
        );
        refused(
            validate_sql_migrations("migrations", &[], Some(&history("a.b.c", None))),
            "migration_history.table",
            "is not",
        );
    }

    #[test]
    fn column_types() {
        let numeric = |precision, scale| ColumnType::Numeric { precision, scale };
        for valid in [
            ColumnType::Text,
            ColumnType::Bytea,
            ColumnType::JsonbArray,
            numeric(None, None),
            numeric(Some(1), None),
            numeric(Some(12), Some(2)),
            numeric(Some(12), Some(12)),
            numeric(Some(1000), Some(0)),
            ColumnType::Varchar { length: 1 },
            ColumnType::Varchar { length: 10_485_760 },
        ] {
            assert_eq!(validate_column_type("c", &valid), Ok(()), "{valid:?}");
        }
        for (invalid, path, reason) in [
            (numeric(None, Some(2)), "c", "needs a precision"),
            (numeric(Some(0), None), "c.precision", "outside"),
            (numeric(Some(1001), None), "c.precision", "outside"),
            (numeric(Some(4), Some(5)), "c.scale", "outside 0..=4"),
            (ColumnType::Varchar { length: 0 }, "c.length", "outside"),
            (
                ColumnType::Varchar { length: 10_485_761 },
                "c.length",
                "outside",
            ),
        ] {
            refused(validate_column_type("c", &invalid), path, reason);
        }
    }

    #[test]
    fn check_constraints() {
        let check = |name: &str, expr: &str| CheckConstraint {
            name: name.into(),
            expr: expr.into(),
        };
        assert_eq!(
            validate_check_constraint("ck", &check("price_nonneg", "price_cents >= 0")),
            Ok(())
        );
        refused(
            validate_check_constraint("ck", &check("price-nonneg", "price_cents >= 0")),
            "ck.name",
            "is not an identifier",
        );
        refused(
            validate_check_constraint("ck", &check(&"n".repeat(64), "a > 0")),
            "ck.name",
            "is not an identifier",
        );
        refused(
            validate_check_constraint("ck", &check("c", "a > 0; DROP TABLE t")),
            "ck.expr",
            "contains ';'",
        );
        refused(
            validate_check_constraints("checks", &[check("c", "a > 0"), check("c", "b > 0")]),
            "checks",
            "check 'c' is listed twice",
        );
        refused(
            validate_check_constraints("checks", &[check("c", "a > 0"), check("d", "(b > 0")]),
            "checks[1].expr",
            "unbalanced",
        );
    }

    fn index(columns: &[&str]) -> IndexDefinition {
        IndexDefinition {
            name: "uq_org_email".into(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn indexes() {
        for valid in [
            index(&["email"]),
            IndexDefinition {
                unique: true,
                predicate: Some("deleted_at IS NULL".into()),
                ..index(&["branch_id", "email"])
            },
            IndexDefinition {
                method: IndexMethod::Gin,
                ..index(&["services"])
            },
            IndexDefinition {
                method: IndexMethod::Brin,
                ..index(&["created_at", "id"])
            },
        ] {
            assert_eq!(validate_index("ix", &valid), Ok(()), "{valid:?}");
        }
        let cases: Vec<(IndexDefinition, &str, &str)> = vec![
            (
                IndexDefinition {
                    name: "uq org".into(),
                    ..index(&["a"])
                },
                "ix.name",
                "is not an identifier",
            ),
            (index(&[]), "ix.columns", "is empty"),
            (
                index(&["a", "b\""]),
                "ix.columns[1]",
                "is not an identifier",
            ),
            (index(&["a", "a"]), "ix.columns", "listed twice"),
            (
                IndexDefinition {
                    unique: true,
                    method: IndexMethod::Gin,
                    ..index(&["a"])
                },
                "ix.method",
                "a unique index is a btree index, not gin",
            ),
            (
                IndexDefinition {
                    method: IndexMethod::Hash,
                    ..index(&["a", "b"])
                },
                "ix.method",
                "a hash index has one column",
            ),
            (
                IndexDefinition {
                    predicate: Some("a IS NULL; DROP TABLE t".into()),
                    ..index(&["a"])
                },
                "ix.predicate",
                "contains ';'",
            ),
        ];
        for (invalid, path, reason) in cases {
            refused(validate_index("ix", &invalid), path, reason);
        }
    }

    #[test]
    fn search_specs() {
        let search = |columns: &[&str], config: &str, vector: Option<&str>| SearchSpec {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            config: config.into(),
            vector_column: vector.map(str::to_string),
        };
        for valid in [
            search(&["name", "email"], "simple", None),
            search(&[], "english", Some("search_vector")),
            search(&["name"], "simple", Some("search_vector")),
        ] {
            assert_eq!(validate_search_spec("se", &valid), Ok(()), "{valid:?}");
        }
        for (invalid, path, reason) in [
            (search(&[], "simple", None), "se", "names neither"),
            (
                search(&["na me"], "simple", None),
                "se.columns[0]",
                "is not an identifier",
            ),
            (
                search(&["name", "name"], "simple", None),
                "se.columns",
                "listed twice",
            ),
            (
                search(&["name"], "English'", None),
                "se.config",
                "is not a text search configuration name",
            ),
            (
                search(&["name"], "simple", Some("v;")),
                "se.vector_column",
                "is not an identifier",
            ),
        ] {
            refused(validate_search_spec("se", &invalid), path, reason);
        }
    }

    #[test]
    fn audit_redaction_rules() {
        let rules =
            |rules: &[&str]| -> Vec<String> { rules.iter().map(|r| r.to_string()).collect() };
        assert_eq!(
            validate_audit_redact(
                "r",
                &rules(&[
                    "dob",
                    "bank_*",
                    "_x*",
                    "profile.dob",
                    "contacts[].email",
                    "meta.card_*",
                    "tags[]",
                    "orders[].lines[].sku*"
                ])
            ),
            Ok(())
        );
        assert_eq!(validate_audit_redact("r", &[]), Ok(()));
        for (invalid, path, reason) in [
            (
                rules(&["*"]),
                "r[0]",
                "a rule is a column name or 'prefix*'",
            ),
            (rules(&["dob", "ba*nk"]), "r[1]", "is not an identifier"),
            (rules(&["bank_**"]), "r[0]", "is not an identifier"),
            (rules(&["dob", "dob"]), "r", "listed twice"),
            (rules(&["profile."]), "r[0]", "is not an identifier"),
            (rules(&[".dob"]), "r[0]", "is not an identifier"),
            (rules(&["a..b"]), "r[0]", "is not an identifier"),
            (rules(&["contacts[]email"]), "r[0]", "is not an identifier"),
            (
                rules(&["contacts[0].email"]),
                "r[0]",
                "is not an identifier",
            ),
            (rules(&["tags[][]"]), "r[0]", "is not an identifier"),
            (rules(&["[]"]), "r[0]", "is not an identifier"),
        ] {
            refused(validate_audit_redact("r", &invalid), path, reason);
        }
    }

    #[test]
    fn served_only_declarations_names_each_declaration() {
        use crate::{ColumnDefinition, Pagination, TableHooks, ValidationRule};
        let table = || TableDefinition {
            name: "loans".into(),
            columns: vec![ColumnDefinition {
                name: "owner".into(),
                column_type: ColumnType::Text,
                ..Default::default()
            }],
            managed: false,
            soft_delete: true,
            ..Default::default()
        };
        let named = |edit: fn(&mut TableDefinition)| {
            let mut table = table();
            edit(&mut table);
            served_only_declarations(&table).collect::<Vec<_>>()
        };
        assert!(named(|_| ()).is_empty());
        type Declare = fn(&mut TableDefinition);
        let cases: [(&str, Declare); 11] = [
            ("hooks", |t| t.hooks = Some(TableHooks::default())),
            ("auth_required", |t| t.auth_required = true),
            ("permission_area", |t| {
                t.permission_area = Some("loans".into())
            }),
            ("owner_field", |t| t.owner_field = Some("owner".into())),
            ("owner_self_service", |t| t.owner_self_service = true),
            ("search", |t| t.search = Some(SearchSpec::default())),
            ("pagination", |t| t.pagination = Pagination::Keyset),
            ("sensitive", |t| t.sensitive = true),
            ("audit_redact", |t| t.audit_redact = vec!["owner".into()]),
            ("validations", |t| {
                t.columns[0].validations = vec![ValidationRule::MinLength { value: 1 }]
            }),
            ("column permissions", |t| {
                t.columns[0].access = Some(ColumnAccess {
                    read: Some("internal".into()),
                    write: None,
                })
            }),
        ];
        for (name, edit) in cases {
            assert_eq!(named(edit), [name]);
            let mut refused = table();
            edit(&mut refused);
            assert_eq!(
                validate_private_table(&refused),
                Err(format!(
                    "Private table 'loans' declares {name}, which only data routes read"
                ))
            );
        }
        assert!(
            named(|t| {
                t.scoped_by = Some("loans".into());
                t.owner_field = Some("owner".into());
                t.permission_area = Some("loans".into());
            })
            .is_empty(),
            "a scope's owner column, and a permission_area equal to scoped_by"
        );
        assert_eq!(
            named(|t| {
                t.scoped_by = Some("loans".into());
                t.owner_field = Some("owner".into());
                t.permission_area = Some("other".into());
            }),
            ["permission_area"]
        );
        assert!(named(|t| t.pagination = Pagination::Offset).is_empty());
        assert_eq!(
            named(|t| {
                t.sensitive = true;
                t.hooks = Some(TableHooks::default());
            }),
            ["hooks", "sensitive"]
        );
        assert_eq!(validate_private_table(&table()), Ok(()));
    }

    fn listings() -> TableDefinition {
        serde_json::from_value(serde_json::json!({
            "name": "listings",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "auto_generate": true},
                {"name": "customer_id", "column_type": "uuid"},
                {"name": "title", "column_type": "text"},
                {"name": "ops_loans", "column_type": "text", "nullable": true},
                {"name": "status", "column_type": "text", "default_value": "draft"},
                {"name": "created_at", "column_type": "timestamp"},
                {"name": "updated_at", "column_type": "timestamp", "auto_generate": true}
            ],
            "owner_field": "customer_id",
            "permission_area": "listings"
        }))
        .expect("table")
    }

    fn restricted(column: &str, read: Option<&str>, write: Option<&str>) -> TableDefinition {
        let mut table = listings();
        let named = table
            .columns
            .iter_mut()
            .find(|c| c.name == column)
            .expect("column");
        named.access = Some(ColumnAccess {
            read: read.map(str::to_string),
            write: write.map(str::to_string),
        });
        table
    }

    #[test]
    fn a_column_permission_names_an_operation_to_read_to_write_or_both() {
        assert_eq!(
            validate_column_access("tables.listings", &listings()),
            Ok(())
        );
        for (column, read, write) in [
            ("ops_loans", Some("internal"), Some("internal")),
            ("ops_loans", Some("internal"), None),
            ("status", None, Some("manage")),
            ("status", Some("internal"), Some("manage")),
            ("title", Some("internal"), None),
        ] {
            assert_eq!(
                validate_column_access("tables.listings", &restricted(column, read, write)),
                Ok(()),
                "{column}"
            );
        }
        let scoped = TableDefinition {
            permission_area: None,
            scoped_by: Some("listings".into()),
            ..restricted("ops_loans", Some("internal"), None)
        };
        assert_eq!(validate_column_access("tables.listings", &scoped), Ok(()));
    }

    #[test]
    fn a_column_permission_is_refused_where_the_data_routes_depend_on_the_column() {
        let internal = Some("internal");
        let unareaed = TableDefinition {
            permission_area: None,
            ..restricted("ops_loans", internal, None)
        };
        let searched = TableDefinition {
            search: Some(SearchSpec {
                columns: vec!["title".into(), "ops_loans".into()],
                ..Default::default()
            }),
            ..restricted("ops_loans", internal, None)
        };
        for (table, path, reason) in [
            (
                restricted("ops_loans", None, None),
                "tables.listings.columns.ops_loans.access",
                "names neither read nor write",
            ),
            (
                restricted("ops_loans", Some("in ternal"), None),
                "tables.listings.columns.ops_loans.access.read",
                "is not an identifier",
            ),
            (
                restricted("ops_loans", None, Some("*")),
                "tables.listings.columns.ops_loans.access.write",
                "is not an identifier",
            ),
            (
                unareaed,
                "tables.listings.columns.ops_loans.access",
                "the table declares none",
            ),
            (
                restricted("id", internal, None),
                "tables.listings.columns.id.access",
                "the primary key is how every caller addresses a row",
            ),
            (
                restricted("customer_id", None, internal),
                "tables.listings.columns.customer_id.access",
                "the owner column is what the table's rows are scoped by",
            ),
            (
                restricted("updated_at", internal, None),
                "tables.listings.columns.updated_at.access",
                "the platform sets the column itself",
            ),
            (
                restricted("created_at", internal, None),
                "tables.listings.columns.created_at.access.read",
                "listings are ordered by 'created_at'",
            ),
            (
                restricted("title", None, internal),
                "tables.listings.columns.title.access.write",
                "a caller without the grant could create no row",
            ),
            (
                searched,
                "tables.listings.search.columns",
                "'ops_loans' is read only with a grant",
            ),
            (
                TableDefinition {
                    search: Some(SearchSpec {
                        vector_column: Some("tsv".into()),
                        ..Default::default()
                    }),
                    ..restricted("ops_loans", internal, None)
                },
                "tables.listings.search.vector_column",
                "which may hold 'ops_loans'",
            ),
            (
                {
                    let mut unique = restricted("ops_loans", internal, None);
                    unique.columns[3].unique = true;
                    unique
                },
                "tables.listings.columns.ops_loans.access",
                "restrict write as well",
            ),
            (
                TableDefinition {
                    indexes: vec![IndexDefinition {
                        name: "uq".into(),
                        columns: vec!["title".into(), "ops_loans".into()],
                        unique: true,
                        ..Default::default()
                    }],
                    ..restricted("ops_loans", internal, None)
                },
                "tables.listings.columns.ops_loans.access",
                "restrict write as well",
            ),
        ] {
            refused(
                validate_column_access("tables.listings", &table),
                path,
                reason,
            );
        }
        let searched_elsewhere = TableDefinition {
            search: Some(SearchSpec {
                columns: vec!["title".into(), "status".into()],
                ..Default::default()
            }),
            ..restricted("status", None, Some("manage"))
        };
        assert_eq!(
            validate_column_access("tables.listings", &searched_elsewhere),
            Ok(()),
            "a column every caller reads is searched whoever may write it"
        );
        let mut unique = restricted("ops_loans", internal, internal);
        unique.columns[3].unique = true;
        assert_eq!(
            validate_column_access("tables.listings", &unique),
            Ok(()),
            "a unique column nobody without the grant writes tells them nothing"
        );
    }
}
