//! Alone in its test binary, so that the process's descriptors are this
//! test's to count.

use cufflink_platform::sql_parser::{SqlParseSettings, SqlParser};
use cufflink_testkit::infra;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

fn open_descriptors() -> usize {
    std::fs::read_dir("/dev/fd").expect("descriptors").count()
}

fn stderr() -> Option<String> {
    std::fs::metadata("/dev/fd/2")
        .ok()
        .map(|m| format!("{m:?}"))
}

#[tokio::test]
async fn parses_leave_no_descriptor_behind_and_the_platforms_stderr_alone() {
    let Some(binary) = infra::require_binary("cufflink-sql-parser") else {
        return;
    };
    let parser = Arc::new(SqlParser::new(SqlParseSettings {
        binary,
        processes: 4,
        per_tenant: 4,
        memory_mb: 1024,
        timeout: Duration::from_millis(300),
    }));
    assert!(parser.start().await);
    let round = || async {
        let calls: Vec<_> = (0..32)
            .map(|n| {
                let parser = Arc::clone(&parser);
                tokio::spawn(async move {
                    parser
                        .parse(
                            Uuid::nil(),
                            &format!("SELECT {n} FROM orders"),
                            usize::MAX,
                            Duration::from_secs(20),
                            |_| Ok::<_, ()>(Vec::new()),
                        )
                        .await
                        .is_ok()
                })
            })
            .collect();
        for call in calls {
            assert!(call.await.expect("joined"));
        }
    };
    let before = stderr();
    round().await;
    let settled = open_descriptors();
    for _ in 0..3 {
        round().await;
    }
    let slow = format!("SELECT 1 WHERE {}", vec!["x = 1"; 50_000].join(" AND "));
    for _ in 0..4 {
        let refused = parser
            .parse(
                Uuid::nil(),
                &slow,
                usize::MAX,
                Duration::from_secs(20),
                |_| Ok::<_, ()>(Vec::new()),
            )
            .await;
        assert!(refused.is_err());
    }
    round().await;
    let started = std::time::Instant::now();
    while open_descriptors() != settled {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "killed processes and finished parses leave no descriptor open: {} then {settled}",
            open_descriptors()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(stderr(), before, "the platform's stderr is its own");
}
