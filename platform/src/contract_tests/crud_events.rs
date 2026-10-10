use super::*;
use crate::auth::scope::{EffectiveScope, RowPredicate};
use crate::outbox::emit::{Change, ChangeKind, Target};
use serde_json::Value;

const GOLDEN: &str = include_str!("../../tests/fixtures/crud_events/v0_27_0.json");
const TENANT: &str = "default";

fn concrete(shaped: &str) -> String {
    let uuid = regex::Regex::new(r"\{uuid:(\d+)\}").expect("pattern");
    let id = |n: &str| uuid::Uuid::from_u128(n.parse().expect("a number")).to_string();
    uuid.replace_all(shaped, |found: &regex::Captures| id(&found[1]))
        .replace("{the id in capitals}", &id("4").to_uppercase())
        .replace("{time}", "2026-09-28T12:00:00.123456")
        .replace("{tenant}", TENANT)
}

fn published() -> Vec<(String, String)> {
    let golden: Value = serde_json::from_str(GOLDEN).expect("the golden file");
    golden["single_writes"]
        .as_array()
        .expect("steps")
        .iter()
        .flat_map(|step| step["messages"].as_array().cloned().unwrap_or_default())
        .map(|message| {
            let text = |key: &str| concrete(message[key].as_str().expect("text"));
            (text("subject"), text("payload"))
        })
        .collect()
}

fn text(of: &Value, key: &str) -> String {
    of[key].as_str().unwrap_or_default().to_string()
}

fn kind(named: &str) -> ChangeKind {
    [
        ChangeKind::Created,
        ChangeKind::Updated,
        ChangeKind::Deleted,
    ]
    .into_iter()
    .find(|kind| kind.event() == named || kind.action() == named)
    .unwrap_or_else(|| panic!("{named} is no change"))
}

fn caller(record: &Value) -> Option<AuthUser> {
    let t = tenants();
    let subject = text(record, "user_id");
    (subject != crate::audit::ANONYMOUS).then(|| AuthUser {
        name: Some(text(record, "user_name")).filter(|name| !name.is_empty()),
        ..jwt(&entry(&t.default, &[]), "orchard", &subject, "portal")
    })
}

fn scope(record: &Value) -> Option<EffectiveScope> {
    let owner = RowPredicate::Owner {
        column: "user_id".into(),
        subject: text(record, "user_id"),
    };
    let claim = RowPredicate::Claim {
        column: "org_no".into(),
        values: vec![json!(5)],
    };
    record.get("scope").map(|scope| match scope.as_str() {
        Some("all") => EffectiveScope::All,
        Some("own") => EffectiveScope::Rows(vec![owner]),
        Some("field") => EffectiveScope::Rows(vec![claim]),
        other => panic!("{other:?} is no scope"),
    })
}

#[test]
fn the_recording_holds_what_the_contract_names() {
    let published = published();
    assert_eq!(published.len(), 38);
    let (audits, events): (Vec<_>, Vec<_>) = published
        .iter()
        .partition(|(subject, _)| subject == "dw.default.logs");
    assert_eq!((audits.len(), events.len()), (19, 19));
    for (_, payload) in &audits {
        let record: Value = serde_json::from_str(payload).expect("json");
        let mut keys: Vec<&str> = record
            .as_object()
            .expect("an object")
            .keys()
            .map(String::as_str)
            .filter(|key| *key != "scope")
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "action",
                "data",
                "previous",
                "record_id",
                "service",
                "table",
                "tenant",
                "timestamp",
                "user_id",
                "user_name"
            ]
        );
    }
}

#[test]
fn an_event_is_byte_for_byte_what_v0_27_0_published() {
    let nobody = None;
    for (subject, payload) in published()
        .into_iter()
        .filter(|(subject, _)| subject != "dw.default.logs")
    {
        let event: Value = serde_json::from_str(&payload).expect("json");
        let (service, table) = (text(&event, "service"), text(&event, "table"));
        let target = Target::of(
            TENANT,
            text(&event, "tenant_id").parse().expect("a tenant id"),
            &service,
            &table,
            &nobody,
        );
        let change = Change {
            kind: kind(&text(&event, "action")),
            row: &event["data"],
            record_id: "",
            scope: None,
            before: None,
        };
        let emission = target.emission(&change, chrono::Utc::now());
        assert_eq!(emission.event.subject, subject);
        assert_eq!(
            String::from_utf8(emission.event.payload).expect("utf-8"),
            payload
        );
    }
}

#[test]
fn an_audit_record_is_byte_for_byte_what_v0_27_0_published() {
    for (subject, payload) in published()
        .into_iter()
        .filter(|(subject, _)| subject == "dw.default.logs")
    {
        let record: Value = serde_json::from_str(&payload).expect("json");
        let (service, table) = (text(&record, "service"), text(&record, "table"));
        let caller = caller(&record);
        let scope = scope(&record);
        let kind = kind(&text(&record, "action"));
        let target = Target::of(TENANT, uuid::Uuid::nil(), &service, &table, &caller);
        let change = Change {
            kind,
            row: match kind {
                ChangeKind::Deleted => &record["previous"],
                _ => &record["data"],
            },
            record_id: record["record_id"].as_str().expect("a record id"),
            scope: scope.as_ref(),
            before: None,
        };
        let at = text(&record, "timestamp").parse().expect("a time");
        let emission = target.emission(&change, at);
        assert_eq!(emission.audit.subject, subject);
        assert_eq!(
            String::from_utf8(emission.audit.payload).expect("utf-8"),
            payload
        );
    }
}

#[test]
fn a_key_minted_for_a_user_names_the_owner_as_v0_27_0_did_and_the_user_durably() {
    let (subject, payload) = published()
        .into_iter()
        .rfind(|(subject, _)| subject == "dw.default.logs")
        .expect("the record of the minted key's write");
    let record: Value = serde_json::from_str(&payload).expect("json");
    assert_eq!(text(&record, "user_name"), "cli-devi");
    let caller = caller(&record).map(|user| AuthUser {
        auth_method: crate::auth::extractor::AuthMethod::ApiKey {
            id: uuid::Uuid::from_u128(7),
            user_sub: Some("devi".into()),
        },
        token_info: None,
        ..user
    });
    let target = Target::of(TENANT, uuid::Uuid::nil(), "shop", "orders", &caller);
    let change = Change {
        kind: ChangeKind::Created,
        row: &record["data"],
        record_id: record["record_id"].as_str().expect("a record id"),
        scope: None,
        before: None,
    };
    let emission = target.emission(&change, text(&record, "timestamp").parse().expect("a time"));
    assert_eq!(emission.audit.subject, subject);
    assert_eq!(
        String::from_utf8(emission.audit.payload).expect("utf-8"),
        payload
    );
    let durable: Value = serde_json::from_slice(&emission.durable.payload).expect("json");
    assert_eq!(durable["user_id"], "devi");
    assert_eq!(
        durable["actor"],
        json!({"sub": "devi", "kind": "api_key",
               "key_id": "00000000-0000-0000-0000-000000000007", "azp": null})
    );
}

#[test]
fn a_row_without_a_text_id_is_recorded_under_no_id() {
    let nobody = None;
    let target = Target::of(TENANT, uuid::Uuid::nil(), "shop", "counters", &nobody);
    let at = "2026-09-28T12:00:00Z".parse().expect("a time");
    for row in [json!({"id": 7, "n": 1}), json!({"n": 1})] {
        let change =
            crate::router::crud::created_row(&row, crate::router::crud::made_key(&row), None);
        let record = target.audit(&change, at);
        assert_eq!(record.record_id.as_deref(), Some(""), "{row}");
        assert_eq!(
            record.legacy_payload().to_string(),
            format!(
                "{{\"action\":\"create\",\"data\":{row},\"previous\":null,\"record_id\":\"\",\
                 \"service\":\"shop\",\"table\":\"counters\",\"tenant\":\"default\",\
                 \"timestamp\":\"2026-09-28T12:00:00+00:00\",\"user_id\":\"anonymous\",\
                 \"user_name\":\"\"}}"
            )
        );
    }
}
