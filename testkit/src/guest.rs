const STRINGS: &[(&str, &str)] = &[
    ("PREFIX", r#"{\"__status\":200,\"__body\":"#),
    ("RC_MINUS_ONE", r#"{\"rc\":-1}"#),
    ("RC_ZERO", r#"{\"rc\":0}"#),
    ("SLEEP_600", "SELECT pg_sleep(600)"),
    ("SLEEP_2", "SELECT pg_sleep(2)"),
    ("OK", r#"{\"ok\":true}"#),
    ("SELECT_ONE", "SELECT 1"),
    (
        "BIG_QUERY",
        "SELECT repeat('x',1000) FROM generate_series(1,200000)",
    ),
    ("SAID", SAID),
    (
        "INSERT_SLOW",
        "INSERT INTO items (title) SELECT 'hook' FROM pg_sleep(2)",
    ),
    ("CLAIMED_SUBJECT", "itclaimed.books.created"),
    ("UNCLAIMED_SUBJECT", "itunclaimed.books.created"),
    ("INSERT_ORDERED_HEAD", "INSERT INTO items (title) SELECT '"),
    ("INSERT_ORDERED_MID", "' FROM pg_sleep(1.0 / "),
    ("INSERT_ORDERED_TAIL", ")"),
    (
        "HOOK_ERROR",
        r#"{\"error\":\"db_query failed inside the hook\"}"#,
    ),
    (
        "UPSTREAM",
        r#"{\"__status\":503,\"__body\":{\"error\":\"upstream\"}}"#,
    ),
];

pub const SAID: &str = "said by the guest";

fn unescaped_len(s: &str) -> usize {
    s.len() - s.matches('\\').count()
}

pub fn module_wat() -> String {
    let mut data = String::new();
    let mut template = TEMPLATE.to_string();
    let mut offset = 0usize;
    for (name, text) in STRINGS {
        data.push_str(&format!("  (data (i32.const {offset}) \"{text}\")\n"));
        template = template
            .replace(&format!("${name}_OFF"), &offset.to_string())
            .replace(&format!("${name}_LEN"), &unescaped_len(text).to_string());
        offset += 64;
    }
    template.replace("$DATA", &data)
}

pub fn module_bytes() -> Vec<u8> {
    wat::parse_str(module_wat()).expect("fixture WAT compiles")
}

const TEMPLATE: &str = r#"
(module
  (import "env" "db_query" (func $db_query (param i32 i32) (result i32)))
  (import "env" "db_execute" (func $db_execute (param i32 i32) (result i32)))
  (import "env" "nats_publish" (func $nats_publish (param i32 i32 i32 i32) (result i32)))
  (import "env" "cufflink_log" (func $log (param i32 i32 i32)))
  (import "env" "util_sleep_ms" (func $sleep_ms (param i32) (result i32)))
  (memory (export "memory") 2)
  (global $heap (mut i32) (i32.const 4096))
$DATA
  (func $alloc (export "alloc") (param $n i32) (result i32)
    (local $p i32) (local $end i32) (local $pages i32)
    (local.set $p (global.get $heap))
    (local.set $end (i32.add (local.get $p) (local.get $n)))
    (local.set $pages (i32.shr_u (i32.add (local.get $end) (i32.const 65535)) (i32.const 16)))
    (if (i32.gt_u (local.get $pages) (memory.size))
      (then (drop (memory.grow (i32.sub (local.get $pages) (memory.size))))))
    (global.set $heap (local.get $end))
    (local.get $p))
  (func $respond (param $src i32) (param $len i32) (result i32)
    (local $out i32)
    (local.set $out (call $alloc (i32.add (local.get $len) (i32.const 4))))
    (i32.store (local.get $out) (local.get $len))
    (memory.copy (i32.add (local.get $out) (i32.const 4)) (local.get $src) (local.get $len))
    (local.get $out))
  (export "echo_forwarded" (func $echo))
  (func $echo (export "echo") (param $ptr i32) (param $len i32) (result i32)
    (local $out i32) (local $total i32) (local $body i32)
    (local.set $total (i32.add (local.get $len) (i32.const $PREFIX_LEN)))
    (local.set $total (i32.add (local.get $total) (i32.const 1)))
    (local.set $out (call $alloc (i32.add (local.get $total) (i32.const 4))))
    (i32.store (local.get $out) (local.get $total))
    (local.set $body (i32.add (local.get $out) (i32.const 4)))
    (memory.copy (local.get $body) (i32.const $PREFIX_OFF) (i32.const $PREFIX_LEN))
    (local.set $body (i32.add (local.get $body) (i32.const $PREFIX_LEN)))
    (memory.copy (local.get $body) (local.get $ptr) (local.get $len))
    (i32.store8 (i32.add (local.get $body) (local.get $len)) (i32.const 125))
    (local.get $out))
  (func (export "spin") (param i32 i32) (result i32)
    (loop $forever (br $forever))
    (unreachable))
  (func (export "hog") (param i32 i32) (result i32)
    (if (i32.eq (memory.grow (i32.const 16384)) (i32.const -1)) (then (unreachable)))
    (call $respond (i32.const $OK_OFF) (i32.const $OK_LEN)))
  (func (export "hold_48") (param i32 i32) (result i32)
    (if (i32.eq (memory.grow (i32.const 768)) (i32.const -1)) (then (unreachable)))
    (i32.store8 (i32.const 50331648) (i32.const 1))
    (drop (call $sleep_ms (i32.const 500)))
    (call $respond (i32.const $OK_OFF) (i32.const $OK_LEN)))
  (func (export "neg_len") (param i32 i32) (result i32)
    (if (result i32) (i32.eq (call $db_query (i32.const 0) (i32.const -1)) (i32.const -1))
      (then (call $respond (i32.const $RC_MINUS_ONE_OFF) (i32.const $RC_MINUS_ONE_LEN)))
      (else (call $respond (i32.const $RC_ZERO_OFF) (i32.const $RC_ZERO_LEN)))))
  (func (export "slow_sql") (param i32 i32) (result i32)
    (if (result i32) (i32.eq (call $db_execute (i32.const $SLEEP_600_OFF) (i32.const $SLEEP_600_LEN)) (i32.const -1))
      (then (call $respond (i32.const $RC_MINUS_ONE_OFF) (i32.const $RC_MINUS_ONE_LEN)))
      (else (call $respond (i32.const $OK_OFF) (i32.const $OK_LEN)))))
  (func (export "big_query") (param i32 i32) (result i32)
    (if (result i32) (i32.eq (call $db_query (i32.const $BIG_QUERY_OFF) (i32.const $BIG_QUERY_LEN)) (i32.const -1))
      (then (call $respond (i32.const $RC_MINUS_ONE_OFF) (i32.const $RC_MINUS_ONE_LEN)))
      (else (call $respond (i32.const $RC_ZERO_OFF) (i32.const $RC_ZERO_LEN)))))
  (func (export "sleep2") (param i32 i32) (result i32)
    (drop (call $db_execute (i32.const $SLEEP_2_OFF) (i32.const $SLEEP_2_LEN)))
    (call $respond (i32.const $OK_OFF) (i32.const $OK_LEN)))
  (func (export "big_response") (param i32 i32) (result i32)
    (i32.store (i32.const 4096) (i32.const 2097152))
    (i32.const 4096))
  (func (export "insert_slow") (param i32 i32) (result i32)
    (drop (call $db_execute (i32.const $INSERT_SLOW_OFF) (i32.const $INSERT_SLOW_LEN)))
    (call $respond (i32.const $OK_OFF) (i32.const $OK_LEN)))
  (func $publish (param $subject i32) (param $len i32) (result i32)
    (if (result i32)
      (i32.eq
        (call $nats_publish (local.get $subject) (local.get $len) (i32.const $OK_OFF) (i32.const $OK_LEN))
        (i32.const -1))
      (then (call $respond (i32.const $RC_MINUS_ONE_OFF) (i32.const $RC_MINUS_ONE_LEN)))
      (else (call $respond (i32.const $RC_ZERO_OFF) (i32.const $RC_ZERO_LEN)))))
  (func (export "publish_claimed") (param i32 i32) (result i32)
    (call $publish (i32.const $CLAIMED_SUBJECT_OFF) (i32.const $CLAIMED_SUBJECT_LEN)))
  (func (export "publish_unclaimed") (param i32 i32) (result i32)
    (call $publish (i32.const $UNCLAIMED_SUBJECT_OFF) (i32.const $UNCLAIMED_SUBJECT_LEN)))
  (func $append (param $to i32) (param $src i32) (param $len i32) (result i32)
    (memory.copy (local.get $to) (local.get $src) (local.get $len))
    (i32.add (local.get $to) (local.get $len)))
  (func (export "insert_ordered") (param $ptr i32) (param $len i32) (result i32)
    (local $sql i32) (local $end i32)
    (local.set $sql (call $alloc (i32.add (i32.const 128) (i32.mul (local.get $len) (i32.const 2)))))
    (local.set $end (call $append (local.get $sql) (i32.const $INSERT_ORDERED_HEAD_OFF) (i32.const $INSERT_ORDERED_HEAD_LEN)))
    (local.set $end (call $append (local.get $end) (local.get $ptr) (local.get $len)))
    (local.set $end (call $append (local.get $end) (i32.const $INSERT_ORDERED_MID_OFF) (i32.const $INSERT_ORDERED_MID_LEN)))
    (local.set $end (call $append (local.get $end) (local.get $ptr) (local.get $len)))
    (local.set $end (call $append (local.get $end) (i32.const $INSERT_ORDERED_TAIL_OFF) (i32.const $INSERT_ORDERED_TAIL_LEN)))
    (drop (call $db_execute (local.get $sql) (i32.sub (local.get $end) (local.get $sql))))
    (call $respond (i32.const $OK_OFF) (i32.const $OK_LEN)))
  (func (export "fail_always") (param i32 i32) (result i32)
    (call $respond (i32.const $UPSTREAM_OFF) (i32.const $UPSTREAM_LEN)))
  (func (export "fail_first") (param $ptr i32) (param $len i32) (result i32)
    (if (result i32)
      (i32.eq (i32.load8_u (i32.add (local.get $ptr) (i32.const 19))) (i32.const 49))
      (then (call $respond (i32.const $UPSTREAM_OFF) (i32.const $UPSTREAM_LEN)))
      (else (call $respond (i32.const $OK_OFF) (i32.const $OK_LEN)))))
  (export "say_on_a_worker" (func $say))
  (func $say (export "say") (param i32 i32) (result i32)
    (call $log (i32.const 2) (i32.const $SAID_OFF) (i32.const $SAID_LEN))
    (drop (call $db_query (i32.const $SELECT_ONE_OFF) (i32.const $SELECT_ONE_LEN)))
    (call $respond (i32.const $OK_OFF) (i32.const $OK_LEN)))
  (func (export "hook_query") (param i32 i32) (result i32)
    (if (result i32) (i32.eq (call $db_query (i32.const $SELECT_ONE_OFF) (i32.const $SELECT_ONE_LEN)) (i32.const -1))
      (then (call $respond (i32.const $HOOK_ERROR_OFF) (i32.const $HOOK_ERROR_LEN)))
      (else (call $respond (i32.const $OK_OFF) (i32.const $OK_LEN)))))
)
"#;
