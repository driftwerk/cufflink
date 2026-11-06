# Parsing SQL with PostgreSQL's parser (`sql_parse`)

A guest that accepts SQL it did not write, such as an ad-hoc query endpoint
or a tool that turns a question into a `SELECT`, may need more than
`db::query_read_only`'s advice to check that the text starts with `SELECT`.
To allow-list the relations and functions a statement reaches, it needs a
parse tree, and the tree has to be the one the database itself would read.
Only PostgreSQL's grammar gives that.

A guest cannot carry that parser. `pg_query` compiles libpg_query's C
sources, every parse runs inside `PG_TRY` (`sigsetjmp`/`siglongjmp`), and
`wasm32-unknown-unknown` has neither a libc nor `setjmp`. Pure-Rust parsers
read PostgreSQL-specific forms differently from the server. So parsing is a
platform capability: a host function, `sql_parse`, and an SDK function,
`cufflink_fn::sql::parse`.

## 1. What a guest sees

```rust
// cufflink-fn, feature "sql-parse"
pub fn parse(sql: &str) -> Result<Parsed, ParseError>;

pub struct Parsed {
    pub tree: serde_json::Value,
    pub parser: String,
    pub fingerprint: String,
}

pub enum ParseError { Syntax(String), TooLarge(String), Unavailable(String) }
```

- **The tree** is exactly `serde_json::to_value(pg_query::parse(sql)?.protobuf)`
  for one pinned release of `pg_query`, which fixes one PostgreSQL grammar.
  Its `version` field (`PG_VERSION_NUM`) is included.
- **`parser`** is that `pg_query` release's version, `6.2.1` (the
  PostgreSQL 17 grammar). `pg_query` 18.1.0 exists. Moving to it is a change
  of identity, made in its own release.
- **`fingerprint`** identifies the parser's output, not only its version. It
  is the SHA-256 of the trees of a probe corpus that the release fixes and
  publishes (`sql-parse/probe.sql`). Each line of the corpus is one
  statement. The digest is taken over, for each statement in order, its
  tree in the canonical form (below) followed by one `\n`. A guest that
  vendors the corpus can compute the same value from its own native parser.
  It can then refuse to act on a platform whose trees would differ: a
  `pg_query` bump, a grammar change, or a different `prost` or `serde` tag
  representation. A release that changes the fingerprint says so in
  `docs/versions.md`.
- **The canonical form** is `serde_json::to_string` of the tree as a
  `serde_json::Value`, with `preserve_order` off. Keys are sorted, there is
  no whitespace, and strings use serde_json's escapes. The platform writes
  that text straight from `pg_query`'s protobuf, sorting each object's
  keys itself, so it never builds the `Value` (about thirty times the
  text) and its output does not depend on `preserve_order` (section 4).

The function is advertised as the platform feature `sql_parse`. The SDK's
`sql-parse` cargo feature records it in the module, so a platform that does
not advertise it refuses the deploy. `parse` lives in the `sql` module, so
the feature enables `db-v2` too.

The guest hands the host function the statement's bytes as they are, not
inside a JSON request, so a 96 KiB statement costs 96 KiB.

## 2. Errors

| Kind | When | Message |
|---|---|---|
| `Syntax` | the statement does not parse, or holds a NUL byte | fixed: `the statement is not valid SQL` |
| `TooLarge` | the input is over 96 KiB, its nesting is over the bound, it has more than 100 forms the grammar warns about (section 4), the tree is too deep for the parser's own decoder or nests deeper than a guest can read (section 4), the tree's text is over the host response cap, or the parse ran out of the parser process's memory | which bound was passed, with no part of the input |
| `Unavailable` | the parser could not run, faulted, or took too long for this call | fixed |

PostgreSQL's messages quote the input (`syntax error at or near "…"`,
`memory exhausted at or near "("`), and so can identifiers in a few others.
No message from the parser reaches the guest: each kind has fixed text. A
guest that must show the caller where the statement went wrong can find the
cursor position itself.

A tree deeper than about 100 nested protobuf messages cannot be decoded by
`prost` 0.13's default recursion limit. `pg_query::parse` then fails with
`Error::Decode`, so a native `pg_query` refuses the statement too. The
platform answers `TooLarge`, so its decision is the same.

## 3. Containment: the parser runs in other processes

The parser is C code running over untrusted input. A fault in it cannot be
caught inside the process: a stack overflow aborts the whole process, and so
can a segfault. Measured on `pg_query` 6.2.1:

| Input | Depth | Outcome on the stated stack |
|---|---|---|
| `SELECT 1+1+…` (a left-deep chain) | 10 000 | parses (Decode error) on 4 MiB, 90 ms |
| the same | 20 000 | stack overflow on 4 MiB; 320 ms on 16 MiB |
| the same | 100 000 | 7.5 s on 1 GiB |
| `UNION SELECT`, `JOIN`, `::int`, `COLLATE`, `\|\|` chains | 10 000 to 100 000 | as above: depth is unbounded, cost quadratic |
| nested `(SELECT …)` | 2 000 | stack overflow on 2 MiB |
| `NOT NOT …` | 9 000 | stack overflow on 2 MiB |
| `ARRAY[[…]]` | 9 000 | stack overflow on 2 MiB |
| 100 000 `(` | | `memory exhausted` error: bison's own stack limit |
| `x = 1 AND …`, 1 MiB | flat | 1.0 s, 875 MB resident, a 27 MB tree |
| `SELECT a, a, …`, 1 MiB | flat | 5.2 s, 5.5 GB resident, an 85 MB tree |
| `CREATE GLOBAL TEMP TABLE t();` × 35 000 | flat | 17 s: quadratic, see section 4 |

Bison bounds the right-nesting forms with its own stack (`YYMAXDEPTH`), but
not before they overflow a small thread stack. The left-deep forms are not
bounded at all. Their tree is as deep as the chain, and the C functions that
write it out recurse once per level. The parser's own `check_stack_depth()`
never fires, because nothing sets `stack_base_ptr`.

So the platform does not run the parser in its own process. A small,
separately built binary, `cufflink-sql-parser`, does the parsing. The
platform keeps a pool of them, `SQL_PARSE_PROCESSES` (2 by default), and
talks to each over its stdin and stdout:

- **The process inherits only stdin, stdout and stderr.** Before anything
  else it closes every descriptor past 2 (`close_range`, or what
  `/proc/self/fd` lists before Linux 5.9 or where a container's filter
  refuses `close_range`), so nothing the platform holds
  outlives its confinement.
- **One parse at a time per process**, all on one thread with a 64 MiB
  stack that the process starts before it confines itself. Each parse
  redirects the process's file descriptor 2, so one parse at a time also
  removes the race between concurrent parses. The process's stderr is
  `/dev/null`, and nothing else in it writes there. After each answer it
  hands the memory the parse freed back to the system (`malloc_trim`, on
  glibc).
- **One allocator arena.** On glibc the process keeps every thread's
  allocations in the main arena (`M_ARENA_MAX` 1), set before the parse
  thread starts. A thread's own arena, the first time it shrinks, opens
  `/proc/sys/vm/overcommit_memory`, which the seccomp filter answers by
  ending the process: the main thread freeing a large tree the parse
  thread built did exactly that in CI, after the answer had been sent.
  The main arena shrinks through `brk` alone, and with one parse at a time
  one arena costs no contention.
- **Confinement.** Before it greets, the process sets its address space
  limit and `oom_score_adj` (below), makes itself undumpable, starts its
  parse thread and computes the fingerprint and one refusal through it, so
  the thread has finished starting, and installs a seccomp filter on every
  thread, with `no_new_privs`. The filter allows only what a parse and its answer need:
  `read` and `write`, `close`, `dup`, `dup2`/`dup3`, `pipe`/`pipe2`,
  `fcntl` and `fstat` for the parser's capture of its own stderr, the
  allocator's `brk`, `mmap`, `munmap`, `mremap`, `mprotect` and `madvise`,
  `futex` and `sched_yield` for the hand-off to the parse thread,
  `clock_gettime`, `getrandom`, signal returns, and `exit`. Any other
  system call ends the process. It cannot open a file, a socket or a
  program, or start a thread or a process. A process that cannot confine
  itself exits without greeting, so it is never used. The platform and the
  worker make themselves undumpable too, so a process running as their
  user cannot read their memory; neither writes a core dump.
- **Separate users and namespaces were considered** and not used: a
  separate uid, or `unshare(CLONE_NEWNET)`, needs `CAP_SETUID` or
  `CAP_SYS_ADMIN` or user namespaces, which an unprivileged container does
  not have. The filter's denial of `socket`, `connect`, `open` and
  `execve` covers what they would.
- **Each parse runs in a task that owns its process** from the request to
  the answer's last byte, never in the caller. A caller that gives up,
  because its budget ran out or its invocation was cancelled, is answered
  `Unavailable`, and the task finishes the exchange, reads and discards
  the tree, and returns the process to the pool. A parse is not started
  with less than 100 ms of the invocation's budget left (`Unavailable`), so
  a nearly spent budget does not take a process for a call it cannot wait
  for.
- **A fault costs one call.** When a process exits, stops answering within
  `SQL_PARSE_TIMEOUT_MS` (5000), or sends a malformed frame, the call is
  answered `Unavailable` and the process is killed. A process that ended
  while idle, killed by the kernel for instance, is found within a second
  or by the next call that would use it, and costs no call. Replacements
  are started in the background, never under a caller's deadline. Every
  process lost and every start that fails since a process last answered a
  call adds to a backoff before the next start, from 100 ms to 5 s, so a
  statement that crashes its process again and again cannot keep the
  platform starting processes.
- **A process serves one tenant.** A process that has served a tenant is
  given back marked with it, and is used again only for that tenant. A
  call of another tenant takes a fresh process, or, when only other
  tenants' processes are idle, has the one idle longest replaced. The
  replacement starts first, and the old process is killed only once the
  replacement has greeted; the replacement is that call's own, so it does
  not queue again, and its start, which is no failure, waits for no
  backoff. Its time is charged to that call's tenant. A start that fails
  leaves the old process idle and serving its tenant, and the call
  `Unavailable`; until the backoff that failure set has passed, calls that
  would replace a process are `Unavailable` without trying, so failing
  starts never drain the pool. A statement that took control of its process
  can therefore read, and answer falsely, only its own tenant's later
  statements. The cost is a process start, 10 to 60 ms measured, most of
  it parsing the probe corpus, whenever a process changes tenants.
- **The platform checks what a process answers.** A tree's text must be
  one well-formed JSON object and nothing after it, or the call is
  `Unavailable` and the process is replaced, so no answer can reach past
  the tree in the answer the platform writes around it. A greeting must
  match the platform's own exactly (section 5).
- **A share for each tenant.** One tenant's parses at once are at most
  `SQL_PARSE_PER_TENANT` (half the processes, at least one); its further
  calls wait for one of its own turns, within their budget. A turn is held
  until the exchange ends, whether or not its caller waited, so a tenant
  whose calls give up early still holds no more than its share. A tenant
  whose parses lose three processes, by timing out or ending them, within
  `SQL_PARSE_TENANT_COOLDOWN_MS` (60 s) has its parses answered
  `Unavailable` for that long, without asking a process, so its statements
  cannot keep the other tenants waiting on replacements. The count is the
  tenant's own: other tenants' answers do not reset it.
- **Process time for each tenant.** Each tenant has a bucket of
  `SQL_PARSE_TENANT_BUSY_MS` (10 000) of process time that refills at that
  much every `SQL_PARSE_TENANT_COOLDOWN_MS` (60 s), a sixth of one process
  on average. Each exchange is charged the time it held its process once
  it ends, a timeout the whole timeout, and so is a process start its call
  forced. A tenant whose bucket is empty has its parses answered
  `Unavailable`, without a process being asked, until it refills; the
  bucket is checked again once a call has waited for its tenant's turn. Ordinary statements parse in a millisecond or two, so this
  touches only a tenant that keeps processes busy with the costliest
  statements, and keeps processes free for the others: over time two such
  tenants take a third of one process between them, and leave five sixths
  of the default pool of two to the rest. Within the share, waiting callers are served in the order
  they came, and each tenant waits at most once in that queue for each of
  its turns, so no tenant can push ahead of another.
- **Memory charged to the guest.** The caller's charge goes with the
  exchange: the request's copy of the statement is charged before it is
  made, and the tree's buffer before it is read, and both charges last
  until the bytes they pay for are freed. A caller that stops waiting
  leaves its charge with the exchange until it ends, and a tree that no
  caller waits for is read and discarded without being charged.
- **Memory.** On Linux each process caps its own address space at
  `SQL_PARSE_MEMORY_MB` (1024) with `setrlimit(RLIMIT_AS)`. A parse that
  runs out of it inside PostgreSQL's allocator is `TooLarge`; one that runs
  out elsewhere ends the process (`Unavailable`). It sets its
  `oom_score_adj` to 1000, so under memory pressure the kernel kills a
  parser before the platform; it does so before it makes itself
  undumpable, which hands its `/proc` entries to root, and a process that
  cannot is never started. The processes share the
  platform's cgroup and sit outside the guest memory budget's ledger: their
  worst case, `SQL_PARSE_PROCESSES × SQL_PARSE_MEMORY_MB`, has to fit in
  the memory the budget leaves outside itself
  ([sizing](../deployment.md#sql-parser-processes)).
- **Advertising.** The platform advertises `sql_parse` once a process has
  started and answered its handshake, which carries `parser` and
  `fingerprint`, both equal to the platform's own. Startup waits at most
  5 s for the first handshake. When no process is left and three starts in
  a row have failed, the feature is no longer advertised, deploys that need
  it are refused, and the host function answers `Unavailable`; the starts
  go on, and the first that succeeds advertises it again.
- **What a compromised process reaches.** A parser bug that let a
  statement take control of its process would reach the statements of the
  same tenant that follow, and nothing outside the process. A guest should
  treat a tree as the parser's reading of the statement, and still run the
  statement with the least privilege it needs.

The alternative of a wasm store was considered. libpg_query would have to be
built for `wasm32-wasip1` with setjmp/longjmp lowered to wasm exceptions,
which needs a wasi-sdk toolchain in every build and wasmtime support that
the pinned release does not ship. It would contain faults as well as a
process does, but costs more to build and to keep current.

## 4. Bounds before the parser

Every bound is applied twice: by the platform before a request is sent, so
that a refusal costs no round trip, and by the parser process before it
parses. The parser process applies them because it is the side that must
not overflow.

- **Input:** at most 96 KiB (`MAX_INPUT_BYTES`), refused `TooLarge` by
  its length alone. A parse's time and memory grow with its input, by up
  to about 6 µs and 4.3 KB a byte, most of it the parse and `pg_query`'s
  decoding of the protobuf into Rust structs, so the bound is what keeps
  every parse short. Measured on a release build (Apple silicon), over 61
  shapes of flat statement, each filled to the bound:

  | Statement, 96 KiB | Time | Peak resident | Tree text |
  |---|---|---|---|
  | `SELECT 1 ORDER BY a, a&a&…&a, …` (31 operands) | 0.43 s | 417 MB | 9.3 MB |
  | `SELECT 1, a&a&…&a, …` (31 operands) | 0.43 s | 417 MB | 9.3 MB |
  | `SELECT 1, a&a&a&a&a&a, …` | 0.42 s | 392 MB | 9.1 MB |
  | `SELECT 1, a&a&a, …` | 0.39 s | 371 MB | 8.9 MB |
  | `SELECT 1, U&x, …` | 0.36 s | 342 MB | 8.6 MB |
  | `SELECT a, a, …` | 0.29 to 0.35 s | 268 MB | 7.9 MB |
  | `SELECT 1 ORDER BY a, a, …` | 0.31 s | 264 MB | 8.5 MB |
  | `SELECT f(a ORDER BY a, a, …)` | 0.32 s | 261 MB | 8.5 MB |
  | `SELECT *, *, …` | 0.29 s | 260 MB | 7.4 MB |
  | `SELECT a[1], a[1], …` | 0.27 s | 215 MB | 6.9 MB |
  | `x = 1 AND …` | 0.14 s | 121 MB | 3.3 MB |
  | `IN (1, 1, …)` | 0.11 s | 56 MB | 3.9 MB |
  | 100 `GLOBAL TEMP` tables after a comment | 0.01 s | 5 MB | 39 KB |

  The worst is two fifths of `SQL_PARSE_MEMORY_MB` and a tenth of
  `SQL_PARSE_TIMEOUT_MS`. At 128 KiB the short operator chains reached
  568 MB, past half the address space the confined process is held to,
  so the bound came down to 96 KiB rather than the check going up. The
  lexer's rescanned runs at their own bound (below), filled to the input
  bound, are cheaper still: 256 signs (`1+-+-…1`) at a time take 0.25 s
  and 56 MB, runs of 128 nested comment openings 0.01 s, and a run of
  256 signs going on into a comment the length of the input 0.03 s.
  Before, the same shapes at 1 MiB took up to
  5.2 s and 5.5 GB with the `Value`, and 2.0 s and 2.5 GB without it, so
  no output bound could have kept them inside the process. A guest that
  allow-lists ad-hoc report SQL, capped at 64 KiB of text, fits with room
  to spare.
- **Forms the grammar warns about:** at most `MAX_WARNED_FORMS` (100). The
  grammar parses `GLOBAL TEMP` and `GLOBAL TEMPORARY` with a `WARNING`,
  and every warning computes its cursor position by counting the
  characters before it, so 35 000 of them in 1 MiB took 17 s. The scan
  counts every `GLOBAL` followed by `TEMP` or `TEMPORARY`, outside quotes
  and comments. No other form warns while parsing: the lexer's escape
  warnings need `standard_conforming_strings` off, and notices are not
  emitted at all.
- **Operator runs and comment nesting:** at most `MAX_OPERATOR_RUN` (256)
  operator characters (`~!@#^&|`?+-*/%<>=`) in a row where the lexer
  rescans them, and block comments nested at most `MAX_COMMENT_DEPTH`
  (1000) deep. The lexer reads a run of operator characters again from
  each point where it puts text back, each time to the run's end. Outside
  comments, the operator rule matches the whole run, then gives back a
  trailing `+` or `-` one at a time. Inside a comment, the only rule that
  takes more than one character of a run is a nested `/*`, which matches
  the rest of the run and gives it back. A run therefore cost up to its
  length squared: `SELECT 1` followed by 32 000 `/*` and as many `*/` took
  15.9 s in 128 KiB, and fourteen runs of 9 000 signs 3.9 s. So the scan
  counts runs outside comments and quoted text, and, inside a comment, a
  run from its first nested `/*` to its end. Each byte is then read at most
  about 400 times: up to 256 restarts of the operator rule before a
  comment, which read on into it, once by the comment's own `/*`, and by up
  to 128 nested `/*` in the 256 bytes before it. Lexing
  stays linear in the input, and those statements are `TooLarge` at once.
  Runs in quoted text and line comments are read once, as are a comment's
  runs with no nested `/*`, so `/* ===…=== */` banners of any width pass.
- **Nesting:** a lexical scan, refused `TooLarge`. It reads the input as
  PostgreSQL's lexer does, with quoted strings (`'…'`, `E'…'` with its
  backslash escapes, `U&'…'`, `B'…'`, `X'…'`, `N'…'`), quoted identifiers,
  dollar quotes, `--` comments and nested `/* */` comments. Text inside any
  of these is not structure. Over the tokens that remain it computes two
  measures:
  - **Bracket depth:** `(`, `[`, and `CASE … END` open and close a level. At
    most `MAX_BRACKET_DEPTH` (1000).
  - **Chain depth:** an upper bound on how deep the tree can be along any
    path. In one level, every token except `,`, `AND` and `OR` counts toward
    the current *segment*. A `,`, `AND` or `OR` ends the segment. Their
    operands are siblings: lists are flat, and the grammar flattens `a AND b
    AND c` into one node. The `AND` of a `BETWEEN` is the exception: it
    joins the bounds of one expression, and `BETWEEN`s can nest
    (`1 BETWEEN 1 AND 1 IS TRUE BETWEEN …`), so it counts as any other
    token. `UNION`, `INTERSECT`, `EXCEPT` and `JOIN` also count toward the
    level as a whole, and are never reset inside it, because their chains
    span commas and `AND`s. A level's depth is 1, plus its whole-level
    count, plus the largest of its segments' counts, each with the deepest
    level nested inside that segment. At most `MAX_CHAIN_DEPTH` (10 000).
  - The scan is linear: a closing bracket finds the level it closes by a
    count of the levels of its kind open, and the scan stops at the first
    count past `MAX_CHAIN_DEPTH`.
  - Both measures over-count: every keyword, identifier and literal in a
    segment counts. They are sized so that nothing that `pg_query` can turn
    into a decodable tree comes near them. Such a tree is at most about 50
    nodes deep, and a flat statement of 96 KiB passes as long as no single
    comma-, `AND`- and `OR`-free run in it has more than 10 000 tokens.
- **Tree nesting:** after the parse, a tree whose text nests deeper than
  `MAX_TREE_NESTING` (120) objects and arrays is refused `TooLarge`. The
  guest reads its answer with `serde_json`, which stops at 128 levels, and
  the answer wraps the tree in two more. In SQL terms the limit is about 35
  links of a left-deep arithmetic chain (3 levels each), 25 nested function
  calls (4 each) or 10 nested subqueries (about 10 each); a `UNION` costs 1.
  The probe corpus's deepest tree nests 30. The test host applies the same
  limit.
- **Stack:** the bounds limit the tree to a depth whose parse fits well
  within the 64 MiB thread. An adversarial test runs the deepest inputs that
  pass each bound on a thread with exactly that stack, in the parser process
  itself, and checks that each one answers and that it does so within a time
  limit.
- **Output:** the parser process refuses a tree whose text is over the
  request's `max_output`, which the platform sets to its host response cap
  (`WASM_MAX_HOST_RESPONSE_MB`, 64) less the answer around the tree. It
  sends `TooLarge` rather than the bytes. The text is written as the tree
  is serialised, and the writer stops at the first byte past `max_output`
  and the first object or array past `MAX_TREE_NESTING`, so neither bound
  waits for the whole text.
- **Guest memory:** the input is charged when the platform reads it from
  the guest, as every host call's input is. The answer's length arrives
  first, in its frame header. The platform reserves that many bytes, and
  the answer around them, on the guest's charge before it reads the body,
  and reads the body straight into the answer. If the reservation is
  refused, the call fails as any host call denied memory does, and the
  process's answer is read and discarded, so the process keeps serving.

## 5. The wire

A frame is a 4-byte big-endian length and that many bytes
(`cufflink_sql_parse::wire`). Control frames hold JSON and are at most
64 KiB; a frame past its limit ends the process or, on the platform's side,
costs the process.

- At start the parser process sends `{"protocol": 1, "build": "0.1.0
  input=98304 brackets=1000 …", "parser": "6.2.1", "fingerprint": "…"}`,
  computing the fingerprint by parsing the probe corpus. `protocol`
  changes whenever the frames, the bounds or the canonical text do, and
  `build` is the `cufflink-sql-parse` release the process was built with
  followed by every bound it applies, so builds whose bounds differ never
  take each other's processes, even when no one changed `protocol`. The platform uses a process only if
  its greeting equals the one its own build would send; a greeting
  without either field is not read at all.
- A request is `{"max_output": n}`, then a frame of the statement's bytes,
  at most 96 KiB.
- An answer is `"tree"` followed by a frame of the tree's canonical text,
  so the tree is never parsed or copied again on the platform's side, or
  `{"error": {"kind": "syntax"|"too_large"|"unavailable", "message": "…"}}`.

The platform answers the guest `{"ok": {"fingerprint": …, "parser": …,
"tree": <text>}}`, assembled around the tree's bytes as they were sent, or
`{"error": {"kind": …, "message": …}}`.

## 6. The native test host

`TestHost` parses in its own process, with the same crate, bounds,
canonical form and error mapping, on a thread with the same 64 MiB stack.
It is built with the guest's dependency graph, so before it parses it
checks that its parser reads the probe corpus as the release does, and
panics otherwise, rather than name the release's fingerprint for trees the
platform would not give. Its canonical text sorts keys itself, so a guest
that enables `serde_json/preserve_order` gets the same trees. One
parse runs at a time, behind a process-wide lock, because of the stderr
redirection. A test therefore gets the same `Parsed` that the platform
gives, and the same refusals. The test host does not contain faults. That is
acceptable in `cargo test`, where the input is the test's own. Only a
guest's tests that enable both `test-host` and `sql-parse` build
`pg_query`, so other guests need neither a C toolchain nor libclang.

## 7. Building

- The new crate `cufflink-sql-parse` (`sql-parse/`) holds the bounds, the
  wire, the corpus and, behind its `parser` feature, the parse itself. It is
  published, since `cufflink-fn`'s test host depends on it. The platform
  depends on it without `parser` and calls nothing of `pg_query`. A
  build that compiles the platform and the parser binary in one cargo
  invocation unifies the crate's features, so the platform's build compiles
  `pg_query` there too, unused.
- The binary is its own unpublished crate, `cufflink-sql-parser`
  (`sql-parser/`).
- `pg_query` builds libpg_query with `cc` and generates its bindings with
  `bindgen`, so building the binary needs a C compiler and libclang. The
  platform and worker images build and ship it next to their own binary.
- The platform finds it at `SQL_PARSER_BIN`, by default
  `cufflink-sql-parser` in the platform binary's own directory. The worker
  runs guests too, so it starts a pool in the same way.

## 8. Tests

- **Golden:** a committed corpus (`sql-parse/golden/`): the probe corpus
  and 35 more statements, accepted and refused, each with the answer this
  release gives it, the tree's canonical text or the refusal's kind. The
  library, the confined parser process, the platform and the test host
  are each checked against those bytes, and the library against
  `pg_query`'s own tree through a `Value`. The fingerprint is recomputed
  and compared with the published constant. An ignored test rewrites the
  answers for a release that changes the parser on purpose.
- **Adversarial:** 100 000 nested parentheses; nested subqueries,
  `ARRAY[[[…]]]`, `CASE`, `NOT` and unary-minus chains; left-deep `+`,
  `UNION`, `JOIN` and cast chains; a long `IN` list; unterminated quotes,
  dollar quotes and comments; the input bound plus one byte. Each answers `Syntax` or
  `TooLarge`, or parses, and the parser process then serves the next call.
- **Cost:** in the confined process, the costliest shapes filled to the
  input bound (columns, stars, `ORDER BY`, function arguments, subscripts,
  `IN`, `AND`, `VALUES`, statements, column definitions, the most
  `GLOBAL TEMP`s after a comment, sign runs and nested comment runs at
  their bound) each pass the bounds and are answered as the library
  answers them within half the parse timeout, the short operator chains
  that use the most memory among them. On Linux, the process's resident peak (`VmHWM`)
  is then under half its address space limit. Through the platform, the
  1 MiB statements that cost seconds before the bound came down are
  `TooLarge` at once, and ask no process.
- **Confinement:** on Linux, the running parser process reports
  `Seccomp: 2` and `NoNewPrivs: 1`, and answers the corpus, every refusal
  and the deepest inputs as the library does. A pipe whose write end it
  inherited ends as soon as the test closes its own copy.
- **Faults:** a process past `SQL_PARSE_TIMEOUT_MS` is killed and replaced
  in the background. A caller that stops waiting costs no process: the
  same process serves the next call. A call with less than 100 ms left
  costs no process. A process killed while idle costs no call. A binary
  that does not greet is never used, and readiness goes when no process
  can start and comes back when one can.
- **Shares:** a tenant at its share waits for its own turn while another
  tenant is served. A tenant whose parses spent their process time is
  refused, without a process, while another is served, and served again
  once it refills. A turn waited for and given up leaves nothing behind.
- **Tenants:** a process serves one tenant, and another tenant's call gets
  a different process. Three tenants taking turns on two processes, one
  turn each, are each answered every call within its budget.
- **Charges:** a caller that stops waiting leaves its charge held until
  the exchange ends, and is charged nothing for the tree; a caller that
  waits gets the charge back with the tree.
- **Answers:** a process that answers a tree that is not one JSON object,
  here a forged one that closes the tree early, costs the call
  (`Unavailable`), and never reaches the caller.
- **Concurrency:** many parses at once from many tasks. fd 2 still works
  afterwards, and the platform has leaked no descriptors.
- **Privacy:** a `Syntax` or `TooLarge` answer carries no fragment of the
  input. Each test input contains a sentinel token, and the test checks that
  it does not appear in the answer.
- **Negotiation:** a module that declares `sql_parse` is refused by a
  platform that does not advertise it.
- **Test host:** `TestHost` answers the same `Parsed` as the platform for
  the corpus.
