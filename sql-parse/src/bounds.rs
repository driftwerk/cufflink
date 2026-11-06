//! What the parser is never handed: input past [`MAX_INPUT_BYTES`], and
//! input whose tree could be deep enough to exhaust the parse's stack.
//!
//! PostgreSQL's parser bounds right-nested forms with bison's own stack,
//! but not left-deep chains (`a + b + …`, `… UNION …`, `… JOIN …`), whose
//! tree is as deep as the chain; the C functions that write the tree out
//! recurse once per level. A lexical scan bounds both before parsing. It
//! reads quoting and comments as the server's lexer does, so text inside a
//! literal or comment is never structure, and it over-counts everything
//! else.

use cufflink_types::sql_parse::ParseError;

/// The longest statement parsed. Within the other bounds a parse's time and
/// memory grow linearly with its input, by up to about 4 µs and 2.8 KB a
/// byte; at this size the costliest measured shape, a flat list of column
/// references, takes 0.5 s and 362 MB (release, Apple silicon), a tenth of
/// the default parse timeout and a third of a process's address space.
pub const MAX_INPUT_BYTES: usize = 128 * 1024;

/// `(`, `[` and `CASE … END` levels open at once.
pub const MAX_BRACKET_DEPTH: u32 = 1_000;

/// An upper bound on the tree's depth along any path (see [`check`]).
pub const MAX_CHAIN_DEPTH: u32 = 10_000;

/// `GLOBAL TEMP` and `GLOBAL TEMPORARY`, which the grammar parses with a
/// warning. Each warning counts the characters before it, so their cost
/// grows with their number times the input's length.
pub const MAX_WARNED_FORMS: u32 = 100;

/// Operator characters (`~!@#^&|`?+-*/%<>=`) in a row, in or out of block
/// comments. The lexer reads such a run again from each point where it
/// puts text back: after a trailing `+` or `-`, and after each `/*` that
/// opens a comment or a nested one. So a run costs up to its length
/// squared, and capping it keeps a statement's lexing linear in its length.
pub const MAX_OPERATOR_RUN: usize = 256;

/// Block comments open at once.
pub const MAX_COMMENT_DEPTH: u32 = 1_000;

/// Refuses, as `TooLarge`, input past one of the bounds.
///
/// Within one level, every token but `,`, `AND` and `OR` counts towards the
/// current segment, and those three end it: their operands are siblings,
/// since lists are flat and the grammar flattens `AND` and `OR`. The `AND`
/// of a `BETWEEN` is the exception: it joins the bounds of one expression,
/// which can nest, so it counts as any other token. `UNION`, `INTERSECT`,
/// `EXCEPT` and `JOIN` chains cross commas and `AND`s, so they also count
/// towards the whole level until its statement ends. A level is 1 deep,
/// plus its whole-level count, plus its deepest segment, where a segment is
/// its own count plus the deepest level nested inside it. The scan stops at
/// the first count past [`MAX_CHAIN_DEPTH`], since a depth is at least any
/// of its counts. A `GLOBAL` followed by `TEMP` or `TEMPORARY` counts as a
/// form the grammar warns about, wherever it is, at most
/// [`MAX_WARNED_FORMS`]. Outside quoted text and line comments, at most
/// [`MAX_OPERATOR_RUN`] operator characters stand in a row, and block
/// comments nest at most [`MAX_COMMENT_DEPTH`] deep.
pub fn check(sql: &str) -> Result<(), ParseError> {
    if sql.len() > MAX_INPUT_BYTES {
        return Err(too_large(format!(
            "the statement is over {MAX_INPUT_BYTES} bytes"
        )));
    }
    let depth = Scan::new(sql.as_bytes()).depth()?;
    match depth > MAX_CHAIN_DEPTH {
        true => Err(too_deep()),
        false => Ok(()),
    }
}

fn too_large(message: String) -> ParseError {
    ParseError::TooLarge(message)
}

fn too_deep() -> ParseError {
    too_large(format!("the statement nests deeper than {MAX_CHAIN_DEPTH}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Opened {
    Root,
    Paren,
    Square,
    Case,
}

impl Opened {
    fn index(self) -> usize {
        match self {
            Opened::Root => 0,
            Opened::Paren => 1,
            Opened::Square => 2,
            Opened::Case => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    Open(Opened),
    Close(Opened),
    Separator,
    And,
    Between,
    Statement,
    Chain,
    Case,
    End,
    Global,
    Temp,
    Other,
}

#[derive(Debug, Clone, Copy)]
struct Level {
    opened: Opened,
    whole: u32,
    segment: u32,
    nested: u32,
    deepest_segment: u32,
    deepest_statement: u32,
    /// `BETWEEN`s whose `AND` is still to come.
    betweens: u32,
}

impl Level {
    fn new(opened: Opened) -> Self {
        Self {
            opened,
            whole: 0,
            segment: 0,
            nested: 0,
            deepest_segment: 0,
            deepest_statement: 0,
            betweens: 0,
        }
    }

    fn count(&mut self) {
        self.segment = self.segment.saturating_add(1);
    }

    /// The least depth this level already reaches.
    fn reached(&self) -> u32 {
        self.whole
            .saturating_add(self.segment)
            .saturating_add(self.nested)
            .saturating_add(1)
    }

    fn end_segment(&mut self) {
        self.deepest_segment = self
            .deepest_segment
            .max(self.segment.saturating_add(self.nested));
        self.segment = 0;
        self.nested = 0;
    }

    fn end_statement(&mut self) {
        self.end_segment();
        self.deepest_statement = self
            .deepest_statement
            .max(self.whole.saturating_add(self.deepest_segment));
        self.whole = 0;
        self.deepest_segment = 0;
        self.betweens = 0;
    }

    fn depth(mut self) -> u32 {
        self.end_statement();
        self.deepest_statement.saturating_add(1)
    }
}

struct Scan<'a> {
    sql: &'a [u8],
    at: usize,
    levels: Vec<Level>,
    /// The levels of each kind open, so a close finds its own in O(1).
    open: [u32; 4],
    after_global: bool,
    warned: u32,
    /// Operator characters in a row up to here.
    run: usize,
}

impl<'a> Scan<'a> {
    fn new(sql: &'a [u8]) -> Self {
        Self {
            sql,
            at: 0,
            levels: vec![Level::new(Opened::Root)],
            open: [1, 0, 0, 0],
            after_global: false,
            warned: 0,
            run: 0,
        }
    }

    fn depth(mut self) -> Result<u32, ParseError> {
        while let Some(token) = self.next_token()? {
            self.take(token)?;
        }
        while self.close_top().is_some() {}
        Ok(self.levels.pop().map_or(0, Level::depth))
    }

    fn top(&mut self) -> &mut Level {
        let last = self.levels.len() - 1;
        &mut self.levels[last]
    }

    fn take(&mut self, token: Token) -> Result<(), ParseError> {
        if token == Token::Temp && self.after_global {
            self.warned += 1;
            if self.warned > MAX_WARNED_FORMS {
                return Err(too_large(format!(
                    "the statement has more than {MAX_WARNED_FORMS} forms the parser warns about"
                )));
            }
        }
        self.after_global = token == Token::Global;
        match token {
            Token::Open(opened) => self.open(opened)?,
            Token::Case => {
                self.top().count();
                self.open(Opened::Case)?;
            }
            Token::Close(opened) => self.close(opened),
            Token::End if self.top().opened == Opened::Case => {
                self.top().count();
                self.close_top();
            }
            Token::Separator => self.top().end_segment(),
            Token::And => {
                let level = self.top();
                match level.betweens {
                    0 => level.end_segment(),
                    _ => {
                        level.betweens -= 1;
                        level.count();
                    }
                }
            }
            Token::Between => {
                let level = self.top();
                level.betweens = level.betweens.saturating_add(1);
                level.count();
            }
            Token::Statement => self.top().end_statement(),
            Token::Chain => {
                let level = self.top();
                level.whole = level.whole.saturating_add(1);
                level.count();
            }
            Token::End | Token::Global | Token::Temp | Token::Other => self.top().count(),
        }
        match self.top().reached() > MAX_CHAIN_DEPTH {
            true => Err(too_deep()),
            false => Ok(()),
        }
    }

    fn open(&mut self, opened: Opened) -> Result<(), ParseError> {
        self.top().count();
        if self.levels.len() > MAX_BRACKET_DEPTH as usize {
            return Err(too_large(format!(
                "the statement opens more than {MAX_BRACKET_DEPTH} brackets at once"
            )));
        }
        self.open[opened.index()] += 1;
        self.levels.push(Level::new(opened));
        Ok(())
    }

    /// A closing bracket ends every `CASE` inside the bracket it closes; one
    /// with no bracket of its kind open is an ordinary token, which the
    /// parser refuses.
    fn close(&mut self, opened: Opened) {
        match self.open[opened.index()] {
            0 => self.top().count(),
            _ => {
                while let Some(closed) = self.close_top() {
                    if closed == opened {
                        break;
                    }
                }
            }
        }
    }

    fn close_top(&mut self) -> Option<Opened> {
        if self.levels.len() < 2 {
            return None;
        }
        let closed = self.levels.pop()?;
        self.open[closed.opened.index()] -= 1;
        let depth = closed.depth();
        let parent = self.top();
        parent.nested = parent.nested.max(depth);
        Some(closed.opened)
    }

    fn peek(&self, ahead: usize) -> Option<u8> {
        self.sql.get(self.at + ahead).copied()
    }

    fn next_token(&mut self) -> Result<Option<Token>, ParseError> {
        loop {
            let Some(byte) = self.peek(0) else {
                return Ok(None);
            };
            match byte {
                b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c => {
                    self.run = 0;
                    self.at += 1;
                }
                b'-' if self.peek(1) == Some(b'-') => {
                    self.run = 0;
                    self.line_comment();
                }
                b'/' if self.peek(1) == Some(b'*') => self.block_comment()?,
                _ => {
                    self.lexed(byte)?;
                    return Ok(Some(self.token(byte)));
                }
            }
        }
    }

    /// Counts `byte` into the run of operator characters it ends or
    /// extends.
    fn lexed(&mut self, byte: u8) -> Result<(), ParseError> {
        self.run = match is_operator_char(byte) {
            true => self.run + 1,
            false => 0,
        };
        match self.run > MAX_OPERATOR_RUN {
            true => Err(too_large(format!(
                "the statement has more than {MAX_OPERATOR_RUN} operator characters in a row"
            ))),
            false => Ok(()),
        }
    }

    fn token(&mut self, byte: u8) -> Token {
        match byte {
            b'(' => self.single(Token::Open(Opened::Paren)),
            b')' => self.single(Token::Close(Opened::Paren)),
            b'[' => self.single(Token::Open(Opened::Square)),
            b']' => self.single(Token::Close(Opened::Square)),
            b',' => self.single(Token::Separator),
            b';' => self.single(Token::Statement),
            b'\'' => {
                self.at += 1;
                self.quoted(b'\'', false);
                Token::Other
            }
            b'"' => {
                self.at += 1;
                self.quoted(b'"', false);
                Token::Other
            }
            b'$' => self.dollar(),
            b'0'..=b'9' => self.number(),
            b'.' if self.peek(1).is_some_and(|next| next.is_ascii_digit()) => self.number(),
            _ if is_ident_start(byte) => self.word(),
            _ => self.single(Token::Other),
        }
    }

    fn single(&mut self, token: Token) -> Token {
        self.at += 1;
        token
    }

    fn line_comment(&mut self) {
        self.at = self.sql[self.at..]
            .iter()
            .position(|&b| b == b'\n' || b == b'\r')
            .map_or(self.sql.len(), |end| self.at + end);
    }

    fn block_comment(&mut self) -> Result<(), ParseError> {
        let mut open = 0u32;
        while let Some(byte) = self.peek(0) {
            match (byte, self.peek(1)) {
                (b'/', Some(b'*')) => {
                    open += 1;
                    if open > MAX_COMMENT_DEPTH {
                        return Err(too_large(format!(
                            "the statement nests more than {MAX_COMMENT_DEPTH} comments"
                        )));
                    }
                    self.lexed(b'/')?;
                    self.lexed(b'*')?;
                    self.at += 2;
                }
                (b'*', Some(b'/')) => {
                    self.lexed(b'*')?;
                    self.lexed(b'/')?;
                    self.at += 2;
                    open -= 1;
                    if open == 0 {
                        return Ok(());
                    }
                }
                _ => {
                    self.lexed(byte)?;
                    self.at += 1;
                }
            }
        }
        Ok(())
    }

    /// The rest of a literal or identifier quoted by `quote`, whose opening
    /// quote has been read. A doubled quote is the quote itself; with
    /// `escapes`, so is a quote after a backslash.
    fn quoted(&mut self, quote: u8, escapes: bool) {
        while let Some(byte) = self.peek(0) {
            self.at += 1;
            match byte {
                b'\\' if escapes => self.at += 1,
                _ if byte == quote => match self.peek(0) == Some(quote) {
                    true => self.at += 1,
                    false => return,
                },
                _ => {}
            }
        }
        self.at = self.at.min(self.sql.len());
    }

    /// `$1`, or a dollar-quoted string `$tag$…$tag$`, or a lone `$`.
    fn dollar(&mut self) -> Token {
        let start = self.at;
        self.at += 1;
        if self.peek(0).is_some_and(|b| b.is_ascii_digit()) {
            while self.peek(0).is_some_and(|b| b.is_ascii_digit()) {
                self.at += 1;
            }
            return Token::Other;
        }
        if self.peek(0).is_some_and(is_ident_start) {
            while self.peek(0).is_some_and(is_dollar_tag) {
                self.at += 1;
            }
        }
        if self.peek(0) != Some(b'$') {
            self.at = start + 1;
            return Token::Other;
        }
        self.at += 1;
        let delimiter = &self.sql[start..self.at];
        self.at = self.sql[self.at..]
            .windows(delimiter.len())
            .position(|window| window == delimiter)
            .map_or(self.sql.len(), |end| self.at + end + delimiter.len());
        Token::Other
    }

    fn number(&mut self) -> Token {
        while self
            .peek(0)
            .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
        {
            self.at += 1;
        }
        Token::Other
    }

    fn word(&mut self) -> Token {
        let start = self.at;
        while self.peek(0).is_some_and(is_ident_cont) {
            self.at += 1;
        }
        let word = &self.sql[start..self.at];
        if let Some(quote) = self.prefixed_quote(word) {
            self.quoted(quote, word.eq_ignore_ascii_case(b"e"));
            return Token::Other;
        }
        keyword(word)
    }

    /// The quote a one-letter prefix opens (`E'`, `B'`, `X'`, `N'`, `U&'`,
    /// `U&"`), consuming the prefix's `&` and the quote.
    fn prefixed_quote(&mut self, word: &[u8]) -> Option<u8> {
        let [letter] = word else {
            return None;
        };
        match (letter.to_ascii_lowercase(), self.peek(0), self.peek(1)) {
            (b'e' | b'b' | b'x' | b'n', Some(b'\''), _) => {
                self.at += 1;
                Some(b'\'')
            }
            (b'u', Some(b'&'), Some(quote @ (b'\'' | b'"'))) => {
                self.at += 2;
                Some(quote)
            }
            _ => None,
        }
    }
}

fn keyword(word: &[u8]) -> Token {
    let is = |keyword: &str| word.eq_ignore_ascii_case(keyword.as_bytes());
    if is("or") {
        Token::Separator
    } else if is("and") {
        Token::And
    } else if is("between") {
        Token::Between
    } else if is("union") || is("intersect") || is("except") || is("join") {
        Token::Chain
    } else if is("case") {
        Token::Case
    } else if is("end") {
        Token::End
    } else if is("global") {
        Token::Global
    } else if is("temp") || is("temporary") {
        Token::Temp
    } else {
        Token::Other
    }
}

fn is_operator_char(byte: u8) -> bool {
    b"~!@#^&|`?+-*/%<>=".contains(&byte)
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

fn is_ident_cont(byte: u8) -> bool {
    is_ident_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

fn is_dollar_tag(byte: u8) -> bool {
    is_ident_start(byte) || byte.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The depth the scan finds, or `u32::MAX` where it stopped early.
    fn depth(sql: &str) -> u32 {
        match Scan::new(sql.as_bytes()).depth() {
            Ok(depth) => depth,
            Err(ParseError::TooLarge(message)) if message.contains("deeper") => u32::MAX,
            Err(other) => panic!("{other:?}"),
        }
    }

    fn refused(sql: &str) -> String {
        match check(sql) {
            Err(ParseError::TooLarge(message)) => message,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn flat_lists_and_boolean_chains_stay_shallow() {
        let in_list = format!("SELECT 1 WHERE x IN (1{})", ",1".repeat(100_000));
        assert!(depth(&in_list) < 10, "{}", depth(&in_list));
        let conjunction = format!("SELECT 1 WHERE {}true", "x = 1 AND ".repeat(12_000));
        assert!(depth(&conjunction) < 10);
        assert!(check(&conjunction).is_ok());
        let columns = format!("SELECT a{} FROM t", ", a AS b".repeat(50_000));
        assert!(depth(&columns) < 10);
        let ranges = format!(
            "SELECT 1 WHERE {}true",
            "x NOT BETWEEN SYMMETRIC 1 AND 2 AND ".repeat(20_000)
        );
        assert!(depth(&ranges) < 20, "{}", depth(&ranges));
        let frames = format!(
            "SELECT {}1",
            "sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AND ".repeat(5_000)
        );
        assert!(depth(&frames) < 20, "{}", depth(&frames));
    }

    #[test]
    fn the_and_of_a_between_does_not_end_its_segment() {
        for chain in [
            format!("SELECT 1{}", " BETWEEN 1 AND 1 IS TRUE".repeat(2_000)),
            format!("SELECT 1{}", " BETWEEN 1 AND NOT 1".repeat(2_500)),
        ] {
            assert!(depth(&chain) > MAX_CHAIN_DEPTH, "{}", depth(&chain));
            refused(&chain);
        }
    }

    #[test]
    fn unmatched_closes_cost_one_step_each() {
        let started = std::time::Instant::now();
        let closes = format!(
            "SELECT {}1{}",
            "(".repeat(1_000),
            "]".repeat(MAX_INPUT_BYTES - 2_000)
        );
        assert!(refused(&closes).contains("deeper"));
        let cases = format!(
            "SELECT {}1{}",
            "CASE WHEN true THEN (".repeat(400),
            ")".repeat(400)
        );
        assert!(check(&cases).is_ok());
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn left_deep_chains_count_every_link() {
        for chain in [
            format!("SELECT 1{}", "+1".repeat(6_000)),
            format!("SELECT 1{}", "::int".repeat(4_000)),
            format!("SELECT 'a'{}", " COLLATE \"C\"".repeat(5_000)),
            format!("SELECT 1, 2{}", " UNION SELECT 1, 2".repeat(10_001)),
            format!("SELECT 1 FROM a{}", " JOIN a ON x AND y".repeat(10_001)),
            format!("SELECT {}true", "NOT ".repeat(10_001)),
        ] {
            assert!(depth(&chain) > MAX_CHAIN_DEPTH, "{}", &chain[..40]);
            refused(&chain);
        }
    }

    #[test]
    fn a_chain_after_a_nested_level_adds_to_its_depth() {
        let inner = format!("({})", "1+".repeat(4_000) + "1");
        let wrapped = format!("SELECT {inner}{}", "+1".repeat(4_000));
        assert!(depth(&wrapped) > 12_000);
        let case = format!(
            "SELECT 1{} + CASE WHEN a AND b THEN 1 END{}",
            "+1".repeat(3_000),
            "+1".repeat(3_000)
        );
        assert!(depth(&case) > 12_000);
    }

    #[test]
    fn brackets_and_case_levels_are_bounded() {
        let parens = format!("SELECT {}1{}", "(".repeat(1_001), ")".repeat(1_001));
        assert!(refused(&parens).contains("brackets"));
        let fine = format!("SELECT {}1{}", "(".repeat(1_000), ")".repeat(1_000));
        assert!(check(&fine).is_ok());
        let unclosed = format!("SELECT {}1", "ARRAY[".repeat(1_001));
        assert!(refused(&unclosed).contains("brackets"));
        let cases = format!(
            "SELECT {}1{}",
            "CASE WHEN true THEN ".repeat(1_001),
            " END".repeat(1_001)
        );
        assert!(refused(&cases).contains("brackets"));
        let nested = format!("SELECT {}1{}", "(SELECT ".repeat(1_001), ")".repeat(1_001));
        assert!(refused(&nested).contains("brackets"));
    }

    #[test]
    fn quoted_text_and_comments_are_not_structure() {
        let parens = "(".repeat(5_000);
        for hidden in [
            format!("SELECT '{parens}'"),
            format!("SELECT 'it''s {parens}'"),
            format!("SELECT E'\\' {parens}'"),
            format!("SELECT e'\\\\', '{parens}'"),
            format!("SELECT U&'{parens}'"),
            format!("SELECT X'{parens}', B'{parens}', n'{parens}'"),
            format!("SELECT \"{parens}\""),
            format!("SELECT U&\"a\"\"{parens}\""),
            format!("SELECT $${parens}$$"),
            format!("SELECT $tag${parens} $$ $tag$"),
            format!("SELECT 1 -- {parens}\n"),
            format!("SELECT 1 /* {parens} /* nested */ {parens} */"),
        ] {
            assert!(check(&hidden).is_ok(), "{}", &hidden[..20]);
            assert!(depth(&hidden) < 10);
        }
    }

    #[test]
    fn what_only_looks_like_quoting_is_structure() {
        let parens = "(".repeat(1_001);
        for open in [
            format!("SELECT '\\' {parens}'"),
            format!("SELECT abcE'x' {parens}"),
            format!("SELECT a$b$ {parens} $b$"),
            format!("SELECT $1 {parens}"),
            format!("SELECT 1 - {parens}"),
            format!("SELECT 1 */ {parens}"),
        ] {
            assert!(
                matches!(check(&open), Err(ParseError::TooLarge(_))),
                "{}",
                &open[..20]
            );
        }
    }

    #[test]
    fn unterminated_quoting_ends_the_scan() {
        for open in [
            "SELECT 'abc",
            "SELECT E'abc\\",
            "SELECT \"abc",
            "SELECT $q$abc",
            "SELECT 1 /* abc /* x */",
            "SELECT U&",
            "SELECT $",
            "SELECT $a",
        ] {
            assert!(check(open).is_ok(), "{open}");
        }
    }

    #[test]
    fn global_temp_is_counted_wherever_it_is_and_bounded() {
        let tables = |n: usize| "CREATE GLOBAL TEMP TABLE t();".repeat(n);
        assert!(check(&tables(MAX_WARNED_FORMS as usize)).is_ok());
        assert!(refused(&tables(MAX_WARNED_FORMS as usize + 1)).contains("warns"));
        let spelled = |n: usize| "create global /* c */ temporary table t();".repeat(n);
        assert!(refused(&spelled(MAX_WARNED_FORMS as usize + 1)).contains("warns"));
        let hidden = format!(
            "SELECT '{}', $q${}$q$, global, temp",
            tables(200),
            tables(200)
        );
        assert!(check(&hidden).is_ok());
        let local = "CREATE LOCAL TEMP TABLE t(); CREATE TEMP TABLE t();".repeat(500);
        assert!(check(&local).is_ok());
    }

    #[test]
    fn operator_runs_are_bounded_in_and_out_of_comments() {
        let signs = |n: usize| format!("SELECT 1{}1", "+-".repeat(n / 2));
        assert!(check(&signs(MAX_OPERATOR_RUN)).is_ok());
        assert!(refused(&signs(MAX_OPERATOR_RUN + 2)).contains("in a row"));
        let opens = |n: usize| format!("SELECT 1 {} x", "/*".repeat(n / 2));
        assert!(check(&opens(MAX_OPERATOR_RUN)).is_ok());
        assert!(refused(&opens(MAX_OPERATOR_RUN + 2)).contains("in a row"));
        let inside = format!("SELECT 1 /* {} */", "+".repeat(MAX_OPERATOR_RUN + 1));
        assert!(refused(&inside).contains("in a row"));
        let glued = format!("SELECT 1 {}/* x */", "@".repeat(MAX_OPERATOR_RUN - 1));
        assert!(refused(&glued).contains("in a row"));
        let apart = format!("SELECT 1{}", ", 1 +-+- 1".repeat(10_000));
        assert!(check(&apart).is_ok());
        let ruled = "=".repeat(MAX_OPERATOR_RUN * 4);
        for quoted in [
            format!("SELECT '{ruled}'"),
            format!("SELECT \"{ruled}\""),
            format!("SELECT $${ruled}$$"),
            format!("SELECT 1 -- {ruled}\n"),
            format!("{}\nSELECT 1", "-".repeat(MAX_OPERATOR_RUN * 4)),
        ] {
            assert!(check(&quoted).is_ok(), "{}", &quoted[..20]);
        }
    }

    #[test]
    fn block_comments_nest_at_most_their_bound() {
        let nested = |n: usize| format!("SELECT 1 {}{}", "/* ".repeat(n), "*/ ".repeat(n));
        assert!(check(&nested(MAX_COMMENT_DEPTH as usize)).is_ok());
        assert!(refused(&nested(MAX_COMMENT_DEPTH as usize + 1)).contains("comments"));
        let apart = "/* a /* b */ c */ SELECT 1;".repeat(2_000);
        assert!(check(&apart).is_ok());
    }

    #[test]
    fn input_past_the_byte_bound_is_refused_before_it_is_read() {
        let largest = format!("SELECT '{}'", "x".repeat(MAX_INPUT_BYTES - 9));
        assert_eq!(largest.len(), MAX_INPUT_BYTES);
        assert!(check(&largest).is_ok());
        let over = format!("{largest} ");
        assert!(refused(&over).contains("bytes"));
    }

    #[test]
    fn refusals_quote_nothing_of_the_input() {
        let sentinel = "zq_sentinel_9f";
        for sql in [
            format!("SELECT {sentinel}{}", "+1".repeat(10_000)),
            format!("SELECT {}{sentinel}", "(".repeat(1_001)),
            format!("SELECT '{sentinel}{}'", "x".repeat(MAX_INPUT_BYTES)),
        ] {
            assert!(!refused(&sql).contains(sentinel));
        }
    }

    #[test]
    fn statements_are_measured_apart() {
        let chain = "+1".repeat(3_000);
        let two = format!("SELECT 1{chain}; SELECT 1{chain}");
        assert!(depth(&two) < MAX_CHAIN_DEPTH);
        let unions = " UNION SELECT 1".repeat(2_000);
        let apart = format!("SELECT 1{unions}; SELECT 1{unions}");
        assert!(depth(&apart) < MAX_CHAIN_DEPTH);
    }
}
