use super::docs::{DocKeyword, RouteDocOpts};
use crate::keywords::{assigned_number, assigned_strings, choice, once, one_of};
use cufflink_types::manifest::validate::{validate_audit_redact, validate_job_config};
use cufflink_types::{BackoffSpec, CustomRouteDefinition, JobConfig, RetryOn, SseRouteDef};
use syn::parse::ParseStream;
use syn::{Ident, LitBool, LitInt, LitStr, Token};

pub(super) struct RouteEntry {
    pub(super) method: LitStr,
    pub(super) path: LitStr,
    pub(super) handler: LitStr,
    pub(super) job: Option<JobConfig>,
    pub(super) posture: Option<Posture>,
    pub(super) forward_auth: bool,
    pub(super) body_limit: Option<u64>,
    pub(super) timeout: Option<u32>,
    pub(super) max_concurrent: Option<u32>,
    pub(super) multipart: bool,
    pub(super) sse: Option<SseOpts>,
    pub(super) audit_redact: Option<Vec<LitStr>>,
    pub(super) doc: RouteDocOpts,
}

pub(super) struct SseOpts {
    pub(super) subject: LitStr,
    pub(super) reducer: Option<LitStr>,
    pub(super) anonymous: bool,
}

pub(super) enum Posture {
    Public(LitStr),
    Authenticated,
    Permission(LitStr, LitStr),
    Owner(LitStr),
    Internal(Vec<LitStr>),
    ApiKey(LitStr),
    Scoped {
        area: LitStr,
        operation: LitStr,
        owner: Option<LitStr>,
    },
}

/// A trailing keyword of a route tuple, after `(METHOD, path, handler`.
#[derive(Clone, Copy)]
enum RouteKeyword {
    Job,
    ForwardAuth,
    BodyLimit,
    Timeout,
    MaxConcurrent,
    Multipart,
    Sse,
    AuditRedact,
    Doc(DocKeyword),
    Posture(PostureKeyword),
}

#[derive(Clone, Copy)]
enum PostureKeyword {
    Public,
    Authenticated,
    Permission,
    Owner,
    Internal,
    ApiKey,
    Scoped,
}

impl RouteKeyword {
    fn of(kw: &Ident) -> syn::Result<Self> {
        let keyword = kw.to_string();
        let known = match keyword.as_str() {
            "job" => Some(RouteKeyword::Job),
            "forward_auth" => Some(RouteKeyword::ForwardAuth),
            "body_limit" => Some(RouteKeyword::BodyLimit),
            "timeout" => Some(RouteKeyword::Timeout),
            "max_concurrent" => Some(RouteKeyword::MaxConcurrent),
            "multipart" => Some(RouteKeyword::Multipart),
            "sse" => Some(RouteKeyword::Sse),
            "audit_redact" => Some(RouteKeyword::AuditRedact),
            "public" => Some(RouteKeyword::Posture(PostureKeyword::Public)),
            "auth" => Some(RouteKeyword::Posture(PostureKeyword::Authenticated)),
            "perm" => Some(RouteKeyword::Posture(PostureKeyword::Permission)),
            "owner" => Some(RouteKeyword::Posture(PostureKeyword::Owner)),
            "internal" => Some(RouteKeyword::Posture(PostureKeyword::Internal)),
            "api_key" => Some(RouteKeyword::Posture(PostureKeyword::ApiKey)),
            "scoped" => Some(RouteKeyword::Posture(PostureKeyword::Scoped)),
            other => DocKeyword::of(other).map(RouteKeyword::Doc),
        };
        known.ok_or_else(|| {
            syn::Error::new(
                kw.span(),
                format!(
                    "unknown route modifier '{keyword}'; expected one of \
                     public(\"reason\"), auth, perm(\"area\",\"op\"), \
                     owner(\"field\"), scoped(\"area\",\"op\"[, owner = \"field\"]), \
                     internal(\"service\", ...), api_key(\"scope\"), job(...), forward_auth, \
                     body_limit = N, \
                     timeout = N, max_concurrent = N, multipart, sse(subject = \"...\"), audit_redact = [...], \
                     request = T, \
                     response(s) = ..., query = T, path = T, tags = [...], \
                     operation_id = \"...\", summary = \"...\", description = \"...\", \
                     deprecated, security = [...]"
                ),
            )
        })
    }
}

/// Parse `[(METHOD, path, handler, keyword…), …]`.
pub(super) fn parse_routes(input: ParseStream) -> syn::Result<Vec<RouteEntry>> {
    let content;
    syn::bracketed!(content in input);
    let mut routes = Vec::new();
    while !content.is_empty() {
        let tuple;
        syn::parenthesized!(tuple in content);
        routes.push(parse_route(&tuple)?);
        if content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
        }
    }
    Ok(routes)
}

fn parse_route(tuple: ParseStream) -> syn::Result<RouteEntry> {
    let method: LitStr = tuple.parse()?;
    tuple.parse::<Token![,]>()?;
    let path: LitStr = tuple.parse()?;
    tuple.parse::<Token![,]>()?;
    let handler: LitStr = tuple.parse()?;
    let mut route = RouteEntry {
        method,
        path,
        handler,
        job: None,
        posture: None,
        forward_auth: false,
        body_limit: None,
        timeout: None,
        max_concurrent: None,
        multipart: false,
        sse: None,
        audit_redact: None,
        doc: RouteDocOpts::default(),
    };
    while tuple.peek(Token![,]) {
        tuple.parse::<Token![,]>()?;
        if tuple.is_empty() {
            break;
        }
        let kw: Ident = tuple.parse()?;
        route.apply(RouteKeyword::of(&kw)?, &kw, tuple)?;
    }
    route.validate_limits()?;
    Ok(route)
}

impl RouteEntry {
    /// Record `keyword`, parsing its arguments from `input`. A keyword may
    /// appear once, and a route declares at most one posture.
    fn apply(&mut self, keyword: RouteKeyword, kw: &Ident, input: ParseStream) -> syn::Result<()> {
        let duplicate = |message: &str| Err(syn::Error::new(kw.span(), message));
        match keyword {
            RouteKeyword::Job if self.job.is_some() => {
                duplicate("duplicate `job(...)` modifier on route")
            }
            RouteKeyword::Job => {
                self.job = Some(parse_job_opts(input)?);
                Ok(())
            }
            RouteKeyword::ForwardAuth if self.forward_auth => {
                duplicate("duplicate forward_auth modifier")
            }
            RouteKeyword::ForwardAuth => {
                self.forward_auth = true;
                Ok(())
            }
            RouteKeyword::BodyLimit if self.body_limit.is_some() => {
                duplicate("duplicate body_limit modifier")
            }
            RouteKeyword::BodyLimit => {
                let limit = parse_assigned(input)?;
                let bytes: u64 = limit.base10_parse()?;
                if !(1..=CustomRouteDefinition::MAX_BODY_LIMIT_BYTES).contains(&bytes) {
                    return Err(syn::Error::new(
                        limit.span(),
                        format!(
                            "body_limit must be between 1 and {} bytes",
                            CustomRouteDefinition::MAX_BODY_LIMIT_BYTES
                        ),
                    ));
                }
                self.body_limit = Some(bytes);
                Ok(())
            }
            RouteKeyword::Timeout if self.timeout.is_some() => {
                duplicate("duplicate timeout modifier")
            }
            RouteKeyword::Timeout => {
                let timeout = parse_assigned(input)?;
                let secs: u32 = timeout.base10_parse()?;
                if !CustomRouteDefinition::TIMEOUT_SECS.contains(&secs) {
                    return Err(syn::Error::new(
                        timeout.span(),
                        "timeout must be between 1 and 120 seconds",
                    ));
                }
                self.timeout = Some(secs);
                Ok(())
            }
            RouteKeyword::MaxConcurrent if self.max_concurrent.is_some() => {
                duplicate("duplicate max_concurrent modifier")
            }
            RouteKeyword::MaxConcurrent => {
                let max = parse_assigned(input)?;
                let invocations: u32 = max.base10_parse()?;
                if !CustomRouteDefinition::MAX_CONCURRENT.contains(&invocations) {
                    return Err(syn::Error::new(
                        max.span(),
                        format!(
                            "max_concurrent must be between {} and {}",
                            CustomRouteDefinition::MAX_CONCURRENT.start(),
                            CustomRouteDefinition::MAX_CONCURRENT.end()
                        ),
                    ));
                }
                self.max_concurrent = Some(invocations);
                Ok(())
            }
            RouteKeyword::Multipart if self.multipart => duplicate("duplicate multipart modifier"),
            RouteKeyword::Multipart => {
                self.multipart = true;
                Ok(())
            }
            RouteKeyword::AuditRedact if self.audit_redact.is_some() => {
                duplicate("duplicate audit_redact modifier")
            }
            RouteKeyword::AuditRedact => {
                let rules = assigned_strings(input)?;
                let named: Vec<String> = rules.iter().map(LitStr::value).collect();
                validate_audit_redact("audit_redact", &named)
                    .map_err(|e| syn::Error::new(kw.span(), e))?;
                self.audit_redact = Some(rules);
                Ok(())
            }
            RouteKeyword::Doc(keyword) => self.doc.apply(keyword, kw, input),
            RouteKeyword::Sse if self.sse.is_some() => duplicate("duplicate sse(...) modifier"),
            RouteKeyword::Sse => {
                self.sse = Some(parse_sse(kw, input)?);
                Ok(())
            }
            RouteKeyword::Posture(_) if self.posture.is_some() => {
                duplicate("a route may declare at most one posture")
            }
            RouteKeyword::Posture(_) if self.has_trailing_keyword() => duplicate(
                "the posture comes before body_limit, timeout, max_concurrent, multipart, sse, \
                 audit_redact and documentation keywords, which trail it",
            ),
            RouteKeyword::Posture(posture) => {
                self.posture = Some(parse_posture(posture, input)?);
                Ok(())
            }
        }
    }
}

impl RouteEntry {
    /// Whether a keyword that must follow the posture was given already:
    /// tools read the posture as the tuple's fourth item.
    fn has_trailing_keyword(&self) -> bool {
        self.body_limit.is_some()
            || self.timeout.is_some()
            || self.max_concurrent.is_some()
            || self.multipart
            || self.sse.is_some()
            || self.audit_redact.is_some()
            || !self.doc.is_empty()
    }

    /// Rules between keywords: a job carries its own timeout and runs as a
    /// job, a stream holds no invocation, and only a method with a body can
    /// take a multipart form.
    fn validate_limits(&self) -> syn::Result<()> {
        if self.timeout.is_some() && self.job.is_some() {
            return Err(syn::Error::new(
                self.handler.span(),
                "timeout cannot be combined with job(...); set the job's own timeout",
            ));
        }
        if self.max_concurrent.is_some() && (self.job.is_some() || self.sse.is_some()) {
            return Err(syn::Error::new(
                self.handler.span(),
                "max_concurrent cannot be combined with job(...) or sse(...)",
            ));
        }
        if self.multipart
            && !CustomRouteDefinition::MULTIPART_METHODS.contains(&self.method.value().as_str())
        {
            return Err(syn::Error::new(
                self.method.span(),
                "multipart is only allowed on POST, PUT and PATCH routes",
            ));
        }
        match &self.sse {
            Some(_) if self.method.value() != "GET" => Err(syn::Error::new(
                self.method.span(),
                "sse(...) is only allowed on GET routes",
            )),
            Some(_) if self.job.is_some() => Err(syn::Error::new(
                self.handler.span(),
                "sse(...) cannot be combined with job(...)",
            )),
            _ => Ok(()),
        }
    }
}

/// `sse(subject = "dw.{tenant}.…", reducer = "handler", anonymous = true)`;
/// the reducer and `anonymous` are optional.
fn parse_sse(kw: &Ident, input: ParseStream) -> syn::Result<SseOpts> {
    let inner;
    syn::parenthesized!(inner in input);
    let mut subject: Option<LitStr> = None;
    let mut reducer: Option<LitStr> = None;
    let mut anonymous: Option<LitBool> = None;
    while !inner.is_empty() {
        let key: Ident = inner.parse()?;
        inner.parse::<Token![=]>()?;
        let duplicate = match key.to_string().as_str() {
            "subject" => subject.replace(inner.parse()?).is_some(),
            "reducer" => reducer.replace(inner.parse()?).is_some(),
            "anonymous" => anonymous.replace(inner.parse()?).is_some(),
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!("unknown sse option '{other}'; expected subject, reducer or anonymous"),
                ))
            }
        };
        if duplicate {
            return Err(syn::Error::new(
                key.span(),
                format!("duplicate sse option '{key}'"),
            ));
        }
        if inner.peek(Token![,]) {
            inner.parse::<Token![,]>()?;
        }
    }
    let subject =
        subject.ok_or_else(|| syn::Error::new(kw.span(), "sse(...) requires subject = \"...\""))?;
    if !subject.value().starts_with(SseRouteDef::SUBJECT_PREFIX) {
        return Err(syn::Error::new(
            subject.span(),
            format!(
                "an sse subject must start with {}",
                SseRouteDef::SUBJECT_PREFIX
            ),
        ));
    }
    Ok(SseOpts {
        subject,
        reducer,
        anonymous: anonymous.is_some_and(|flag| flag.value),
    })
}

/// `= <integer>` after a keyword.
fn parse_assigned(input: ParseStream) -> syn::Result<LitInt> {
    input.parse::<Token![=]>()?;
    input.parse()
}

fn parse_posture(keyword: PostureKeyword, input: ParseStream) -> syn::Result<Posture> {
    match keyword {
        PostureKeyword::Authenticated => Ok(Posture::Authenticated),
        PostureKeyword::Public => parse_public(input),
        PostureKeyword::Permission => parse_permission(input),
        PostureKeyword::Owner => parse_owner(input),
        PostureKeyword::Internal => parse_internal(input),
        PostureKeyword::ApiKey => parse_api_key(input),
        PostureKeyword::Scoped => parse_scoped(input),
    }
}

/// `public("reason")`: the reason is mandatory and non-empty.
fn parse_public(input: ParseStream) -> syn::Result<Posture> {
    let inner;
    syn::parenthesized!(inner in input);
    let reason: LitStr = inner.parse()?;
    if reason.value().trim().is_empty() {
        return Err(syn::Error::new(
            reason.span(),
            "public(\"...\") requires a non-empty reason",
        ));
    }
    Ok(Posture::Public(reason))
}

/// `perm("area", "operation")`.
fn parse_permission(input: ParseStream) -> syn::Result<Posture> {
    let inner;
    syn::parenthesized!(inner in input);
    let area: LitStr = inner.parse()?;
    inner.parse::<Token![,]>()?;
    let op: LitStr = inner.parse()?;
    Ok(Posture::Permission(area, op))
}

/// `owner("field")`.
fn parse_owner(input: ParseStream) -> syn::Result<Posture> {
    let inner;
    syn::parenthesized!(inner in input);
    let field: LitStr = inner.parse()?;
    Ok(Posture::Owner(field))
}

/// `internal("service", …)` or `internal("*")`: at least one caller.
fn parse_internal(input: ParseStream) -> syn::Result<Posture> {
    let inner;
    let parens = syn::parenthesized!(inner in input);
    let callers: Vec<LitStr> = inner
        .parse_terminated(|s| s.parse::<LitStr>(), Token![,])?
        .into_iter()
        .collect();
    if callers.is_empty() {
        return Err(syn::Error::new(
            parens.span.join(),
            "internal(...) names the services that may call it, or \"*\"",
        ));
    }
    Ok(Posture::Internal(callers))
}

/// `scoped("area", "operation")` or
/// `scoped("area", "operation", owner = "field")`.
fn parse_scoped(input: ParseStream) -> syn::Result<Posture> {
    let inner;
    syn::parenthesized!(inner in input);
    let area: LitStr = inner.parse()?;
    inner.parse::<Token![,]>()?;
    let operation: LitStr = inner.parse()?;
    let owner = if inner.parse::<Option<Token![,]>>()?.is_some() && !inner.is_empty() {
        let key: Ident = inner.parse()?;
        if key != "owner" {
            return Err(syn::Error::new(
                key.span(),
                format!("unknown scoped option '{key}'; expected owner = \"field\""),
            ));
        }
        inner.parse::<Token![=]>()?;
        Some(inner.parse::<LitStr>()?)
    } else {
        None
    };
    if !inner.is_empty() {
        return Err(inner.error("expected scoped(\"area\", \"op\"[, owner = \"field\"])"));
    }
    Ok(Posture::Scoped {
        area,
        operation,
        owner,
    })
}

/// `api_key("resource:action")`: the partner-key scope the route requires.
fn parse_api_key(input: ParseStream) -> syn::Result<Posture> {
    let inner;
    syn::parenthesized!(inner in input);
    let scope: LitStr = inner.parse()?;
    if scope.value().is_empty() {
        return Err(syn::Error::new(
            scope.span(),
            "api_key(\"...\") requires the scope a partner key must hold",
        ));
    }
    Ok(Posture::ApiKey(scope))
}

const RETRIES: [(&str, RetryOn); 2] = [("error", RetryOn::Error), ("timeout", RetryOn::Timeout)];

const JOB_OPTIONS: [&str; 4] = ["timeout", "max_attempts", "retry_on", "backoff"];

/// `= [error, timeout]`.
fn parse_retry_on(input: ParseStream) -> syn::Result<Vec<RetryOn>> {
    input.parse::<Token![=]>()?;
    let content;
    syn::bracketed!(content in input);
    content
        .parse_terminated(|s| s.parse::<Ident>(), Token![,])?
        .iter()
        .map(|failure| choice(failure, "retry_on failure", &RETRIES))
        .collect()
}

/// `(base = 30, max = 3600)`, either optional.
fn parse_backoff(input: ParseStream) -> syn::Result<BackoffSpec> {
    let inner;
    syn::parenthesized!(inner in input);
    let (mut base, mut max) = (None, None);
    while !inner.is_empty() {
        let key: Ident = inner.parse()?;
        match key.to_string().as_str() {
            "base" => once(&mut base, &key, assigned_number(&inner)?)?,
            "max" => once(&mut max, &key, assigned_number(&inner)?)?,
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!("unknown backoff option '{other}'; expected base or max"),
                ))
            }
        }
        if inner.peek(Token![,]) {
            inner.parse::<Token![,]>()?;
        }
    }
    Ok(BackoffSpec {
        base_secs: base.unwrap_or(BackoffSpec::DEFAULT_BASE_SECS),
        max_secs: max.unwrap_or(BackoffSpec::DEFAULT_MAX_SECS),
    })
}

/// Parse `job(timeout = N, max_attempts = M, retry_on = [error, timeout],
/// backoff(base = N, max = M))`. Every option is optional; a bare `job`
/// keyword (no parens) yields all defaults. The `job` keyword has already
/// been consumed by the caller.
pub(super) fn parse_job_opts(input: ParseStream) -> syn::Result<JobConfig> {
    let mut job = JobConfig::default();
    let (mut retry_on, mut backoff) = (None, None);

    if input.peek(syn::token::Paren) {
        let inner;
        syn::parenthesized!(inner in input);
        while !inner.is_empty() {
            let key: Ident = inner.parse()?;
            match key.to_string().as_str() {
                "timeout" | "timeout_secs" => job.timeout_secs = assigned_number(&inner)?,
                "max_attempts" => job.max_attempts = assigned_number(&inner)?,
                "retry_on" => once(&mut retry_on, &key, parse_retry_on(&inner)?)?,
                "backoff" => once(&mut backoff, &key, parse_backoff(&inner)?)?,
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "unknown job option '{other}'; expected {}",
                            one_of(&JOB_OPTIONS)
                        ),
                    ));
                }
            }
            if inner.peek(Token![,]) {
                inner.parse::<Token![,]>()?;
            }
        }
    }

    if job.timeout_secs == 0 {
        return Err(input.error("job timeout must be > 0"));
    }
    if job.max_attempts == 0 {
        return Err(input.error("job max_attempts must be >= 1"));
    }
    let job = JobConfig {
        retry_on: retry_on.unwrap_or_default(),
        backoff,
        ..job
    };
    validate_job_config("job", &job)
        .map(|()| job)
        .map_err(|reason| input.error(reason))
}
