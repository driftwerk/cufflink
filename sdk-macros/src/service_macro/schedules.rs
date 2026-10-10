use super::emit::{job_config, optional};
use super::routes::parse_job_opts;
use crate::keywords::{assigned, assigned_choice, assigned_number, once, one_of, trailing, tuples};
use cufflink_types::{CatchUp, JobConfig, Overlap, ScheduleDefinition, ScheduleSpec};
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::ParseStream;
use syn::{Ident, LitInt, LitStr, Token};

pub(super) struct ScheduleEntry {
    name: LitStr,
    definition: ScheduleDefinition,
}

#[derive(Default)]
struct Options {
    timezone: Option<LitStr>,
    overlap: Option<Overlap>,
    catch_up: Option<CatchUp>,
    job: Option<JobConfig>,
    timeout: Option<u32>,
}

const KEYWORDS: [&str; 5] = ["timezone", "overlap", "catch_up", "job", "timeout"];

const OVERLAPS: [(&str, Overlap); 2] = [("skip", Overlap::Skip), ("allow", Overlap::Allow)];

const CATCH_UPS: [(&str, CatchUp); 3] = [
    ("latest", CatchUp::Latest),
    ("none", CatchUp::None),
    ("all", CatchUp::All),
];

/// Parse `[(name, every(secs) | cron("expr"), handler[, keyword…]), …]`.
pub(super) fn parse_schedules(input: ParseStream) -> syn::Result<Vec<ScheduleEntry>> {
    tuples(input, parse_schedule)
}

fn parse_schedule(tuple: ParseStream) -> syn::Result<ScheduleEntry> {
    let name: LitStr = tuple.parse()?;
    tuple.parse::<Token![,]>()?;
    let (spec, spec_span) = parse_spec(tuple)?;
    tuple.parse::<Token![,]>()?;
    let handler: LitStr = tuple.parse()?;
    let mut options = Options::default();
    trailing(tuple, |key, input| options.apply(key, input))?;
    let definition = ScheduleDefinition {
        name: name.value(),
        handler: handler.value(),
        spec,
        timezone: options.timezone.as_ref().map(LitStr::value),
        overlap: options.overlap.unwrap_or_default(),
        catch_up: options.catch_up.unwrap_or_default(),
        job: options.job,
        timeout_secs: options.timeout,
    };
    cufflink_types::schedule::compile("schedule", &definition)
        .map_err(|reason| syn::Error::new(spec_span, reason))?;
    Ok(ScheduleEntry { name, definition })
}

/// `every(30)` or `cron("0 * * * *")`.
fn parse_spec(input: ParseStream) -> syn::Result<(ScheduleSpec, proc_macro2::Span)> {
    let kind: Ident = input.parse()?;
    let inner;
    syn::parenthesized!(inner in input);
    let spec = match kind.to_string().as_str() {
        "every" => ScheduleSpec::Every {
            secs: inner.parse::<LitInt>()?.base10_parse()?,
        },
        "cron" => ScheduleSpec::Cron {
            expr: inner.parse::<LitStr>()?.value(),
        },
        other => {
            return Err(syn::Error::new(
                kind.span(),
                format!("unknown schedule '{other}'; expected every(secs) or cron(\"expr\")"),
            ))
        }
    };
    if inner.is_empty() {
        Ok((spec, kind.span()))
    } else {
        Err(inner.error(format!("{kind}(...) takes one argument")))
    }
}

impl Options {
    fn apply(&mut self, key: &Ident, input: ParseStream) -> syn::Result<()> {
        match key.to_string().as_str() {
            "timezone" => once(&mut self.timezone, key, assigned(input)?),
            "overlap" => once(
                &mut self.overlap,
                key,
                assigned_choice(input, "overlap policy", &OVERLAPS)?,
            ),
            "catch_up" => once(
                &mut self.catch_up,
                key,
                assigned_choice(input, "catch-up policy", &CATCH_UPS)?,
            ),
            "job" => once(&mut self.job, key, parse_job_opts(input)?),
            "timeout" => once(&mut self.timeout, key, assigned_number(input)?),
            other => Err(syn::Error::new(
                key.span(),
                format!(
                    "unknown schedule keyword '{other}'; expected {}",
                    one_of(&KEYWORDS)
                ),
            )),
        }
    }
}

/// Two schedules of one service cannot share a name.
pub(super) fn validate_schedules(schedules: &[ScheduleEntry]) -> syn::Result<()> {
    schedules
        .iter()
        .enumerate()
        .find(|(index, schedule)| {
            schedules[..*index]
                .iter()
                .any(|earlier| earlier.definition.name == schedule.definition.name)
        })
        .map_or(Ok(()), |(_, schedule)| {
            Err(syn::Error::new(
                schedule.name.span(),
                format!("schedule '{}' is declared twice", schedule.definition.name),
            ))
        })
}

impl ScheduleEntry {
    pub(super) fn tokens(&self) -> TokenStream {
        let ScheduleDefinition {
            name,
            handler,
            spec,
            timezone,
            overlap,
            catch_up,
            job,
            timeout_secs,
        } = &self.definition;
        let spec = match spec {
            ScheduleSpec::Cron { expr } => {
                quote! { ::cufflink::manifest::ScheduleSpec::Cron { expr: #expr.to_string() } }
            }
            ScheduleSpec::Every { secs } => {
                quote! { ::cufflink::manifest::ScheduleSpec::Every { secs: #secs } }
            }
        };
        let timezone = optional(timezone.as_ref().map(|zone| quote! { #zone.to_string() }));
        let overlap = match overlap {
            Overlap::Skip => quote! { ::cufflink::manifest::Overlap::Skip },
            Overlap::Allow => quote! { ::cufflink::manifest::Overlap::Allow },
        };
        let catch_up = match catch_up {
            CatchUp::Latest => quote! { ::cufflink::manifest::CatchUp::Latest },
            CatchUp::None => quote! { ::cufflink::manifest::CatchUp::None },
            CatchUp::All => quote! { ::cufflink::manifest::CatchUp::All },
        };
        let job = job_config(job.as_ref());
        let timeout = optional(timeout_secs.map(|secs| quote! { #secs }));
        quote! {
            ::cufflink::manifest::ScheduleDefinition {
                name: #name.to_string(),
                handler: #handler.to_string(),
                spec: #spec,
                timezone: #timezone,
                overlap: #overlap,
                catch_up: #catch_up,
                job: #job,
                timeout_secs: #timeout,
            }
        }
    }
}
