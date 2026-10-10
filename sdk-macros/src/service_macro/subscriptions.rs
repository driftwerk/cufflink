use super::emit::{optional, optional_string};
use crate::keywords::{
    assigned, assigned_bool, assigned_choice, assigned_number, assigned_numbers, assigned_signed,
    assigned_strings, once, one_of, trailing, tuples,
};
use cufflink_types::manifest::validate::validate_subscription_delivery;
use cufflink_types::{
    AckMode, ConsumerSpec, DeliverPolicySpec, HandlerType, SubscriptionDefinition,
};
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::ParseStream;
use syn::{Ident, LitStr, Token};

pub(super) struct SubscriptionEntry {
    pub(super) subject: LitStr,
    pub(super) handler: LitStr,
    pub(super) handler_type: Ident,
    pub(super) stream: Option<LitStr>,
    pub(super) delivery: Delivery,
}

/// The keywords after a subscription's positional items.
#[derive(Default)]
pub(super) struct Delivery {
    durable: Option<LitStr>,
    ack: Option<AckMode>,
    ack_wait: Option<u32>,
    max_deliver: Option<i64>,
    backoff: Option<Vec<u32>>,
    max_ack_pending: Option<i64>,
    batch: Option<u32>,
    deliver: Option<DeliverPolicySpec>,
    filters: Option<Vec<LitStr>>,
    concurrency: Option<u32>,
    dead_letter: Option<bool>,
    timeout: Option<u32>,
}

const KEYWORDS: [&str; 12] = [
    "durable",
    "ack",
    "ack_wait",
    "max_deliver",
    "backoff",
    "max_ack_pending",
    "batch",
    "deliver",
    "filters",
    "concurrency",
    "dead_letter",
    "timeout",
];

const ACK_MODES: [(&str, AckMode); 2] = [
    ("always", AckMode::Always),
    ("on_success", AckMode::OnSuccess),
];

const DELIVER_POLICIES: [(&str, DeliverPolicySpec); 4] = [
    ("all", DeliverPolicySpec::All),
    ("new", DeliverPolicySpec::New),
    ("last", DeliverPolicySpec::Last),
    ("last_per_subject", DeliverPolicySpec::LastPerSubject),
];

/// Parse `[(subject, handler, handler_type[, stream][, keyword = value…]), …]`.
pub(super) fn parse_subscriptions(input: ParseStream) -> syn::Result<Vec<SubscriptionEntry>> {
    tuples(input, parse_subscription)
}

fn parse_subscription(tuple: ParseStream) -> syn::Result<SubscriptionEntry> {
    let subject: LitStr = tuple.parse()?;
    tuple.parse::<Token![,]>()?;
    let handler: LitStr = tuple.parse()?;
    tuple.parse::<Token![,]>()?;
    let handler_type: Ident = tuple.parse()?;
    let stream = if tuple.peek(Token![,]) && tuple.peek2(LitStr) {
        tuple.parse::<Token![,]>()?;
        Some(tuple.parse::<LitStr>()?)
    } else {
        None
    };
    let mut delivery = Delivery::default();
    trailing(tuple, |key, input| delivery.apply(key, input))?;
    let entry = SubscriptionEntry {
        subject,
        handler,
        handler_type,
        stream,
        delivery,
    };
    entry.validate()?;
    Ok(entry)
}

impl Delivery {
    fn apply(&mut self, key: &Ident, input: ParseStream) -> syn::Result<()> {
        match key.to_string().as_str() {
            "durable" => once(&mut self.durable, key, assigned(input)?),
            "ack" => once(
                &mut self.ack,
                key,
                assigned_choice(input, "ack mode", &ACK_MODES)?,
            ),
            "ack_wait" => once(&mut self.ack_wait, key, assigned_number(input)?),
            "max_deliver" => once(&mut self.max_deliver, key, assigned_signed(input)?.0),
            "backoff" => once(&mut self.backoff, key, assigned_numbers(input)?),
            "max_ack_pending" => once(&mut self.max_ack_pending, key, assigned_number(input)?),
            "batch" => once(&mut self.batch, key, assigned_number(input)?),
            "deliver" => once(
                &mut self.deliver,
                key,
                assigned_choice(input, "deliver policy", &DELIVER_POLICIES)?,
            ),
            "filters" => once(&mut self.filters, key, assigned_strings(input)?),
            "concurrency" => once(&mut self.concurrency, key, assigned_number(input)?),
            "dead_letter" => once(&mut self.dead_letter, key, assigned_bool(input)?),
            "timeout" => once(&mut self.timeout, key, assigned_number(input)?),
            other => Err(syn::Error::new(
                key.span(),
                format!(
                    "unknown subscription keyword '{other}'; expected {}",
                    one_of(&KEYWORDS)
                ),
            )),
        }
    }

    /// The consumer the keywords configure; `None` when they leave it to
    /// the platform.
    fn consumer(&self) -> Option<ConsumerSpec> {
        let spec = ConsumerSpec {
            durable_name: self.durable.as_ref().map(LitStr::value),
            ack_wait_secs: self.ack_wait,
            max_deliver: self.max_deliver,
            backoff_secs: self.backoff.clone().unwrap_or_default(),
            max_ack_pending: self.max_ack_pending,
            batch: self.batch,
            deliver_policy: self.deliver,
            filter_subjects: self.filters.iter().flatten().map(LitStr::value).collect(),
            concurrency: self.concurrency,
            dead_letter: self.dead_letter,
        };
        (spec != ConsumerSpec::default()).then_some(spec)
    }
}

impl SubscriptionEntry {
    fn validate(&self) -> syn::Result<()> {
        let definition = SubscriptionDefinition {
            ack: self.delivery.ack.unwrap_or_default(),
            consumer: self.delivery.consumer(),
            timeout_secs: self.delivery.timeout,
            ..SubscriptionDefinition::new(
                self.subject.value(),
                self.handler.value(),
                HandlerType::Wasm,
            )
        };
        validate_subscription_delivery("subscription", &definition)
            .map_err(|reason| syn::Error::new(self.handler.span(), reason))
    }

    pub(super) fn tokens(&self) -> TokenStream {
        let subject = &self.subject;
        let handler = &self.handler;
        let handler_type = match self.handler_type.to_string().as_str() {
            "webhook" => quote! { ::cufflink::manifest::HandlerType::Webhook },
            "delete_cascade" => quote! { ::cufflink::manifest::HandlerType::DeleteCascade },
            "update_field" => quote! { ::cufflink::manifest::HandlerType::UpdateField },
            _ => quote! { ::cufflink::manifest::HandlerType::Wasm },
        };
        let stream = optional_string(self.stream.as_ref());
        let ack = match self.delivery.ack.unwrap_or_default() {
            AckMode::Always => quote! { ::cufflink::manifest::AckMode::Always },
            AckMode::OnSuccess => quote! { ::cufflink::manifest::AckMode::OnSuccess },
        };
        let consumer = optional(self.delivery.consumer().map(|spec| consumer_tokens(&spec)));
        let timeout = optional(self.delivery.timeout.map(|secs| quote! { #secs }));
        quote! {
            ::cufflink::manifest::SubscriptionDefinition {
                stream: #stream,
                ack: #ack,
                consumer: #consumer,
                timeout_secs: #timeout,
                ..::cufflink::manifest::SubscriptionDefinition::new(#subject, #handler, #handler_type)
            }
        }
    }
}

fn consumer_tokens(spec: &ConsumerSpec) -> TokenStream {
    let durable = optional(
        spec.durable_name
            .as_ref()
            .map(|name| quote! { #name.to_string() }),
    );
    let ack_wait = optional(spec.ack_wait_secs.map(|n| quote! { #n }));
    let max_deliver = optional(spec.max_deliver.map(|n| quote! { #n }));
    let backoff = &spec.backoff_secs;
    let max_ack_pending = optional(spec.max_ack_pending.map(|n| quote! { #n }));
    let batch = optional(spec.batch.map(|n| quote! { #n }));
    let deliver = optional(spec.deliver_policy.map(|policy| match policy {
        DeliverPolicySpec::All => quote! { ::cufflink::manifest::DeliverPolicySpec::All },
        DeliverPolicySpec::New => quote! { ::cufflink::manifest::DeliverPolicySpec::New },
        DeliverPolicySpec::Last => quote! { ::cufflink::manifest::DeliverPolicySpec::Last },
        DeliverPolicySpec::LastPerSubject => {
            quote! { ::cufflink::manifest::DeliverPolicySpec::LastPerSubject }
        }
    }));
    let filters = &spec.filter_subjects;
    let concurrency = optional(spec.concurrency.map(|n| quote! { #n }));
    let dead_letter = optional(spec.dead_letter.map(|flag| quote! { #flag }));
    quote! {
        ::cufflink::manifest::ConsumerSpec {
            durable_name: #durable,
            ack_wait_secs: #ack_wait,
            max_deliver: #max_deliver,
            backoff_secs: vec![#(#backoff),*],
            max_ack_pending: #max_ack_pending,
            batch: #batch,
            deliver_policy: #deliver,
            filter_subjects: vec![#(#filters.to_string()),*],
            concurrency: #concurrency,
            dead_letter: #dead_letter,
        }
    }
}
