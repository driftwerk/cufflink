use super::emit::optional;
use crate::keywords::{
    assigned, assigned_choice, assigned_number, choice, once, one_of, strings, trailing, tuples,
};
use cufflink_types::manifest::validate::validate_stream_definition;
use cufflink_types::{DiscardSpec, StreamDefinition, StreamRetention, StreamStorage};
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::ParseStream;
use syn::{Ident, LitStr, Token};

pub(super) struct StreamEntry {
    name: LitStr,
    definition: StreamDefinition,
}

#[derive(Default)]
struct Limits {
    max_age: Option<u64>,
    max_msgs: Option<i64>,
    max_bytes: Option<i64>,
    storage: Option<StreamStorage>,
    duplicate_window: Option<u64>,
    discard: Option<DiscardSpec>,
    description: Option<LitStr>,
}

const KEYWORDS: [&str; 7] = [
    "max_age",
    "max_msgs",
    "max_bytes",
    "storage",
    "duplicate_window",
    "discard",
    "description",
];

const RETENTIONS: [(&str, StreamRetention); 3] = [
    ("limits", StreamRetention::Limits),
    ("work_queue", StreamRetention::WorkQueue),
    ("interest", StreamRetention::Interest),
];

const STORAGES: [(&str, StreamStorage); 2] = [
    ("file", StreamStorage::File),
    ("memory", StreamStorage::Memory),
];

const DISCARDS: [(&str, DiscardSpec); 2] = [("old", DiscardSpec::Old), ("new", DiscardSpec::New)];

/// Parse `[(name, [subject, …], retention[, keyword = value…]), …]`.
pub(super) fn parse_streams(input: ParseStream) -> syn::Result<Vec<StreamEntry>> {
    tuples(input, parse_stream)
}

fn parse_stream(tuple: ParseStream) -> syn::Result<StreamEntry> {
    let name: LitStr = tuple.parse()?;
    tuple.parse::<Token![,]>()?;
    let subjects = strings(tuple)?;
    tuple.parse::<Token![,]>()?;
    let retention = choice(&tuple.parse::<Ident>()?, "stream retention", &RETENTIONS)?;
    let mut limits = Limits::default();
    trailing(tuple, |key, input| limits.apply(key, input))?;
    let definition = StreamDefinition {
        name: name.value(),
        subjects: subjects.iter().map(LitStr::value).collect(),
        retention,
        max_age_secs: limits.max_age,
        max_msgs: limits.max_msgs,
        max_bytes: limits.max_bytes,
        storage: limits.storage.unwrap_or_default(),
        duplicate_window_secs: limits.duplicate_window,
        discard: limits.discard.unwrap_or_default(),
        description: limits.description.as_ref().map(LitStr::value),
    };
    validate_stream_definition("stream", &definition)
        .map_err(|reason| syn::Error::new(name.span(), reason))?;
    Ok(StreamEntry { name, definition })
}

impl Limits {
    fn apply(&mut self, key: &Ident, input: ParseStream) -> syn::Result<()> {
        match key.to_string().as_str() {
            "max_age" => once(&mut self.max_age, key, assigned_number(input)?),
            "max_msgs" => once(&mut self.max_msgs, key, assigned_number(input)?),
            "max_bytes" => once(&mut self.max_bytes, key, assigned_number(input)?),
            "storage" => once(
                &mut self.storage,
                key,
                assigned_choice(input, "stream storage", &STORAGES)?,
            ),
            "duplicate_window" => once(&mut self.duplicate_window, key, assigned_number(input)?),
            "discard" => once(
                &mut self.discard,
                key,
                assigned_choice(input, "discard policy", &DISCARDS)?,
            ),
            "description" => once(&mut self.description, key, assigned(input)?),
            other => Err(syn::Error::new(
                key.span(),
                format!(
                    "unknown stream keyword '{other}'; expected {}",
                    one_of(&KEYWORDS)
                ),
            )),
        }
    }
}

/// Two streams of one service cannot share a name.
pub(super) fn validate_streams(streams: &[StreamEntry]) -> syn::Result<()> {
    streams
        .iter()
        .enumerate()
        .find(|(index, stream)| {
            streams[..*index]
                .iter()
                .any(|earlier| earlier.definition.name == stream.definition.name)
        })
        .map_or(Ok(()), |(_, stream)| {
            Err(syn::Error::new(
                stream.name.span(),
                format!("stream '{}' is declared twice", stream.definition.name),
            ))
        })
}

impl StreamEntry {
    pub(super) fn tokens(&self) -> TokenStream {
        let StreamDefinition {
            name,
            subjects,
            retention,
            max_age_secs,
            max_msgs,
            max_bytes,
            storage,
            duplicate_window_secs,
            discard,
            description,
        } = &self.definition;
        let retention = match retention {
            StreamRetention::Limits => quote! { ::cufflink::manifest::StreamRetention::Limits },
            StreamRetention::WorkQueue => {
                quote! { ::cufflink::manifest::StreamRetention::WorkQueue }
            }
            StreamRetention::Interest => quote! { ::cufflink::manifest::StreamRetention::Interest },
        };
        let storage = match storage {
            StreamStorage::File => quote! { ::cufflink::manifest::StreamStorage::File },
            StreamStorage::Memory => quote! { ::cufflink::manifest::StreamStorage::Memory },
        };
        let discard = match discard {
            DiscardSpec::Old => quote! { ::cufflink::manifest::DiscardSpec::Old },
            DiscardSpec::New => quote! { ::cufflink::manifest::DiscardSpec::New },
        };
        let max_age = optional(max_age_secs.map(|n| quote! { #n }));
        let max_msgs = optional(max_msgs.map(|n| quote! { #n }));
        let max_bytes = optional(max_bytes.map(|n| quote! { #n }));
        let duplicate_window = optional(duplicate_window_secs.map(|n| quote! { #n }));
        let description = optional(
            description
                .as_ref()
                .map(|text| quote! { #text.to_string() }),
        );
        quote! {
            ::cufflink::manifest::StreamDefinition {
                name: #name.to_string(),
                subjects: vec![#(#subjects.to_string()),*],
                retention: #retention,
                max_age_secs: #max_age,
                max_msgs: #max_msgs,
                max_bytes: #max_bytes,
                storage: #storage,
                duplicate_window_secs: #duplicate_window,
                discard: #discard,
                description: #description,
            }
        }
    }
}
