use crate::TestkitError;
use percent_encoding::percent_decode_str;
use regex::Regex;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

pub const TENANTS: &str = "Tenants";

pub fn assets() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../infra/monitoring")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    Prometheus,
    Loki,
    Tempo,
}

impl Source {
    pub const ALL: [Self; 3] = [Self::Prometheus, Self::Loki, Self::Tempo];

    pub fn uid(self) -> &'static str {
        match self {
            Self::Prometheus => "prometheus",
            Self::Loki => "loki",
            Self::Tempo => "tempo",
        }
    }

    fn of(uid: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|source| source.uid() == uid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub source: Source,
    pub text: String,
}

static MATCHER: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"([A-Za-z_][A-Za-z0-9_.]*)\s*(?:=~|!~|!=|=)\s*""#).expect("a pattern")
});
static GROUPING: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:by|without)\s*\(([^)]*)\)").expect("a pattern"));
static LABEL_VALUES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^label_values\((.*),\s*([A-Za-z_]+)\s*\)$").expect("a pattern"));
static METRIC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(?:cufflink|traces)_[a-z0-9_]+").expect("a pattern"));
static SELECTOR: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{[^{}]*\}").expect("a pattern"));
static BRACED_VARIABLE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{[^}]*\}").expect("a pattern"));
static DASHBOARD_LINK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^/d/([A-Za-z0-9_-]+)").expect("a pattern"));

fn captured<'t>(pattern: &'static Regex, text: &'t str) -> impl Iterator<Item = &'t str> {
    pattern
        .captures_iter(text)
        .filter_map(|found| found.get(1))
        .map(|name| name.as_str())
}

impl Query {
    pub fn labels(&self) -> BTreeSet<&str> {
        captured(&MATCHER, &self.text)
            .chain(
                captured(&GROUPING, &self.text)
                    .flat_map(|names| names.split(','))
                    .map(str::trim),
            )
            .chain(self.label_values().map(|(_, label)| label))
            .filter(|name| !name.is_empty())
            .collect()
    }

    pub fn label_values(&self) -> Option<(&str, &str)> {
        let found = LABEL_VALUES.captures(&self.text)?;
        Some((found.get(1)?.as_str(), found.get(2)?.as_str()))
    }

    pub fn selectors(&self) -> Vec<String> {
        SELECTOR
            .find_iter(&BRACED_VARIABLE.replace_all(&self.text, "$$variable"))
            .map(|found| found.as_str().to_string())
            .filter(|selector| MATCHER.is_match(selector))
            .collect()
    }

    pub fn reads_a_bare_metric(&self) -> bool {
        let read = GROUPING.replace_all(&self.text, "");
        METRIC
            .find_iter(&read)
            .any(|metric| !read[metric.end()..].starts_with('{'))
    }

    pub fn metrics(&self) -> BTreeSet<&str> {
        METRIC
            .find_iter(&self.text)
            .map(|found| found.as_str())
            .map(|name| {
                ["_bucket", "_count", "_sum"]
                    .into_iter()
                    .find_map(|suffix| name.strip_suffix(suffix))
                    .unwrap_or(name)
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct Dashboard {
    pub folder: String,
    pub file: String,
    pub json: Value,
}

fn within(json: &Value) -> Box<dyn Iterator<Item = &Value> + '_> {
    let inner: Box<dyn Iterator<Item = &Value>> = match json {
        Value::Array(items) => Box::new(items.iter().flat_map(within)),
        Value::Object(members) => Box::new(members.values().flat_map(within)),
        _ => Box::new(std::iter::empty()),
    };
    Box::new(std::iter::once(json).chain(inner))
}

fn explored(url: &str) -> Option<Vec<Query>> {
    let Some((_, panes)) = url.split_once("panes=") else {
        return Some(Vec::new());
    };
    let panes = panes.split('&').next().unwrap_or(panes);
    let panes = percent_decode_str(panes).decode_utf8().ok()?;
    let panes = serde_json::from_str::<Value>(&panes).ok()?;
    Some(within(&panes).filter_map(asked).collect::<Vec<_>>()).filter(|asked| !asked.is_empty())
}

fn asked(object: &Value) -> Option<Query> {
    let source = Source::of(object["datasource"]["uid"].as_str()?)?;
    let text = object["expr"].as_str().or(object["query"].as_str())?;
    Some(Query {
        source,
        text: text.to_string(),
    })
}

impl Dashboard {
    pub fn uid(&self) -> &str {
        self.json["uid"].as_str().unwrap_or_default()
    }

    pub fn datasources(&self) -> BTreeSet<&str> {
        within(&self.json)
            .filter_map(|value| value["datasource"]["uid"].as_str())
            .collect()
    }

    pub fn links(&self) -> Vec<&str> {
        within(&self.json)
            .filter(|value| value["url"].is_string() && value["title"].is_string())
            .filter_map(|link| link["url"].as_str())
            .collect()
    }

    pub fn linked_dashboards(&self) -> BTreeSet<&str> {
        self.links()
            .into_iter()
            .flat_map(|url| captured(&DASHBOARD_LINK, url))
            .collect()
    }

    pub fn panels(&self) -> Vec<&Value> {
        within(&self.json)
            .filter(|value| value["type"].is_string() && value["gridPos"].is_object())
            .collect()
    }

    pub fn queries(&self) -> Vec<Query> {
        within(&self.json)
            .filter_map(asked)
            .chain(self.links().into_iter().filter_map(explored).flatten())
            .collect()
    }

    pub fn unread(&self) -> Vec<String> {
        let targets = self
            .panels()
            .into_iter()
            .flat_map(|panel| panel["targets"].as_array().into_iter().flatten());
        let variables = self.json["templating"]["list"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|variable| variable["type"] == "query");
        targets
            .chain(variables)
            .filter(|asking| asked(asking).is_none())
            .map(Value::to_string)
            .chain(
                self.links()
                    .into_iter()
                    .filter(|url| explored(url).is_none())
                    .map(str::to_string),
            )
            .collect()
    }
}

pub fn dashboards() -> Result<Vec<Dashboard>, TestkitError> {
    let mut found = Vec::new();
    for folder in std::fs::read_dir(assets().join("dashboards"))? {
        let folder = folder?;
        for file in std::fs::read_dir(folder.path())? {
            let file = file?;
            let json = serde_json::from_slice(&std::fs::read(file.path())?)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            found.push(Dashboard {
                folder: folder.file_name().to_string_lossy().into_owned(),
                file: file.file_name().to_string_lossy().into_owned(),
                json,
            });
        }
    }
    found.sort_by(|a, b| (&a.folder, &a.file).cmp(&(&b.folder, &b.file)));
    Ok(found)
}
