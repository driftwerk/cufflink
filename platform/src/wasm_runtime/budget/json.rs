use super::hold::{Meter, Refused, STEP};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::Serialize;
use serde_json::{Map, Number, Value};
use std::fmt;
use std::io;

const VALUE: usize = std::mem::size_of::<Value>();
const STRING: usize = std::mem::size_of::<String>();
const BTREE_CAPACITY: usize = 11;
const BTREE_MIN: usize = 5;
const LEAF: usize = 16 + BTREE_CAPACITY * (STRING + VALUE);
const INTERNAL: usize = LEAF + (BTREE_CAPACITY + 1) * std::mem::size_of::<usize>();
const SMALL_VEC: usize = 4;
const ENTRY: usize = 192;

#[derive(Debug)]
pub enum ChargedJsonError {
    Syntax(serde_json::Error),
    Capacity(Refused),
}

impl fmt::Display for ChargedJsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax(e) => e.fmt(f),
            Self::Capacity(refused) => refused.fmt(f),
        }
    }
}

impl std::error::Error for ChargedJsonError {}

struct Tally<'m, M: Meter + ?Sized> {
    meter: &'m mut M,
    used: usize,
    charged: usize,
    peak: usize,
    refused: Option<Refused>,
}

impl<'m, M: Meter + ?Sized> Tally<'m, M> {
    fn new(meter: &'m mut M) -> Self {
        Self {
            meter,
            used: 0,
            charged: 0,
            peak: 0,
            refused: None,
        }
    }

    fn add(&mut self, bytes: usize) -> Result<(), Refused> {
        let used = self.used.saturating_add(bytes);
        if used > self.charged {
            let target = used.div_ceil(STEP).saturating_mul(STEP);
            if let Err(refused) = self.meter.charge(target - self.charged) {
                self.refused = Some(refused);
                return Err(refused);
            }
            self.charged = target;
            self.peak = self.peak.max(target);
        }
        self.used = used;
        Ok(())
    }

    fn sub(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes);
        let needed = self.used.div_ceil(STEP).saturating_mul(STEP);
        if let Some(spare) = self.charged.checked_sub(needed).filter(|spare| *spare > 0) {
            self.meter.credit(spare);
            self.charged = needed;
        }
    }

    fn release_all(&mut self) {
        if self.charged > 0 {
            self.meter.credit(self.charged);
        }
        self.charged = 0;
        self.used = 0;
    }

    fn fail<E: de::Error>(&mut self, bytes: usize) -> Result<(), E> {
        self.add(bytes)
            .map_err(|refused| E::custom(format_args!("{refused}")))
    }
}

fn map_cost(entries: usize) -> usize {
    match entries {
        0 => 0,
        1..=BTREE_CAPACITY => LEAF,
        _ => {
            let leaves = entries.div_ceil(BTREE_MIN);
            leaves * LEAF + leaves.div_ceil(BTREE_MIN) * INTERNAL
        }
    }
}

fn next_capacity(capacity: usize) -> usize {
    match capacity {
        0 => SMALL_VEC,
        _ => capacity.saturating_mul(2),
    }
}

struct ValueSeed<'t, 'm, M: Meter + ?Sized>(&'t mut Tally<'m, M>);

impl<'de, M: Meter + ?Sized> DeserializeSeed<'de> for ValueSeed<'_, '_, M> {
    type Value = Value;

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

struct KeySeed<'t, 'm, M: Meter + ?Sized>(&'t mut Tally<'m, M>);

impl<'de, M: Meter + ?Sized> DeserializeSeed<'de> for KeySeed<'_, '_, M> {
    type Value = String;

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<String, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de, M: Meter + ?Sized> Visitor<'de> for KeySeed<'_, '_, M> {
    type Value = String;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a string key")
    }

    fn visit_str<E: de::Error>(self, key: &str) -> Result<String, E> {
        self.0.fail(key.len())?;
        Ok(key.to_owned())
    }

    fn visit_string<E: de::Error>(self, key: String) -> Result<String, E> {
        self.0.fail(key.capacity())?;
        Ok(key)
    }
}

impl<'de, M: Meter + ?Sized> Visitor<'de> for ValueSeed<'_, '_, M> {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Value, E> {
        Ok(Number::from_f64(value).map_or(Value::Null, Value::Number))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        self.0.fail(value.len())?;
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<Value, E> {
        self.0.fail(value.capacity())?;
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let tally = self.0;
        let mut items: Vec<Value> = Vec::new();
        while let Some(item) = seq.next_element_seed(ValueSeed(&mut *tally))? {
            if items.len() == items.capacity() {
                let held = items.capacity();
                let grown = next_capacity(held);
                tally.fail::<A::Error>(grown * VALUE)?;
                items.reserve_exact(grown - items.len());
                tally.sub(held * VALUE);
            }
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let tally = self.0;
        let mut map = Map::new();
        while let Some(key) = access.next_key_seed(KeySeed(&mut *tally))? {
            let entries = map.len();
            let grows = map_cost(entries + 1) - map_cost(entries);
            tally.fail::<A::Error>(grows)?;
            let value = access.next_value_seed(ValueSeed(&mut *tally))?;
            map.insert(key, value);
        }
        Ok(Value::Object(map))
    }
}

struct Sink<'t, 'm, M: Meter + ?Sized> {
    buf: Vec<u8>,
    tally: &'t mut Tally<'m, M>,
}

impl<M: Meter + ?Sized> io::Write for Sink<'_, '_, M> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let needed = self.buf.len().saturating_add(data.len());
        if needed > self.buf.capacity() {
            let capacity = self.buf.capacity();
            let grown = match needed <= STEP {
                true => needed.next_power_of_two().max(64),
                false => needed.div_ceil(STEP).saturating_mul(STEP),
            };
            self.tally
                .add(grown - capacity)
                .map_err(|refused| io::Error::other(refused.to_string()))?;
            self.buf.reserve_exact(grown - self.buf.len());
        }
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Counter(usize);

impl io::Write for Counter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(data.len());
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct Parsed {
    pub value: Value,
    pub cost: usize,
    pub held: usize,
}

pub(crate) fn stepped(bytes: usize) -> usize {
    bytes.div_ceil(STEP).saturating_mul(STEP)
}

pub struct ChargedJson;

impl ChargedJson {
    #[doc(hidden)]
    pub fn parse<M: Meter + ?Sized>(
        input: &[u8],
        meter: &mut M,
    ) -> Result<Parsed, ChargedJsonError> {
        let mut tally = Tally::new(meter);
        let parsed = Self::parse_in(input, &mut tally);
        let cost = tally.peak;
        let charged = tally.charged;
        let tree = tally.used.saturating_sub(input.len());
        match parsed {
            Ok(value) => {
                let held = stepped(tree).min(charged);
                if charged > held {
                    tally.meter.credit(charged - held);
                }
                Ok(Parsed { value, cost, held })
            }
            Err(e) => {
                tally.release_all();
                Err(e)
            }
        }
    }

    fn parse_in<M: Meter + ?Sized>(
        input: &[u8],
        tally: &mut Tally<'_, M>,
    ) -> Result<Value, ChargedJsonError> {
        tally.add(input.len()).map_err(ChargedJsonError::Capacity)?;
        let mut deserializer = serde_json::Deserializer::from_slice(input);
        let parsed = ValueSeed(&mut *tally)
            .deserialize(&mut deserializer)
            .and_then(|value| deserializer.end().map(|()| value));
        match (parsed, tally.refused) {
            (_, Some(refused)) => Err(ChargedJsonError::Capacity(refused)),
            (Ok(value), None) => Ok(value),
            (Err(e), None) => Err(ChargedJsonError::Syntax(e)),
        }
    }

    #[doc(hidden)]
    pub fn write<T: Serialize + ?Sized, M: Meter + ?Sized>(
        value: &T,
        meter: &mut M,
    ) -> Result<String, ChargedJsonError> {
        let mut tally = Tally::new(meter);
        let written = Self::write_in(value, &mut tally);
        if written.is_err() {
            tally.release_all();
        }
        written
    }

    fn write_in<T: Serialize + ?Sized, M: Meter + ?Sized>(
        value: &T,
        tally: &mut Tally<'_, M>,
    ) -> Result<String, ChargedJsonError> {
        let mut sink = Sink {
            buf: Vec::new(),
            tally,
        };
        let written = serde_json::to_writer(&mut sink, value);
        let Sink { buf, tally } = sink;
        match (written, tally.refused) {
            (_, Some(refused)) => Err(ChargedJsonError::Capacity(refused)),
            (Err(e), None) => Err(ChargedJsonError::Syntax(e)),
            (Ok(()), None) => String::from_utf8(buf)
                .map_err(|e| ChargedJsonError::Syntax(serde::ser::Error::custom(e.utf8_error()))),
        }
    }

    #[doc(hidden)]
    pub fn to_value<T: Serialize + ?Sized, M: Meter + ?Sized>(
        value: &T,
        meter: &mut M,
    ) -> Result<Parsed, ChargedJsonError> {
        let text = Self::write(value, meter)?;
        let written = stepped(text.capacity());
        let parsed = Self::parse(text.as_bytes(), meter);
        drop(text);
        meter.credit(written);
        parsed
    }

    pub(crate) fn delivery_bound(text: &[u8]) -> usize {
        let structure: usize = text
            .iter()
            .map(|byte| match byte {
                b'{' => LEAF,
                b'[' => SMALL_VEC * VALUE,
                b',' => ENTRY,
                _ => 0,
            })
            .sum();
        text.len() * 3 + structure + 2 * STEP
    }

    pub fn measure<T: Serialize + ?Sized>(value: &T) -> usize {
        let mut counter = Counter(0);
        match serde_json::to_writer(&mut counter, value) {
            Ok(()) => counter.0,
            Err(_) => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::hold::Capacity;
    use super::super::Reason;
    use super::*;
    use serde_json::json;

    #[test]
    fn the_delivery_bound_covers_a_tree_and_its_render_for_every_shape() {
        let repeated = |item: &str, count: usize| format!("[{}]", vec![item; count].join(","));
        let wide = |keys: usize| {
            let entries: Vec<String> = (0..keys).map(|key| format!("\"{key}\":0")).collect();
            format!("{{{}}}", entries.join(","))
        };
        let quoted = format!("\"{}\"", "x".repeat(STEP));
        let shapes = [
            "0".to_string(),
            "{}".to_string(),
            quoted,
            repeated(r#"{"":0}"#, 100_000),
            repeated("[0]", 100_000),
            repeated("0", 1_000_000),
            repeated(r#"{"":{"":0}}"#, 10_000),
            repeated(r#""a,b,c""#, 10_000),
            wide(11),
            wide(12),
            wide(26),
            wide(100_000),
        ];
        for text in shapes {
            let bound = ChargedJson::delivery_bound(text.as_bytes());
            let mut limit = Limit {
                used: 0,
                limit: usize::MAX,
            };
            let parsed = ChargedJson::parse(text.as_bytes(), &mut limit).expect("parses");
            let needed = parsed.cost + text.len();
            assert!(needed <= bound, "{needed} past {bound}: {:.40}", text);
        }
    }

    #[test]
    fn a_tree_of_small_records_costs_up_to_95_times_its_text() {
        let text = format!("[{}]", vec![r#"{"":0}"#; 100_000].join(","));
        let mut limit = Limit {
            used: 0,
            limit: usize::MAX,
        };
        let parsed = ChargedJson::parse(text.as_bytes(), &mut limit).expect("parses");
        assert!(
            parsed.held > 90 * text.len(),
            "{} for {}",
            parsed.held,
            text.len()
        );
    }

    struct Limit {
        used: usize,
        limit: usize,
    }

    impl Limit {
        fn of(limit: usize) -> Self {
            Self { used: 0, limit }
        }
    }

    impl Meter for Limit {
        fn charge(&mut self, bytes: usize) -> Result<(), Refused> {
            match self.used + bytes > self.limit {
                true => Err(Refused::Budget(Capacity {
                    reason: Reason::Memory,
                    requested: bytes,
                    holding: self.used,
                })),
                false => {
                    self.used += bytes;
                    Ok(())
                }
            }
        }

        fn credit(&mut self, bytes: usize) {
            self.used -= bytes;
        }
    }

    const SAMPLES: &[&str] = &[
        r#"{"b":1,"a":[1,2.5,-3,1e2,1.0,"xé\n",null,true,false],"c":{"z":{},"y":[]}}"#,
        r#"[[0],[0],[0],{},{},{"k":"v"}]"#,
        r#""a string with \"escapes\" and 😀""#,
        r#"{"dup":1,"dup":2}"#,
        r#"18446744073709551615"#,
        r#"-9223372036854775808"#,
        r#"0.30000000000000004"#,
        r#"   {"spaced" : [ 1 , 2 ] }   "#,
    ];

    #[test]
    fn a_charged_parse_is_the_value_serde_json_builds() {
        for sample in SAMPLES {
            let mut meter = Limit::of(usize::MAX);
            let parsed = ChargedJson::parse(sample.as_bytes(), &mut meter).expect(sample);
            let expected: Value = serde_json::from_str(sample).expect(sample);
            assert_eq!(parsed.value, expected, "{sample}");
            assert_eq!(
                serde_json::to_string(&parsed.value).expect("render"),
                serde_json::to_string(&expected).expect("render"),
                "{sample}"
            );
            assert!(parsed.cost >= STEP, "{sample}");
        }
    }

    #[test]
    fn syntax_and_capacity_are_never_confused() {
        for broken in ["{", "[1,", "nul", "{\"a\" 1}", "[] []", ""] {
            let mut meter = Limit::of(usize::MAX);
            assert!(
                matches!(
                    ChargedJson::parse(broken.as_bytes(), &mut meter),
                    Err(ChargedJsonError::Syntax(_))
                ),
                "{broken:?}"
            );
            assert_eq!(meter.used, 0, "a failed parse keeps nothing charged");
        }
        let large = format!("[{}0]", "0,".repeat(200_000));
        let mut meter = Limit::of(2 * STEP);
        assert!(matches!(
            ChargedJson::parse(large.as_bytes(), &mut meter),
            Err(ChargedJsonError::Capacity(Refused::Budget(_)))
        ));
        assert_eq!(meter.used, 0);
        let mut meter = Limit::of(0);
        assert!(matches!(
            ChargedJson::parse(b"{", &mut meter),
            Err(ChargedJsonError::Capacity(_))
        ));
    }

    #[test]
    fn the_scratch_charge_is_returned_and_the_tree_stays_charged() {
        let input = format!("[{}0]", "0,".repeat(300_000));
        let mut meter = Limit::of(usize::MAX);
        let parsed = ChargedJson::parse(input.as_bytes(), &mut meter).expect("parse");
        let tree = 300_001usize.next_power_of_two() * VALUE;
        assert_eq!(meter.used, tree.div_ceil(STEP) * STEP);
        assert!(parsed.cost >= meter.used + input.len());
        drop(parsed);
    }

    #[test]
    fn an_array_of_one_element_arrays_is_charged_what_it_costs() {
        let input = format!("[{}[0]]", "[0],".repeat(100_000));
        let mut meter = Limit::of(usize::MAX);
        ChargedJson::parse(input.as_bytes(), &mut meter).expect("parse");
        let inner = 100_001 * SMALL_VEC * VALUE;
        let outer = 100_001usize.next_power_of_two() * VALUE;
        assert!(
            meter.used >= inner + outer,
            "{} < {}",
            meter.used,
            inner + outer
        );
    }

    #[test]
    fn write_matches_serde_json_and_charges_its_buffer() {
        let value = json!({"b": [1, 2.5, "x"], "a": {"nested": null}});
        let mut meter = Limit::of(usize::MAX);
        let text = ChargedJson::write(&value, &mut meter).expect("write");
        assert_eq!(text, serde_json::to_string(&value).expect("render"));
        assert_eq!(meter.used, STEP);
        let mut meter = Limit::of(0);
        assert!(matches!(
            ChargedJson::write(&value, &mut meter),
            Err(ChargedJsonError::Capacity(_))
        ));
        let large = vec!["x".repeat(1000); 3000];
        let mut meter = Limit::of(usize::MAX);
        let text = ChargedJson::write(&large, &mut meter).expect("write");
        assert_eq!(
            text.len(),
            serde_json::to_string(&large).expect("render").len()
        );
        assert_eq!(meter.used, text.capacity().div_ceil(STEP) * STEP);
        assert!(text.capacity() - text.len() < STEP);
    }

    #[derive(Serialize)]
    struct Request<'a> {
        method: &'a str,
        body: Value,
        body_raw_b64: String,
        fields: Vec<Field<'a>>,
        number: f64,
    }

    #[derive(Serialize)]
    struct Field<'a> {
        name: &'a str,
        data_b64: &'a str,
    }

    #[test]
    fn to_value_is_what_serde_json_to_value_builds() {
        let request = Request {
            method: "POST",
            body: json!({"z": 1, "a": [0.1, 1e300, -0.0]}),
            body_raw_b64: "eyJ6IjoxfQ==".into(),
            fields: vec![Field {
                name: "file",
                data_b64: "AAEC",
            }],
            number: 0.1 + 0.2,
        };
        let mut meter = Limit::of(usize::MAX);
        let charged = ChargedJson::to_value(&request, &mut meter).expect("to_value");
        assert_eq!(
            charged.value,
            serde_json::to_value(&request).expect("to_value")
        );
        assert_eq!(
            meter.used,
            VALUE.max(1).div_ceil(STEP) * STEP,
            "only the tree stays charged"
        );
    }

    #[test]
    fn float_roundtrip_stays_on() {
        let low = 0.1f64;
        let high = f64::from_bits(low.to_bits() + 1);
        for value in [low, high] {
            let text = serde_json::to_string(&value).expect("render");
            assert_eq!(
                serde_json::from_str::<f64>(&text).expect("parse").to_bits(),
                value.to_bits()
            );
        }
    }

    #[test]
    fn measure_counts_what_write_renders() {
        let value = json!({"n": [1e2, 1.0, 100], "s": "é\n"});
        assert_eq!(
            ChargedJson::measure(&value),
            serde_json::to_string(&value).expect("render").len()
        );
    }
}
