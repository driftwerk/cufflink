//! A tree's canonical text, written straight from the value: what
//! `serde_json::to_string(&serde_json::to_value(value))` writes with
//! `preserve_order` off, with every object's keys sorted here rather than
//! by `serde_json::Map`, and without building the `Value`, which costs
//! about thirty times the text.
//!
//! The writer stops at the first byte past its limit, and at the first
//! object or array past its nesting limit, so a tree too large or too deep
//! for its reader is refused before it is written out.

use serde::ser::{self, Impossible, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The text would be longer than its limit.
    TooLong,
    /// Objects and arrays would nest deeper than their limit.
    TooDeep,
    /// The value has a form the canonical text does not take: a map key
    /// that is not a string, or a serializer's own error.
    Unsupported,
}

impl Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::TooLong => "the text is over its limit",
            Self::TooDeep => "the text nests deeper than its limit",
            Self::Unsupported => "the value has no canonical text",
        })
    }
}

impl std::error::Error for Refusal {}

impl ser::Error for Refusal {
    fn custom<T: Display>(_: T) -> Self {
        Self::Unsupported
    }
}

/// `value`'s canonical text, refused past `max_len` bytes or past
/// `max_nesting` levels of objects and arrays.
pub fn to_text(
    value: &impl Serialize,
    max_len: usize,
    max_nesting: usize,
) -> Result<String, Refusal> {
    let mut limits = Limits {
        left: max_len,
        depth: 0,
        max_nesting,
    };
    let mut out = Vec::new();
    value.serialize(&mut Writer {
        out: &mut out,
        limits: &mut limits,
    })?;
    String::from_utf8(out).map_err(|_| Refusal::Unsupported)
}

/// What every writer of one text shares: the bytes still allowed and the
/// nesting now open. Every byte of the text is counted once, where it is
/// first written, so `left` runs out exactly when the text would pass
/// `max_len`.
struct Limits {
    left: usize,
    depth: usize,
    max_nesting: usize,
}

impl Limits {
    fn open(&mut self) -> Result<(), Refusal> {
        self.depth += 1;
        match self.depth > self.max_nesting {
            true => Err(Refusal::TooDeep),
            false => Ok(()),
        }
    }

    fn close(&mut self) {
        self.depth -= 1;
    }
}

struct Writer<'a> {
    out: &'a mut Vec<u8>,
    limits: &'a mut Limits,
}

impl Writer<'_> {
    fn put(&mut self, bytes: &[u8]) -> Result<(), Refusal> {
        self.limits.left = self
            .limits
            .left
            .checked_sub(bytes.len())
            .ok_or(Refusal::TooLong)?;
        self.out.extend_from_slice(bytes);
        Ok(())
    }

    /// A scalar as `serde_json` writes it. It is counted once written, so
    /// a text can pass its limit by at most one scalar before it stops.
    fn scalar(&mut self, value: &impl Serialize) -> Result<(), Refusal> {
        let before = self.out.len();
        serde_json::to_writer(&mut *self.out, value).map_err(|_| Refusal::Unsupported)?;
        self.limits.left = self
            .limits
            .left
            .checked_sub(self.out.len() - before)
            .ok_or(Refusal::TooLong)?;
        Ok(())
    }

    fn child<'b>(&'b mut self, out: &'b mut Vec<u8>) -> Writer<'b> {
        Writer {
            out,
            limits: self.limits,
        }
    }

    fn open(&mut self, bracket: u8) -> Result<(), Refusal> {
        self.limits.open()?;
        self.put(&[bracket])
    }

    fn close(&mut self, bracket: u8) -> Result<(), Refusal> {
        self.limits.close();
        self.put(&[bracket])
    }

    /// `{"variant":` before a variant's content, which `close_variant` ends.
    fn open_variant(&mut self, variant: &str) -> Result<(), Refusal> {
        self.open(b'{')?;
        self.scalar(&variant)?;
        self.put(b":")
    }

    fn close_variant(&mut self) -> Result<(), Refusal> {
        self.close(b'}')
    }

    fn object(&mut self) -> Result<Object<'_>, Refusal> {
        self.open(b'{')?;
        Ok(Object {
            writer: self.child_of_self(),
            entries: Vec::new(),
            key: None,
            variant: false,
        })
    }

    fn array(&mut self) -> Result<Array<'_>, Refusal> {
        self.open(b'[')?;
        Ok(Array {
            writer: self.child_of_self(),
            first: true,
            variant: false,
        })
    }

    fn child_of_self(&mut self) -> Writer<'_> {
        Writer {
            out: self.out,
            limits: self.limits,
        }
    }
}

/// An object's entries, each key and value written to buffers of their
/// own and put in order of their keys when the object ends, as
/// `serde_json::Map` orders them. A byte is counted where it is first
/// written, so moving an entry into place costs nothing more.
struct Object<'a> {
    writer: Writer<'a>,
    entries: Vec<Entry>,
    key: Option<String>,
    variant: bool,
}

struct Entry {
    key: String,
    key_text: Vec<u8>,
    value: Vec<u8>,
}

impl Object<'_> {
    fn entry(&mut self, key: String, value: &(impl Serialize + ?Sized)) -> Result<(), Refusal> {
        let mut key_text = Vec::new();
        self.writer.child(&mut key_text).scalar(&key)?;
        let mut text = Vec::new();
        value.serialize(&mut self.writer.child(&mut text))?;
        self.entries.push(Entry {
            key,
            key_text,
            value: text,
        });
        Ok(())
    }

    fn end(mut self) -> Result<(), Refusal> {
        self.entries.sort_by(|a, b| a.key.cmp(&b.key));
        if self
            .entries
            .windows(2)
            .any(|pair| pair[0].key == pair[1].key)
        {
            return Err(Refusal::Unsupported);
        }
        for (at, entry) in self.entries.iter().enumerate() {
            if at > 0 {
                self.writer.put(b",")?;
            }
            self.writer.out.extend_from_slice(&entry.key_text);
            self.writer.put(b":")?;
            self.writer.out.extend_from_slice(&entry.value);
        }
        self.writer.close(b'}')?;
        match self.variant {
            true => self.writer.close_variant(),
            false => Ok(()),
        }
    }
}

struct Array<'a> {
    writer: Writer<'a>,
    first: bool,
    variant: bool,
}

impl Array<'_> {
    fn element(&mut self, value: &(impl Serialize + ?Sized)) -> Result<(), Refusal> {
        if !std::mem::replace(&mut self.first, false) {
            self.writer.put(b",")?;
        }
        value.serialize(&mut self.writer.child_of_self())
    }

    fn end(mut self) -> Result<(), Refusal> {
        self.writer.close(b']')?;
        match self.variant {
            true => self.writer.close_variant(),
            false => Ok(()),
        }
    }
}

impl<'a, 'b> ser::Serializer for &'b mut Writer<'a> {
    type Ok = ();
    type Error = Refusal;
    type SerializeSeq = Array<'b>;
    type SerializeTuple = Array<'b>;
    type SerializeTupleStruct = Array<'b>;
    type SerializeTupleVariant = Array<'b>;
    type SerializeMap = Object<'b>;
    type SerializeStruct = Object<'b>;
    type SerializeStructVariant = Object<'b>;

    fn serialize_bool(self, v: bool) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_i8(self, v: i8) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_i16(self, v: i16) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_i32(self, v: i32) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_i64(self, v: i64) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_i128(self, v: i128) -> Result<(), Refusal> {
        self.scalar(&serde_json::to_value(v).map_err(|_| Refusal::Unsupported)?)
    }

    fn serialize_u8(self, v: u8) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_u16(self, v: u16) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_u32(self, v: u32) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_u64(self, v: u64) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_u128(self, v: u128) -> Result<(), Refusal> {
        self.scalar(&serde_json::to_value(v).map_err(|_| Refusal::Unsupported)?)
    }

    /// As a `Value` writes it, which differs from `serde_json`'s own
    /// writer for an `f32`.
    fn serialize_f32(self, v: f32) -> Result<(), Refusal> {
        self.scalar(&serde_json::Value::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<(), Refusal> {
        self.scalar(&serde_json::Value::from(v))
    }

    fn serialize_char(self, v: char) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    fn serialize_str(self, v: &str) -> Result<(), Refusal> {
        self.scalar(&v)
    }

    /// As a `Value`: an array of numbers.
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Refusal> {
        let mut array = self.array()?;
        v.iter().try_for_each(|byte| array.element(byte))?;
        array.end()
    }

    fn serialize_none(self) -> Result<(), Refusal> {
        self.put(b"null")
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Refusal> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), Refusal> {
        self.put(b"null")
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Refusal> {
        self.put(b"null")
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<(), Refusal> {
        self.scalar(&variant)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Refusal> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Refusal> {
        self.open_variant(variant)?;
        value.serialize(&mut *self)?;
        self.close_variant()
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Array<'b>, Refusal> {
        self.array()
    }

    fn serialize_tuple(self, _: usize) -> Result<Array<'b>, Refusal> {
        self.array()
    }

    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Array<'b>, Refusal> {
        self.array()
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Array<'b>, Refusal> {
        self.open_variant(variant)?;
        let mut array = self.array()?;
        array.variant = true;
        Ok(array)
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Object<'b>, Refusal> {
        self.object()
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Object<'b>, Refusal> {
        self.object()
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Object<'b>, Refusal> {
        self.open_variant(variant)?;
        let mut object = self.object()?;
        object.variant = true;
        Ok(object)
    }
}

macro_rules! array_parts {
    ($($trait:ident :: $method:ident),*) => {$(
        impl ser::$trait for Array<'_> {
            type Ok = ();
            type Error = Refusal;

            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Refusal> {
                self.element(value)
            }

            fn end(self) -> Result<(), Refusal> {
                Array::end(self)
            }
        }
    )*};
}

array_parts!(
    SerializeSeq::serialize_element,
    SerializeTuple::serialize_element,
    SerializeTupleStruct::serialize_field,
    SerializeTupleVariant::serialize_field
);

impl ser::SerializeStruct for Object<'_> {
    type Ok = ();
    type Error = Refusal;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Refusal> {
        self.entry(key.to_string(), value)
    }

    fn end(self) -> Result<(), Refusal> {
        Object::end(self)
    }
}

impl ser::SerializeStructVariant for Object<'_> {
    type Ok = ();
    type Error = Refusal;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Refusal> {
        self.entry(key.to_string(), value)
    }

    fn end(self) -> Result<(), Refusal> {
        Object::end(self)
    }
}

impl ser::SerializeMap for Object<'_> {
    type Ok = ();
    type Error = Refusal;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Refusal> {
        self.key = Some(key.serialize(KeyText)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Refusal> {
        let key = self.key.take().ok_or(Refusal::Unsupported)?;
        self.entry(key, value)
    }

    fn end(self) -> Result<(), Refusal> {
        Object::end(self)
    }
}

/// A map key, which the canonical text takes only as a string.
struct KeyText;

impl ser::Serializer for KeyText {
    type Ok = String;
    type Error = Refusal;
    type SerializeSeq = Impossible<String, Refusal>;
    type SerializeTuple = Impossible<String, Refusal>;
    type SerializeTupleStruct = Impossible<String, Refusal>;
    type SerializeTupleVariant = Impossible<String, Refusal>;
    type SerializeMap = Impossible<String, Refusal>;
    type SerializeStruct = Impossible<String, Refusal>;
    type SerializeStructVariant = Impossible<String, Refusal>;

    fn serialize_str(self, v: &str) -> Result<String, Refusal> {
        Ok(v.to_string())
    }

    fn serialize_bool(self, _: bool) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_i8(self, _: i8) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_i16(self, _: i16) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_i32(self, _: i32) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_i64(self, _: i64) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_u8(self, _: u8) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_u16(self, _: u16) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_u32(self, _: u32) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_u64(self, _: u64) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_f32(self, _: f32) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_f64(self, _: f64) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_char(self, _: char) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_none(self) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_unit(self) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &T,
    ) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<String, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Refusal> {
        Err(Refusal::Unsupported)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Refusal> {
        Err(Refusal::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;
    use std::collections::{BTreeMap, HashMap};

    #[derive(Serialize)]
    enum Shape {
        Unit,
        Newtype(Box<Inner>),
        Tuple(i32, String),
        Struct { zeta: bool, alpha: Option<u8> },
    }

    #[derive(Serialize)]
    struct Inner {
        zebra: Vec<Shape>,
        apple: f64,
        mango: f32,
        bytes: serde_bytes_like::Bytes,
        nothing: Option<i64>,
        unit: (),
        text: String,
        letter: char,
        map: HashMap<String, i32>,
        big: u64,
        small: i64,
    }

    mod serde_bytes_like {
        pub struct Bytes(pub Vec<u8>);

        impl serde::Serialize for Bytes {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_bytes(&self.0)
            }
        }
    }

    fn sample() -> Shape {
        Shape::Newtype(Box::new(Inner {
            zebra: vec![
                Shape::Unit,
                Shape::Tuple(-3, "t\u{e9}\"\\\n\u{1}".into()),
                Shape::Struct {
                    zeta: true,
                    alpha: None,
                },
                Shape::Struct {
                    zeta: false,
                    alpha: Some(7),
                },
            ],
            apple: 1.5e300,
            mango: 0.1,
            bytes: serde_bytes_like::Bytes(vec![0, 255]),
            nothing: None,
            unit: (),
            text: "\u{1f980} \u{2028}".into(),
            letter: '\'',
            map: HashMap::from([("b".into(), 2), ("a".into(), 1), ("ab".into(), 3)]),
            big: u64::MAX,
            small: i64::MIN,
        }))
    }

    fn by_value(value: &impl Serialize) -> String {
        serde_json::to_string(&serde_json::to_value(value).expect("value")).expect("text")
    }

    #[test]
    fn the_text_is_what_a_value_writes() {
        let value = sample();
        assert_eq!(
            to_text(&value, usize::MAX, usize::MAX),
            Ok(by_value(&value))
        );
        for odd in [f64::NAN, f64::INFINITY, -0.0, 1e-7] {
            assert_eq!(to_text(&odd, usize::MAX, usize::MAX), Ok(by_value(&odd)));
        }
        let empty: (Vec<u8>, BTreeMap<String, ()>) = (Vec::new(), BTreeMap::new());
        assert_eq!(
            to_text(&empty, usize::MAX, usize::MAX),
            Ok(by_value(&empty))
        );
    }

    #[test]
    fn a_text_past_its_length_stops() {
        let text = by_value(&sample());
        assert_eq!(to_text(&sample(), text.len(), usize::MAX), Ok(text.clone()));
        for short in [0, 1, text.len() / 2, text.len() - 1] {
            assert_eq!(
                to_text(&sample(), short, usize::MAX),
                Err(Refusal::TooLong),
                "{short}"
            );
        }
    }

    #[test]
    fn a_text_past_its_nesting_stops() {
        let nested = |levels: usize| {
            (0..levels).fold(serde_json::json!(1), |inner, at| match at % 2 {
                0 => serde_json::json!([inner]),
                _ => serde_json::json!({ "k": inner }),
            })
        };
        assert!(to_text(&nested(5), usize::MAX, 5).is_ok());
        assert_eq!(to_text(&nested(6), usize::MAX, 5), Err(Refusal::TooDeep));
        assert_eq!(
            to_text(&sample(), usize::MAX, 3),
            Err(Refusal::TooDeep),
            "a variant is an object of its own"
        );
    }

    #[test]
    fn only_string_keys_are_written() {
        let keyed = BTreeMap::from([(1, 2)]);
        assert_eq!(
            to_text(&keyed, usize::MAX, usize::MAX),
            Err(Refusal::Unsupported)
        );
    }
}
