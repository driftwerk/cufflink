//! Exact Postgres `NUMERIC` values: decimal text in, the binary wire format
//! (base-10000 digits, weight, sign, display scale) out, and back, with
//! `NaN` and the infinities. No precision is lost in either direction.

use bytes::{BufMut, BytesMut};
use std::error::Error;
use std::fmt;
use std::str::FromStr;
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

const SIGN_POSITIVE: u16 = 0x0000;
const SIGN_NEGATIVE: u16 = 0x4000;
const SIGN_NAN: u16 = 0xC000;
const SIGN_INFINITY: u16 = 0xD000;
const SIGN_NEG_INFINITY: u16 = 0xF000;
const DSCALE_MAX: usize = 0x3FFF;
const WEIGHT_MAX: i32 = i16::MAX as i32;
const WEIGHT_MIN: i32 = i16::MIN as i32;
const BASE: i32 = 10_000;
/// The wire format counts base-10000 digits in an `i16`.
const DIGITS_MAX: usize = i16::MAX as usize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgNumeric {
    NaN,
    Infinity,
    NegInfinity,
    Finite {
        negative: bool,
        weight: i16,
        dscale: u16,
        /// Base-10000 digits, without leading or trailing zero digits.
        digits: Vec<i16>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericError(String);

impl fmt::Display for NumericError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for NumericError {}

impl PgNumeric {
    fn zero(dscale: u16) -> Self {
        PgNumeric::Finite {
            negative: false,
            weight: 0,
            dscale,
            digits: Vec::new(),
        }
    }

    /// The value when it is an integer that fits `i64`.
    pub fn to_i64(&self) -> Option<i64> {
        let text = self.to_string();
        let (int, frac) = text.split_once('.').unwrap_or((&text, ""));
        frac.bytes()
            .all(|b| b == b'0')
            .then(|| int.parse().ok())
            .flatten()
    }
}

impl FromStr for PgNumeric {
    type Err = NumericError;

    fn from_str(text: &str) -> Result<Self, NumericError> {
        let invalid = || NumericError(format!("{text:?} is not a decimal number"));
        let trimmed = text.trim();
        match trimmed.to_ascii_lowercase().as_str() {
            "nan" => return Ok(PgNumeric::NaN),
            "infinity" | "+infinity" | "inf" | "+inf" => return Ok(PgNumeric::Infinity),
            "-infinity" | "-inf" => return Ok(PgNumeric::NegInfinity),
            _ => {}
        }
        let (negative, unsigned) = match trimmed.as_bytes().first() {
            Some(b'-') => (true, &trimmed[1..]),
            Some(b'+') => (false, &trimmed[1..]),
            _ => (false, trimmed),
        };
        let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
            Some(at) => (
                &unsigned[..at],
                unsigned[at + 1..].parse::<i32>().map_err(|_| invalid())?,
            ),
            None => (unsigned, 0),
        };
        let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
        if int_part.len() + frac_part.len() == 0 || !all_digits(int_part) || !all_digits(frac_part)
        {
            return Err(invalid());
        }
        let dscale = usize::try_from(
            i64::try_from(frac_part.len()).map_err(|_| invalid())? - i64::from(exponent),
        )
        .unwrap_or(0);
        if dscale > DSCALE_MAX {
            return Err(NumericError(format!(
                "{text:?} has more than {DSCALE_MAX} fractional digits"
            )));
        }
        let dscale = dscale as u16;
        let decimal: Vec<u8> = int_part
            .bytes()
            .chain(frac_part.bytes())
            .map(|b| b - b'0')
            .collect();
        let leading = decimal.iter().take_while(|d| **d == 0).count();
        let significant = &decimal[leading..];
        let trailing = significant.iter().rev().take_while(|d| **d == 0).count();
        let significant = &significant[..significant.len() - trailing];
        if significant.is_empty() {
            return Ok(PgNumeric::zero(dscale));
        }
        let point = i64::try_from(int_part.len()).map_err(|_| invalid())? + i64::from(exponent)
            - i64::try_from(leading).map_err(|_| invalid())?;
        let pad_front = (4 - point.rem_euclid(4)) % 4;
        let weight = (point + pad_front) / 4 - 1;
        if !(i64::from(WEIGHT_MIN)..=i64::from(WEIGHT_MAX)).contains(&weight) {
            return Err(NumericError(format!(
                "{text:?} is outside the numeric range"
            )));
        }
        if (pad_front as usize + significant.len()).div_ceil(4) > DIGITS_MAX {
            return Err(NumericError(format!(
                "{text:?} has more than {} significant digits",
                DIGITS_MAX * 4
            )));
        }
        let padded: Vec<u8> = std::iter::repeat_n(0, pad_front as usize)
            .chain(significant.iter().copied())
            .collect();
        let digits = padded
            .chunks(4)
            .map(|chunk| {
                chunk
                    .iter()
                    .chain(std::iter::repeat(&0))
                    .take(4)
                    .fold(0i16, |acc, d| acc * 10 + i16::from(*d))
            })
            .collect();
        Ok(PgNumeric::Finite {
            negative,
            weight: weight as i16,
            dscale,
            digits,
        })
    }
}

impl fmt::Display for PgNumeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (negative, weight, dscale, digits) = match self {
            PgNumeric::NaN => return f.write_str("NaN"),
            PgNumeric::Infinity => return f.write_str("Infinity"),
            PgNumeric::NegInfinity => return f.write_str("-Infinity"),
            PgNumeric::Finite {
                negative,
                weight,
                dscale,
                digits,
            } => (*negative, i32::from(*weight), usize::from(*dscale), digits),
        };
        let group = |i: i32| {
            usize::try_from(i)
                .ok()
                .and_then(|i| digits.get(i))
                .copied()
                .unwrap_or(0)
        };
        if negative {
            f.write_str("-")?;
        }
        match weight {
            w if w < 0 => f.write_str("0")?,
            w => {
                write!(f, "{}", group(0))?;
                (1..=w).try_for_each(|i| write!(f, "{:04}", group(i)))?;
            }
        }
        if dscale > 0 {
            let fraction: String = (weight + 1..)
                .flat_map(|i| format!("{:04}", group(i)).into_bytes())
                .take(dscale)
                .map(char::from)
                .collect();
            write!(f, ".{fraction}")?;
        }
        Ok(())
    }
}

impl ToSql for PgNumeric {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let (sign, weight, dscale, digits): (u16, i16, u16, &[i16]) = match self {
            PgNumeric::NaN => (SIGN_NAN, 0, 0, &[]),
            PgNumeric::Infinity => (SIGN_INFINITY, 0, 0, &[]),
            PgNumeric::NegInfinity => (SIGN_NEG_INFINITY, 0, 0, &[]),
            PgNumeric::Finite {
                negative,
                weight,
                dscale,
                digits,
            } => (
                if *negative {
                    SIGN_NEGATIVE
                } else {
                    SIGN_POSITIVE
                },
                *weight,
                *dscale,
                digits,
            ),
        };
        out.put_i16(i16::try_from(digits.len())?);
        out.put_i16(weight);
        out.put_u16(sign);
        out.put_u16(dscale);
        digits.iter().for_each(|d| out.put_i16(*d));
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let word = |i: usize| -> Result<[u8; 2], NumericError> {
            raw.get(i * 2..i * 2 + 2)
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| NumericError("truncated numeric value".into()))
        };
        let ndigits = usize::try_from(i16::from_be_bytes(word(0)?))
            .map_err(|_| NumericError("negative numeric digit count".into()))?;
        let weight = i16::from_be_bytes(word(1)?);
        let sign = u16::from_be_bytes(word(2)?);
        let dscale = u16::from_be_bytes(word(3)?);
        let digits = (4..4 + ndigits)
            .map(|i| {
                let digit = i16::from_be_bytes(word(i)?);
                (0..BASE as i16)
                    .contains(&digit)
                    .then_some(digit)
                    .ok_or_else(|| NumericError(format!("numeric digit {digit} out of range")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(match sign {
            SIGN_NAN => PgNumeric::NaN,
            SIGN_INFINITY => PgNumeric::Infinity,
            SIGN_NEG_INFINITY => PgNumeric::NegInfinity,
            SIGN_POSITIVE | SIGN_NEGATIVE => {
                let leading = digits.iter().take_while(|d| **d == 0).count();
                let trailing = digits[leading..]
                    .iter()
                    .rev()
                    .take_while(|d| **d == 0)
                    .count();
                let digits = digits[leading..digits.len() - trailing].to_vec();
                match digits.is_empty() {
                    true => PgNumeric::zero(dscale),
                    false => PgNumeric::Finite {
                        negative: sign == SIGN_NEGATIVE,
                        weight: weight - leading as i16,
                        dscale,
                        digits,
                    },
                }
            }
            other => return Err(NumericError(format!("unknown numeric sign {other:#06x}")).into()),
        })
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric(text: &str) -> PgNumeric {
        text.parse().expect("valid numeric")
    }

    fn wire(value: &PgNumeric) -> Vec<u8> {
        let mut out = BytesMut::new();
        value.to_sql(&Type::NUMERIC, &mut out).expect("encode");
        out.to_vec()
    }

    fn round_trip(text: &str) -> String {
        PgNumeric::from_sql(&Type::NUMERIC, &wire(&numeric(text)))
            .expect("decode")
            .to_string()
    }

    #[test]
    fn vectors_encode_to_the_postgres_layout() {
        let words =
            |words: &[i16]| -> Vec<u8> { words.iter().flat_map(|w| w.to_be_bytes()).collect() };
        assert_eq!(wire(&numeric("0")), words(&[0, 0, 0, 0]));
        assert_eq!(wire(&numeric("123.45")), words(&[2, 0, 0, 2, 123, 4500]));
        assert_eq!(wire(&numeric("-1")), words(&[1, 0, 0x4000, 0, 1]));
        assert_eq!(wire(&numeric("1e-20")), words(&[1, -5, 0, 20, 1]));
        assert_eq!(wire(&numeric("10000")), words(&[1, 1, 0, 0, 1]));
        assert_eq!(wire(&numeric("NaN")), words(&[0, 0, 0xC000u16 as i16, 0]));
        assert_eq!(
            wire(&numeric("Infinity")),
            words(&[0, 0, 0xD000u16 as i16, 0])
        );
        assert_eq!(
            wire(&numeric("-Infinity")),
            words(&[0, 0, 0xF000u16 as i16, 0])
        );
    }

    #[test]
    fn values_survive_a_round_trip_exactly() {
        for (input, canonical) in [
            ("0", "0"),
            ("-0.00", "0.00"),
            ("1e-20", "0.00000000000000000001"),
            (
                "123456789012345678901234567890.123",
                "123456789012345678901234567890.123",
            ),
            ("1.50", "1.50"),
            ("0.001", "0.001"),
            ("-42", "-42"),
            ("+7.0", "7.0"),
            ("1.5e3", "1500"),
            ("12.5E-1", "1.25"),
            ("0000.1000", "0.1000"),
            ("99999999", "99999999"),
            (".5", "0.5"),
            ("5.", "5"),
            ("nan", "NaN"),
            ("-inf", "-Infinity"),
            ("Infinity", "Infinity"),
        ] {
            assert_eq!(round_trip(input), canonical, "{input}");
            assert_eq!(numeric(input).to_string(), canonical, "{input}");
        }
    }

    #[test]
    fn decimal_text_round_trips_for_generated_values() {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..2_000 {
            let int_len = (next() % 25) as usize;
            let frac_len = (next() % 25) as usize;
            let digit = |n: u64| char::from(b'0' + (n % 10) as u8);
            let int: String = (0..int_len).map(|_| digit(next())).collect();
            let frac: String = (0..frac_len).map(|_| digit(next())).collect();
            let sign = if next() % 2 == 0 { "-" } else { "" };
            let int = int.trim_start_matches('0');
            let int = if int.is_empty() { "0" } else { int };
            let text = match frac_len {
                0 => format!("{sign}{int}"),
                _ => format!("{sign}{int}.{frac}"),
            };
            let zero = text
                .trim_start_matches('-')
                .bytes()
                .all(|b| matches!(b, b'0' | b'.'));
            let expected = if zero {
                text.trim_start_matches('-').to_string()
            } else {
                text.clone()
            };
            assert_eq!(round_trip(&text), expected, "{text}");
        }
    }

    #[test]
    fn malformed_text_is_refused() {
        for bad in [
            "",
            "-",
            ".",
            "1.2.3",
            "1e",
            "abc",
            "1,5",
            "e5",
            "1e99999999999",
            "--1",
        ] {
            assert!(bad.parse::<PgNumeric>().is_err(), "{bad:?}");
        }
        assert!(format!("1e-{}", DSCALE_MAX + 1)
            .parse::<PgNumeric>()
            .is_err());
        assert!("1e200000".parse::<PgNumeric>().is_err());
        let widest = "9".repeat(131_068);
        assert!(widest.parse::<PgNumeric>().is_ok());
        let too_many = format!("{widest}.{}", "9".repeat(8));
        assert!(too_many
            .parse::<PgNumeric>()
            .is_err_and(|e| e.to_string().contains("significant digits")));
    }

    #[test]
    fn corrupt_wire_values_are_errors() {
        assert!(PgNumeric::from_sql(&Type::NUMERIC, &[0, 1]).is_err());
        assert!(PgNumeric::from_sql(&Type::NUMERIC, &[0, 1, 0, 0, 0, 0, 0, 0]).is_err());
        assert!(
            PgNumeric::from_sql(&Type::NUMERIC, &[0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x10]).is_err()
        );
        assert!(PgNumeric::from_sql(&Type::NUMERIC, &[0, 0, 0, 0, 0x12, 0x34, 0, 0]).is_err());
    }

    #[test]
    fn integers_are_recognised() {
        assert_eq!(numeric("42").to_i64(), Some(42));
        assert_eq!(numeric("-42.000").to_i64(), Some(-42));
        assert_eq!(numeric("42.5").to_i64(), None);
        assert_eq!(numeric("1e30").to_i64(), None);
        assert_eq!(numeric("NaN").to_i64(), None);
    }
}
