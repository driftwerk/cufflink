use cufflink_platform::wasm_runtime::budget::{
    s3_host_bytes, ChargedJson, Meter, Refused, IMAGE_BYTES_PER_PIXEL,
};
use cufflink_platform::wasm_runtime::image::{decode_oriented, resize_to, Format};
use cufflink_platform::wasm_runtime::outline;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

const HARNESS_SLACK: usize = 4096;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grew(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(ptr, layout, new_size) };
        if !moved.is_null() {
            CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
            grew(new_size);
        }
        moved
    }
}

fn grew(bytes: usize) {
    let now = CURRENT.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

struct Measured<T> {
    value: T,
    peak: usize,
    kept: usize,
}

fn measure<T>(work: impl FnOnce() -> T) -> Measured<T> {
    let base = CURRENT.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let value = work();
    Measured {
        peak: PEAK.load(Ordering::Relaxed).saturating_sub(base),
        kept: CURRENT.load(Ordering::Relaxed).saturating_sub(base),
        value,
    }
}

#[derive(Default)]
struct Count(usize);

impl Meter for Count {
    fn charge(&mut self, bytes: usize) -> Result<(), Refused> {
        self.0 += bytes;
        Ok(())
    }

    fn credit(&mut self, bytes: usize) {
        self.0 = self.0.saturating_sub(bytes);
    }
}

const MIB: usize = 1024 * 1024;

fn shapes() -> Vec<(&'static str, String)> {
    let repeated = |item: &str, n: usize| format!("[{}]", vec![item; n].join(","));
    vec![
        (
            "ordinary",
            repeated(
                r#"{"id":12345,"name":"catalog item","price":19.99,"tags":["a","b"],"active":true}"#,
                200_000,
            ),
        ),
        ("nested arrays", repeated("[0]", 200_000)),
        ("empty objects", repeated("{}", 200_000)),
        ("one-key objects", repeated(r#"{"k":1}"#, 1_000_000)),
        ("escaped strings", repeated(r#""é\n\t\"\\""#, 200_000)),
        (
            "deep nesting",
            repeated(&format!("{}{}", "[".repeat(120), "]".repeat(120)), 10_000),
        ),
        ("one large string", format!("\"{}\"", "x".repeat(8 * MIB))),
    ]
}

fn within_tenth(name: &str, path: &str, charged: usize, allocated: usize) {
    assert!(
        charged as f64 >= allocated as f64 * 0.9,
        "{name}: {path} charged {charged} for {allocated} allocated"
    );
}

// The allocator counts every thread, so the paths are measured one after
// another in one test: no other test, nor its result line, runs meanwhile.
#[test]
fn host_buffers_stay_within_what_they_charge() {
    charged_json_is_charged_within_a_tenth_of_what_it_allocates();
    an_object_read_stays_within_its_declared_multiple();
    an_image_transform_stays_within_its_bytes_per_pixel();
    an_outline_allocates_nothing_its_text_sizes();
}

fn charged_json_is_charged_within_a_tenth_of_what_it_allocates() {
    for (name, text) in shapes() {
        let mut meter = Count::default();
        let parsed = measure(|| ChargedJson::parse(text.as_bytes(), &mut meter).expect(name));
        within_tenth(
            name,
            "parse peak",
            parsed.value.cost,
            parsed.peak + text.len(),
        );
        within_tenth(name, "parse tree", parsed.value.held, parsed.kept);
        assert_eq!(
            meter.0, parsed.value.held,
            "{name}: the charge left is the tree's"
        );

        let tree = parsed.value.value;
        let mut meter = Count::default();
        let written = measure(|| ChargedJson::write(&tree, &mut meter).expect(name));
        within_tenth(name, "write", meter.0, written.peak);
        assert_eq!(written.value.len(), text.len(), "{name}");

        let mut meter = Count::default();
        let rebuilt = measure(|| ChargedJson::to_value(&tree, &mut meter).expect(name));
        within_tenth(name, "to_value tree", rebuilt.value.held, rebuilt.kept);
    }
}

type Unread = (&'static str, String, Option<u16>);

const OUTLINE_BOUND: usize = 256 * 1024;
const DELIVERY_BLOCK_BOUND: usize = 4 * MIB;
const OUTLINE_SIZES: [usize; 2] = [256 * 1024, MIB];
const STRING_UNITS: [&str; 4] = ["x", r#"\""#, "\u{80}", r"\u0080"];

fn unread_strings(unit: &str, bytes: usize) -> Vec<Unread> {
    let long = unit.repeat(bytes / unit.len());
    vec![
        (
            "__status: a string",
            format!(r#"{{"__status":"{long}"}}"#),
            Some(200),
        ),
        (
            "__status: an array",
            format!(r#"{{"__status":["{long}"]}}"#),
            Some(200),
        ),
        (
            "__status: repeated",
            format!(r#"{{"__status":"{long}","__status":500}}"#),
            Some(200),
        ),
        (
            "__status: a string, v2",
            format!(r#"{{"__v":2,"__status":"{long}"}}"#),
            None,
        ),
        (
            "__v: a string",
            format!(r#"{{"__v":"{long}","__status":500}}"#),
            Some(500),
        ),
        (
            "__delivery: a string",
            format!(r#"{{"__v":2,"__status":200,"__delivery":"{long}"}}"#),
            None,
        ),
        (
            "__delivery: a terminal reason",
            format!(r#"{{"__v":2,"__status":200,"__delivery":{{"terminal":"{long}"}}}}"#),
            None,
        ),
        (
            "__delivery: in a v1 answer",
            format!(r#"{{"__status":500,"__delivery":"{long}"}}"#),
            Some(500),
        ),
        (
            "unknown key: a string",
            format!(r#"{{"a":"{long}","__status":500}}"#),
            Some(500),
        ),
        (
            "unknown key: a nested key",
            format!(r#"{{"a":{{"{long}":1}},"__status":500}}"#),
            Some(500),
        ),
        (
            "text that never ends",
            format!(r#"{{"__status":500,"a":"{long}"#),
            Some(200),
        ),
    ]
}

fn unread_shapes(bytes: usize) -> Vec<Unread> {
    let (digits, zeros) = ("1".repeat(bytes), "0".repeat(bytes));
    let (letters, brackets) = ("x".repeat(bytes), "[".repeat(bytes));
    let escaped_key = format!(r#""\n{}":0,"#, "x".repeat(60 * 1024));
    vec![
        (
            "__status: a long integer",
            format!(r#"{{"__status":{digits}}}"#),
            Some(200),
        ),
        (
            "__status: a long fraction",
            format!(r#"{{"__status":0.{digits}}}"#),
            Some(200),
        ),
        (
            "__status: a long exponent",
            format!(r#"{{"__status":1e{zeros}}}"#),
            Some(200),
        ),
        (
            "__v: a long number",
            format!(r#"{{"__v":{digits},"__status":500}}"#),
            Some(500),
        ),
        (
            "unknown key: a long integer",
            format!(r#"{{"a":{digits},"__status":500}}"#),
            Some(500),
        ),
        (
            "unknown key: a long fraction",
            format!(r#"{{"a":0.{digits},"__status":500}}"#),
            Some(500),
        ),
        (
            "key: an escape, then text",
            format!(r#"{{"\n{letters}":1,"__status":500}}"#),
            Some(200),
        ),
        (
            "key: text, then an escape",
            format!(r#"{{"{letters}\n":1,"__status":500}}"#),
            Some(200),
        ),
        (
            "key: text",
            format!(r#"{{"{letters}":1,"__status":500}}"#),
            Some(500),
        ),
        (
            "key: many escaped ones within their limit",
            format!(
                r#"{{{}"__status":500}}"#,
                escaped_key.repeat(bytes / escaped_key.len())
            ),
            Some(500),
        ),
        (
            "key: many short ones",
            format!(r#"{{{}"__status":500}}"#, r#""a":0,"#.repeat(bytes / 6)),
            Some(500),
        ),
        (
            "depth: past a tree's",
            format!(r#"{{"__status":500,"a":{brackets}"#),
            Some(200),
        ),
        (
            "depth: brackets in a key",
            format!(r#"{{"{brackets}":1,"__status":500}}"#),
            Some(500),
        ),
        (
            "__delivery: an array",
            format!(
                r#"{{"__v":2,"__status":200,"__delivery":[{}0]}}"#,
                "0,".repeat(bytes / 2)
            ),
            None,
        ),
    ]
}

fn outline_peak(name: &str, text: &str, status: Option<u16>) -> usize {
    let outlined = measure(|| {
        outline(text.as_bytes())
            .ok()
            .map(|answer| answer.status.as_u16())
    });
    assert_eq!(outlined.value, status, "{name}");
    outlined.peak
}

fn an_outline_does_not_grow_with(rows: impl Fn(usize) -> Vec<Unread>) {
    let [short, long] = OUTLINE_SIZES.map(rows);
    for ((name, small, status), (_, large, _)) in short.into_iter().zip(long) {
        let peaks = [&small, &large].map(|text| outline_peak(name, text, status));
        assert!(
            peaks[1] <= OUTLINE_BOUND && peaks[1] <= peaks[0] + HARNESS_SLACK,
            "{name}: {peaks:?} for {} and {} bytes of text",
            small.len(),
            large.len()
        );
    }
}

fn an_outline_allocates_nothing_its_text_sizes() {
    STRING_UNITS
        .into_iter()
        .for_each(|unit| an_outline_does_not_grow_with(|bytes| unread_strings(unit, bytes)));
    an_outline_does_not_grow_with(unread_shapes);

    let key = format!(r#"{{"\n{}":1,"__status":500}}"#, "x".repeat(60 * 1024));
    let peak = outline_peak("an escaped key within its limit", &key, Some(500));
    assert!(peak <= OUTLINE_BOUND, "an escaped key: {peak}");

    for unit in ["0", "[0]", r#"{"":0}"#] {
        let units = (64 * 1024 - 2) / (unit.len() + 1) - 1;
        let block = format!(
            r#"{{"__v":2,"__status":200,"__delivery":[{}{unit}]}}"#,
            format!("{unit},").repeat(units)
        );
        let peak = outline_peak("a block at its limit", &block, None);
        assert!(
            (block.len()..=DELIVERY_BLOCK_BOUND).contains(&peak),
            "a delivery block of {unit}: {peak}"
        );
    }
}

fn an_object_read_stays_within_its_declared_multiple() {
    use base64::Engine;
    for size in [1, 3 * MIB, 24 * MIB + 7] {
        let read = measure(|| {
            let bytes = vec![7u8; size];
            base64::engine::general_purpose::STANDARD.encode(bytes)
        });
        assert!(
            read.peak <= s3_host_bytes(size as u64) + HARNESS_SLACK,
            "{size}: {} past {}",
            read.peak,
            s3_host_bytes(size as u64)
        );
    }
}

fn an_image_transform_stays_within_its_bytes_per_pixel() {
    use image::{ImageBuffer, Rgb};
    let (width, height) = (3000u32, 2000u32);
    let source: ImageBuffer<Rgb<u8>, Vec<u8>> = ImageBuffer::from_fn(width, height, |x, y| {
        Rgb([(x % 256) as u8, (y % 256) as u8, ((x ^ y) % 256) as u8])
    });
    let mut jpeg = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, 90)
        .encode(&source, width, height, image::ExtendedColorType::Rgb8)
        .expect("jpeg");
    drop(source);
    let allowed = u64::from(width) * u64::from(height) * IMAGE_BYTES_PER_PIXEL;
    for (format, max_dim) in [
        (Format::Jpeg, 0),
        (Format::Jpeg, 1024),
        (Format::Webp, 2048),
        (Format::Avif, 512),
    ] {
        let work = measure(|| {
            let decoded = decode_oriented(&jpeg, image::Limits::default()).expect("decode");
            format
                .encode(&resize_to(&decoded, max_dim), 80)
                .expect("encode")
        });
        assert!(
            work.peak as u64 <= allowed,
            "{format:?} at {max_dim}: {} past {allowed}",
            work.peak
        );
    }
}
