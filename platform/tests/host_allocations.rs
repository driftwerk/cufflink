use cufflink_platform::wasm_runtime::budget::{
    s3_host_bytes, ChargedJson, Meter, Refused, IMAGE_BYTES_PER_PIXEL,
};
use cufflink_platform::wasm_runtime::image::{decode_oriented, resize_to, Format};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

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
    let n = 200_000;
    let repeated = |item: &str| format!("[{}]", vec![item; n].join(","));
    vec![
        ("ordinary", {
            let row = r#"{"id":12345,"name":"catalog item","price":19.99,"tags":["a","b"],"active":true}"#;
            repeated(row)
        }),
        ("nested arrays", repeated("[0]")),
        ("empty objects", repeated("{}")),
        ("one-key objects", repeated(r#"{"k":1}"#)),
        ("escaped strings", repeated(r#""é\n\t\"\\""#)),
        ("deep nesting", {
            let depth = 120;
            let inner = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
            repeated(&inner)
        }),
        ("one large string", format!("\"{}\"", "x".repeat(8 * MIB))),
    ]
}

fn within_tenth(name: &str, path: &str, charged: usize, allocated: usize) {
    assert!(
        charged as f64 >= allocated as f64 * 0.9,
        "{name}: {path} charged {charged} for {allocated} allocated"
    );
}

#[test]
fn charged_json_is_charged_within_a_tenth_of_what_it_allocates() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
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

#[test]
fn an_object_read_stays_within_its_declared_multiple() {
    use base64::Engine;
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
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

#[test]
fn an_image_transform_stays_within_its_bytes_per_pixel() {
    use image::{ImageBuffer, Rgb};
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
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
