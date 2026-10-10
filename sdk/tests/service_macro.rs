#[test]
fn service_macro_accepts_every_keyword_and_rejects_malformed_input() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/pass/*.rs");
    cases.compile_fail("tests/ui/fail/*.rs");
}
