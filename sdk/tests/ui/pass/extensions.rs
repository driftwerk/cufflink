cufflink::service! {
    name: "reports-service",
    tables: [],
    mode: wasm,
    custom_routes: [("GET", "/reports/traffic", "traffic", auth)],
    extensions: ["google.token", "pdf.render-v2"],
}
