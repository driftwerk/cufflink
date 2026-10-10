//! The host functions provided by the Cufflink platform WASM runtime.
//! Users never call these directly — use the `db`, `nats`, and `log` modules.

#[link(wasm_import_module = "env")]
extern "C" {
    #[link_name = "cufflink_log"]
    pub(crate) fn cufflink_log(level: i32, msg_ptr: i32, msg_len: i32);
    pub(crate) fn db_query(sql_ptr: i32, sql_len: i32) -> i32;
    pub(crate) fn db_execute(sql_ptr: i32, sql_len: i32) -> i32;
    pub(crate) fn db_execute_all(items_ptr: i32, items_len: i32) -> i32;
    pub(crate) fn db_query_read_only(sql_ptr: i32, sql_len: i32) -> i32;
    pub(crate) fn get_host_response_len() -> i32;
    pub(crate) fn get_host_response(buf_ptr: i32, buf_len: i32) -> i32;
    pub(crate) fn nats_publish(
        subj_ptr: i32,
        subj_len: i32,
        payload_ptr: i32,
        payload_len: i32,
    ) -> i32;
    pub(crate) fn nats_request(
        subj_ptr: i32,
        subj_len: i32,
        payload_ptr: i32,
        payload_len: i32,
        timeout_ms: i32,
    ) -> i32;
    pub(crate) fn http_fetch(
        method_ptr: i32,
        method_len: i32,
        url_ptr: i32,
        url_len: i32,
        headers_ptr: i32,
        headers_len: i32,
        body_ptr: i32,
        body_len: i32,
    ) -> i32;
    pub(crate) fn http_fetch_timeout(
        method_ptr: i32,
        method_len: i32,
        url_ptr: i32,
        url_len: i32,
        headers_ptr: i32,
        headers_len: i32,
        body_ptr: i32,
        body_len: i32,
        timeout_ms: i64,
    ) -> i32;
    pub(crate) fn get_config(key_ptr: i32, key_len: i32) -> i32;
    pub(crate) fn s3_download(bucket_ptr: i32, bucket_len: i32, key_ptr: i32, key_len: i32) -> i32;
    pub(crate) fn s3_download_many(items_ptr: i32, items_len: i32) -> i32;
    pub(crate) fn http_fetch_many(items_ptr: i32, items_len: i32) -> i32;
    pub(crate) fn image_transform_jpeg(
        bucket_ptr: i32,
        bucket_len: i32,
        in_key_ptr: i32,
        in_key_len: i32,
        out_key_ptr: i32,
        out_key_len: i32,
        max_dim: i32,
        quality: i32,
    ) -> i32;
    pub(crate) fn image_transform_webp(
        bucket_ptr: i32,
        bucket_len: i32,
        in_key_ptr: i32,
        in_key_len: i32,
        out_key_ptr: i32,
        out_key_len: i32,
        max_dim: i32,
        quality: i32,
    ) -> i32;
    pub(crate) fn image_transform_avif(
        bucket_ptr: i32,
        bucket_len: i32,
        in_key_ptr: i32,
        in_key_len: i32,
        out_key_ptr: i32,
        out_key_len: i32,
        max_dim: i32,
        quality: i32,
    ) -> i32;
    pub(crate) fn image_transform_multi(
        bucket_ptr: i32,
        bucket_len: i32,
        in_key_ptr: i32,
        in_key_len: i32,
        targets_ptr: i32,
        targets_len: i32,
    ) -> i32;
    pub(crate) fn s3_presign_upload(
        bucket_ptr: i32,
        bucket_len: i32,
        key_ptr: i32,
        key_len: i32,
        content_type_ptr: i32,
        content_type_len: i32,
        expires_secs: i32,
    ) -> i32;
    pub(crate) fn s3_presign_download(
        bucket_ptr: i32,
        bucket_len: i32,
        key_ptr: i32,
        key_len: i32,
        expires_secs: i32,
    ) -> i32;
    pub(crate) fn s3_delete(bucket_ptr: i32, bucket_len: i32, key_ptr: i32, key_len: i32) -> i32;
    pub(crate) fn jobs_enqueue(
        handler_ptr: i32,
        handler_len: i32,
        payload_ptr: i32,
        payload_len: i32,
        timeout_secs: i32,
        max_attempts: i32,
    ) -> i32;
    pub(crate) fn redis_get(key_ptr: i32, key_len: i32) -> i32;
    pub(crate) fn redis_set(
        key_ptr: i32,
        key_len: i32,
        val_ptr: i32,
        val_len: i32,
        ttl_secs: i32,
    ) -> i32;
    pub(crate) fn redis_del(key_ptr: i32, key_len: i32) -> i32;
    pub(crate) fn redis_get_status(key_ptr: i32, key_len: i32) -> i32;
    pub(crate) fn redis_mget(keys_ptr: i32, keys_len: i32) -> i32;
    pub(crate) fn generate_uuid() -> i32;
    pub(crate) fn current_time() -> i32;
    pub(crate) fn context_tenant() -> i32;
}
