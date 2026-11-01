-- How many times in a row a job has been put off for guest capacity. It
-- survives redelivery and replicas, so the bound on consecutive deferrals
-- holds across them, and any other conclusion of an attempt resets it.
-- Additive only, so an earlier binary runs against this schema.

SET LOCAL lock_timeout = '10s';
ALTER TABLE wasm_jobs ADD COLUMN IF NOT EXISTS capacity_deferrals INTEGER NOT NULL DEFAULT 0;
