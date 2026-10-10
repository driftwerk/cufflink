-- cufflink:no-transaction
CREATE INDEX CONCURRENTLY loans_kind ON loans (kind);
