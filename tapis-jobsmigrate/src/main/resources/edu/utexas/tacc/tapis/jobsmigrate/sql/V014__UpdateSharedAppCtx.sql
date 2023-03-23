ALTER TABLE jobs ALTER COLUMN shared_app_ctx TYPE character varying(64);
ALTER TABLE jobs ALTER COLUMN shared_app_ctx SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN shared_app_ctx SET DEFAULT '';
UPDATE jobs SET shared_app_ctx='' WHERE shared_app_ctx='false';
CREATE INDEX jobs_shared_app_ctx_idx ON jobs (shared_app_ctx);