-- Add support for auditing by adding the tracking id character field.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS tracking_id character varying(126);
CREATE INDEX job_tracking_id_idx ON jobs (tracking_id);