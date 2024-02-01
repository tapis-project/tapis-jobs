-- Change DTN field values and names to accommodate new design.
UPDATE TABLE jobs SET dtn_system_id=NULL;
UPDATE TABLE jobs SET dtn_mount_source_path=NULL;
UPDATE TABLE jobs SET dtn_mount_point=NULL;
ALTER TABLE jobs ALTER COLUMN dtn_mount_source_path TYPE character varying(4096);
ALTER TABLE jobs ALTER COLUMN dtn_mount_point TYPE character varying(4096);
ALTER TABLE jobs RENAME COLUMN dtn_mount_source_path TO dtn_system_input_dir;
ALTER TABLE jobs RENAME COLUMN dtn_mount_point TO dtn_system_output_dir;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dtn_in_transaction_id character varying(64);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dtn_in_correlation_id character varying(64);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dtn_out_transaction_id character varying(64);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dtn_out_correlation_id character varying(64);