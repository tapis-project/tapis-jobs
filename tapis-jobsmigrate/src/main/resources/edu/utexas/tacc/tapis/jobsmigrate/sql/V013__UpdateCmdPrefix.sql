-- Increase character limit on cmd_prefix columin in jobs table.
ALTER TABLE jobs ALTER COLUMN cmd_prefix TYPE character varying(1024);