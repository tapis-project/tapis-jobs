-- Add top level archiveMode attribute to support skipping ARCHIVING phase. Represented in code as an enum.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS archive_mode text NOT NULL DEFAULT 'ALWAYS';
UPDATE jobs SET archive_mode = 'SKIP_ON_FAIL' WHERE archive_on_app_err = 'false';