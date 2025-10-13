-- to limit the octet length of tags field of each job to 128K bytes.
ALTER TABLE tapis_jobs.jobs
ADD CONSTRAINT jobs_tags_bytes_ck
CHECK (octet_length(coalesce(tags, ARRAY[]::text[])::text) <= 131072);

-- to limit the octet length of notes field of each job to 128K bytes.
ALTER TABLE tapis_jobs.jobs
ADD CONSTRAINT jobs_notes_bytes_ck
CHECK (octet_length(coalesce(notes, '{}'::jsonb)::text) <= 131072);