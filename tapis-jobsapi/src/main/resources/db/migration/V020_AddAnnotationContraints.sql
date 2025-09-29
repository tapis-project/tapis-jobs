-- to limit the maximum number of tags to 128 for each job.
ALTER TABLE public.jobs
ADD CONSTRAINT jobs_tags_count_ck
CHECK (coalesce(array_length(tags, 1), 0) <= 128);

-- to limit the octet length of tags field of each job to 128K bytes.
ALTER TABLE public.jobs
ADD CONSTRAINT jobs_tags_bytes_ck
CHECK (octet_length(coalesce(tags, ARRAY[]::text[])::text) <= 131072);

-- to limit the octet length of notes field of each job to 128K bytes.
ALTER TABLE public.jobs
ADD CONSTRAINT jobs_notes_size_ck
CHECK (octet_length(coalesce(notes, '{}'::jsonb)::text) <= 131072);