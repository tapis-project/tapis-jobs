-- Move types from the public schema to the tapis_jobs schema
-- NOTE: Tables should have been already moved as part of the DB deployment process.
DO $$
DECLARE t TEXT;
BEGIN FOR t IN
  SELECT t.typname FROM pg_type t
    JOIN  pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typtype = 'e' AND n.nspname = 'public'
    AND t.typname in ('job_event_enum','job_status_enum','job_remote_outcome_enum')
  LOOP
    EXECUTE format('ALTER TYPE public.%s SET SCHEMA tapis_jobs', t);
  END LOOP;
END;$$;
