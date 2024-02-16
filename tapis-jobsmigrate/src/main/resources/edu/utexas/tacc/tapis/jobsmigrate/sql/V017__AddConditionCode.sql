-- Add support for terminal condition codes by adding the condition character field.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS condition character varying(40);