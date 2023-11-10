-- Add support for ZIP runtime type
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS app_asset_transaction_id character varying(64);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS app_asset_correlation_id character varying(64);
