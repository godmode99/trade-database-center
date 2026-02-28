-- v1 baseline schema for Trade Database Center (Neon/Postgres)

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS features;
CREATE SCHEMA IF NOT EXISTS normalized;

-- ------------------------------------------------------------
-- Shared helper: keep updated_at_utc in sync
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.set_updated_at_utc()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at_utc = NOW();
  RETURN NEW;
END;
$$;

-- ------------------------------------------------------------
-- Pipeline run tracking
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
  run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  pipeline_name text NOT NULL,
  run_mode text NOT NULL DEFAULT 'scheduled',
  source text,
  source_ref text,
  started_at_utc timestamptz NOT NULL DEFAULT NOW(),
  ended_at_utc timestamptz,
  status text NOT NULL DEFAULT 'running',
  rows_in integer NOT NULL DEFAULT 0 CHECK (rows_in >= 0),
  rows_out integer NOT NULL DEFAULT 0 CHECK (rows_out >= 0),
  rows_error integer NOT NULL DEFAULT 0 CHECK (rows_error >= 0),
  error_message text,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT NOW(),
  updated_at_utc timestamptz NOT NULL DEFAULT NOW(),
  CHECK (status IN ('running', 'success', 'failed', 'partial', 'skipped')),
  CHECK (ended_at_utc IS NULL OR ended_at_utc >= started_at_utc)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name_started
  ON ops.pipeline_runs (pipeline_name, started_at_utc DESC);

CREATE TRIGGER trg_pipeline_runs_updated_at
BEFORE UPDATE ON ops.pipeline_runs
FOR EACH ROW
EXECUTE FUNCTION ops.set_updated_at_utc();

-- ------------------------------------------------------------
-- RAW layer
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.calendar_events (
  id bigserial PRIMARY KEY,
  run_id uuid REFERENCES ops.pipeline_runs(run_id) ON DELETE SET NULL,
  source text NOT NULL,
  source_ref text,
  event_id text NOT NULL,
  event_time_utc timestamptz NOT NULL,
  event_time_bkk timestamptz,
  dateline_epoch bigint,
  currency text,
  country text,
  impact text,
  impact_score smallint,
  event_name text NOT NULL,
  event_name_prefixed text,
  actual text,
  forecast text,
  previous text,
  revision text,
  url text,
  solo_url text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at_utc timestamptz NOT NULL DEFAULT NOW(),
  created_at_utc timestamptz NOT NULL DEFAULT NOW(),
  updated_at_utc timestamptz NOT NULL DEFAULT NOW(),
  CHECK (impact_score IS NULL OR impact_score BETWEEN 1 AND 3)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_calendar_events_business_key
  ON raw.calendar_events (source, event_id, event_time_utc);
CREATE INDEX IF NOT EXISTS idx_calendar_events_ui
  ON raw.calendar_events (event_time_utc DESC, currency, impact);
CREATE INDEX IF NOT EXISTS idx_calendar_events_run_id
  ON raw.calendar_events (run_id);

CREATE TRIGGER trg_calendar_events_updated_at
BEFORE UPDATE ON raw.calendar_events
FOR EACH ROW
EXECUTE FUNCTION ops.set_updated_at_utc();

CREATE TABLE IF NOT EXISTS raw.fred_observations (
  id bigserial PRIMARY KEY,
  run_id uuid REFERENCES ops.pipeline_runs(run_id) ON DELETE SET NULL,
  source text NOT NULL DEFAULT 'fred',
  source_ref text,
  frequency text NOT NULL,
  series_id text NOT NULL,
  observation_date date NOT NULL,
  value numeric(20,8),
  value_text text,
  realtime_start date,
  realtime_end date,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at_utc timestamptz NOT NULL DEFAULT NOW(),
  created_at_utc timestamptz NOT NULL DEFAULT NOW(),
  updated_at_utc timestamptz NOT NULL DEFAULT NOW(),
  CHECK (frequency IN ('daily', 'weekly', 'monthly'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fred_observations_business_key
  ON raw.fred_observations (series_id, frequency, observation_date);
CREATE INDEX IF NOT EXISTS idx_fred_observations_ui
  ON raw.fred_observations (series_id, observation_date DESC);
CREATE INDEX IF NOT EXISTS idx_fred_observations_run_id
  ON raw.fred_observations (run_id);

CREATE TRIGGER trg_fred_observations_updated_at
BEFORE UPDATE ON raw.fred_observations
FOR EACH ROW
EXECUTE FUNCTION ops.set_updated_at_utc();

CREATE TABLE IF NOT EXISTS raw.mt5_ohlcv (
  id bigserial PRIMARY KEY,
  run_id uuid REFERENCES ops.pipeline_runs(run_id) ON DELETE SET NULL,
  source text NOT NULL DEFAULT 'mt5',
  source_ref text,
  symbol text NOT NULL,
  timeframe text NOT NULL,
  broker_time timestamptz,
  bar_time_utc timestamptz NOT NULL,
  open numeric(18,8) NOT NULL,
  high numeric(18,8) NOT NULL,
  low numeric(18,8) NOT NULL,
  close numeric(18,8) NOT NULL,
  tick_volume bigint,
  real_volume bigint,
  spread integer,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at_utc timestamptz NOT NULL DEFAULT NOW(),
  created_at_utc timestamptz NOT NULL DEFAULT NOW(),
  updated_at_utc timestamptz NOT NULL DEFAULT NOW(),
  CHECK (high >= low),
  CHECK (open >= low AND open <= high),
  CHECK (close >= low AND close <= high),
  CHECK (tick_volume IS NULL OR tick_volume >= 0),
  CHECK (real_volume IS NULL OR real_volume >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mt5_ohlcv_business_key
  ON raw.mt5_ohlcv (symbol, timeframe, bar_time_utc);
CREATE INDEX IF NOT EXISTS idx_mt5_ohlcv_ui
  ON raw.mt5_ohlcv (symbol, timeframe, bar_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_mt5_ohlcv_run_id
  ON raw.mt5_ohlcv (run_id);

CREATE TRIGGER trg_mt5_ohlcv_updated_at
BEFORE UPDATE ON raw.mt5_ohlcv
FOR EACH ROW
EXECUTE FUNCTION ops.set_updated_at_utc();

CREATE TABLE IF NOT EXISTS raw.cme_quotes (
  id bigserial PRIMARY KEY,
  run_id uuid REFERENCES ops.pipeline_runs(run_id) ON DELETE SET NULL,
  source text NOT NULL DEFAULT 'cme',
  source_ref text,
  product_code text NOT NULL,
  contract_month text NOT NULL,
  contract_year smallint,
  contract_code text,
  asof_time_utc timestamptz NOT NULL,
  last numeric(18,8),
  change numeric(18,8),
  bid numeric(18,8),
  ask numeric(18,8),
  volume bigint,
  open_interest bigint,
  settlement numeric(18,8),
  source_hash text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at_utc timestamptz NOT NULL DEFAULT NOW(),
  created_at_utc timestamptz NOT NULL DEFAULT NOW(),
  updated_at_utc timestamptz NOT NULL DEFAULT NOW(),
  CHECK (volume IS NULL OR volume >= 0),
  CHECK (open_interest IS NULL OR open_interest >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_cme_quotes_business_key
  ON raw.cme_quotes (product_code, contract_month, asof_time_utc);
CREATE INDEX IF NOT EXISTS idx_cme_quotes_lookup
  ON raw.cme_quotes (product_code, contract_month, asof_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_cme_quotes_run_id
  ON raw.cme_quotes (run_id);

CREATE TRIGGER trg_cme_quotes_updated_at
BEFORE UPDATE ON raw.cme_quotes
FOR EACH ROW
EXECUTE FUNCTION ops.set_updated_at_utc();

CREATE TABLE IF NOT EXISTS raw.cme_probabilities (
  id bigserial PRIMARY KEY,
  run_id uuid REFERENCES ops.pipeline_runs(run_id) ON DELETE SET NULL,
  source text NOT NULL DEFAULT 'cme_fedwatch',
  source_ref text,
  underlying text NOT NULL,
  meeting_date date NOT NULL,
  rate_bin text NOT NULL,
  probability numeric(10,8) NOT NULL,
  current_target_range text,
  expected_rate_mid numeric(10,4),
  asof_time_utc timestamptz NOT NULL,
  source_hash text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at_utc timestamptz NOT NULL DEFAULT NOW(),
  created_at_utc timestamptz NOT NULL DEFAULT NOW(),
  updated_at_utc timestamptz NOT NULL DEFAULT NOW(),
  CHECK (probability >= 0 AND probability <= 1)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_cme_probabilities_business_key
  ON raw.cme_probabilities (underlying, meeting_date, rate_bin, asof_time_utc);
CREATE INDEX IF NOT EXISTS idx_cme_probabilities_lookup
  ON raw.cme_probabilities (underlying, meeting_date, asof_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_cme_probabilities_run_id
  ON raw.cme_probabilities (run_id);

CREATE TRIGGER trg_cme_probabilities_updated_at
BEFORE UPDATE ON raw.cme_probabilities
FOR EACH ROW
EXECUTE FUNCTION ops.set_updated_at_utc();

-- ------------------------------------------------------------
-- FEATURES layer
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features.mt5_price_features (
  id bigserial PRIMARY KEY,
  run_id uuid REFERENCES ops.pipeline_runs(run_id) ON DELETE SET NULL,
  source text NOT NULL DEFAULT 'mt5',
  source_ref text,
  symbol text NOT NULL,
  timeframe text NOT NULL,
  bar_time_utc timestamptz NOT NULL,
  tr numeric(18,8),
  atr14 numeric(18,8),
  range numeric(18,8),
  body numeric(18,8),
  upper_wick numeric(18,8),
  lower_wick numeric(18,8),
  close_pos numeric(10,6),
  swing_high boolean,
  swing_low boolean,
  structure_event text,
  bos_up boolean,
  bos_dn boolean,
  choch_up boolean,
  choch_dn boolean,
  ema20 numeric(18,8),
  ema50 numeric(18,8),
  pdh numeric(18,8),
  pdl numeric(18,8),
  pdc numeric(18,8),
  sweep_prev_high boolean,
  sweep_prev_low boolean,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at_utc timestamptz NOT NULL DEFAULT NOW(),
  created_at_utc timestamptz NOT NULL DEFAULT NOW(),
  updated_at_utc timestamptz NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mt5_price_features_business_key
  ON features.mt5_price_features (symbol, timeframe, bar_time_utc);
CREATE INDEX IF NOT EXISTS idx_mt5_price_features_lookup
  ON features.mt5_price_features (symbol, timeframe, bar_time_utc DESC);

CREATE TRIGGER trg_mt5_price_features_updated_at
BEFORE UPDATE ON features.mt5_price_features
FOR EACH ROW
EXECUTE FUNCTION ops.set_updated_at_utc();

CREATE TABLE IF NOT EXISTS features.calendar_surprise (
  id bigserial PRIMARY KEY,
  run_id uuid REFERENCES ops.pipeline_runs(run_id) ON DELETE SET NULL,
  source text NOT NULL DEFAULT 'calendar',
  source_ref text,
  event_id text NOT NULL,
  event_time_utc timestamptz NOT NULL,
  currency text,
  event_name text,
  actual_value numeric(20,8),
  forecast_value numeric(20,8),
  previous_value numeric(20,8),
  surprise_value numeric(20,8),
  surprise_pct numeric(10,6),
  surprise_zscore numeric(10,6),
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at_utc timestamptz NOT NULL DEFAULT NOW(),
  created_at_utc timestamptz NOT NULL DEFAULT NOW(),
  updated_at_utc timestamptz NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_calendar_surprise_business_key
  ON features.calendar_surprise (event_id, event_time_utc);
CREATE INDEX IF NOT EXISTS idx_calendar_surprise_lookup
  ON features.calendar_surprise (event_time_utc DESC, currency);

CREATE TRIGGER trg_calendar_surprise_updated_at
BEFORE UPDATE ON features.calendar_surprise
FOR EACH ROW
EXECUTE FUNCTION ops.set_updated_at_utc();

-- ------------------------------------------------------------
-- NORMALIZED views for web/API latest lookups
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW normalized.calendar_events_latest AS
SELECT DISTINCT ON (source, event_id)
  source,
  source_ref,
  event_id,
  event_time_utc,
  currency,
  country,
  impact,
  impact_score,
  event_name,
  actual,
  forecast,
  previous,
  revision,
  run_id,
  ingested_at_utc
FROM raw.calendar_events
ORDER BY source, event_id, event_time_utc DESC, ingested_at_utc DESC;

CREATE OR REPLACE VIEW normalized.fred_series_latest AS
SELECT DISTINCT ON (series_id, frequency)
  series_id,
  frequency,
  observation_date,
  value,
  run_id,
  ingested_at_utc
FROM raw.fred_observations
ORDER BY series_id, frequency, observation_date DESC, ingested_at_utc DESC;

CREATE OR REPLACE VIEW normalized.mt5_latest_bar AS
SELECT DISTINCT ON (symbol, timeframe)
  symbol,
  timeframe,
  bar_time_utc,
  open,
  high,
  low,
  close,
  tick_volume,
  run_id,
  ingested_at_utc
FROM raw.mt5_ohlcv
ORDER BY symbol, timeframe, bar_time_utc DESC, ingested_at_utc DESC;

CREATE OR REPLACE VIEW normalized.cme_contract_latest AS
SELECT DISTINCT ON (product_code, contract_month)
  product_code,
  contract_month,
  contract_year,
  contract_code,
  asof_time_utc,
  last,
  change,
  bid,
  ask,
  volume,
  open_interest,
  settlement,
  run_id,
  ingested_at_utc
FROM raw.cme_quotes
ORDER BY product_code, contract_month, asof_time_utc DESC, ingested_at_utc DESC;

CREATE OR REPLACE VIEW normalized.cme_probabilities_latest AS
SELECT DISTINCT ON (underlying, meeting_date, rate_bin)
  underlying,
  meeting_date,
  rate_bin,
  probability,
  current_target_range,
  expected_rate_mid,
  asof_time_utc,
  run_id,
  ingested_at_utc
FROM raw.cme_probabilities
ORDER BY underlying, meeting_date, rate_bin, asof_time_utc DESC, ingested_at_utc DESC;

COMMIT;
