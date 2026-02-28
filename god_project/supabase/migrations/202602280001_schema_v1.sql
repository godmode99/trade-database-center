-- Trade Database Center - Supabase schema v1
-- Phase 1 Foundation: core schemas/tables/indexes/views

begin;

create schema if not exists ops;
create schema if not exists raw;
create schema if not exists features;
create schema if not exists normalized;

create or replace function ops.set_updated_at_utc()
returns trigger
language plpgsql
as $$
begin
  new.updated_at_utc = now();
  return new;
end;
$$;

create table if not exists ops.pipeline_runs (
  run_id text primary key,
  pipeline_name text not null,
  run_mode text not null check (run_mode in ('scheduled', 'manual', 'backfill', 'adhoc')),
  status text not null check (status in ('running', 'success', 'failed', 'cancelled')),
  started_at_utc timestamptz not null default now(),
  ended_at_utc timestamptz,
  trigger_ref text,
  source_ref text,
  rows_read integer,
  rows_written integer,
  error_message text,
  metadata jsonb not null default '{}'::jsonb,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create index if not exists idx_pipeline_runs_name_started
  on ops.pipeline_runs (pipeline_name, started_at_utc desc);
create index if not exists idx_pipeline_runs_status_started
  on ops.pipeline_runs (status, started_at_utc desc);

create table if not exists raw.calendar_events (
  id bigint generated always as identity primary key,
  source text not null default 'calendar',
  source_ref text,
  run_id text references ops.pipeline_runs(run_id) on delete set null,
  event_id text,
  event_time_utc timestamptz not null,
  event_time_bkk timestamptz,
  dateline_epoch bigint,
  currency text,
  country text,
  impact text,
  impact_score smallint,
  event_name text not null,
  event_name_prefixed text,
  actual text,
  forecast text,
  previous text,
  revision text,
  url text,
  solo_url text,
  payload jsonb not null default '{}'::jsonb,
  ingested_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create unique index if not exists uq_calendar_events_natural_key
  on raw.calendar_events (source, coalesce(event_id, event_name), event_time_utc);

create index if not exists idx_calendar_events_ui
  on raw.calendar_events (event_time_utc desc, currency, impact);
create index if not exists idx_calendar_events_name
  on raw.calendar_events (event_name);

create table if not exists raw.fred_observations (
  id bigint generated always as identity primary key,
  source text not null default 'fred',
  source_ref text,
  run_id text references ops.pipeline_runs(run_id) on delete set null,
  frequency text not null check (frequency in ('daily', 'weekly', 'monthly')),
  series_id text not null,
  observation_date date not null,
  value_numeric numeric,
  value_text text,
  payload jsonb not null default '{}'::jsonb,
  ingested_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  unique (source, series_id, observation_date)
);

create index if not exists idx_fred_series_date
  on raw.fred_observations (series_id, observation_date desc);

create table if not exists raw.mt5_ohlcv (
  id bigint generated always as identity primary key,
  source text not null default 'mt5',
  source_ref text,
  run_id text references ops.pipeline_runs(run_id) on delete set null,
  symbol text not null,
  timeframe text not null check (timeframe in ('m15', 'h4', 'd1', 'w1', 'm1')),
  bar_time_utc timestamptz not null,
  open numeric(18,8) not null,
  high numeric(18,8) not null,
  low numeric(18,8) not null,
  close numeric(18,8) not null,
  tick_volume bigint,
  payload jsonb not null default '{}'::jsonb,
  ingested_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  unique (source, symbol, timeframe, bar_time_utc)
);

create index if not exists idx_mt5_symbol_tf_time
  on raw.mt5_ohlcv (symbol, timeframe, bar_time_utc desc);

create table if not exists raw.cme_quotes (
  id bigint generated always as identity primary key,
  source text not null default 'cme_fedwatch',
  source_ref text,
  run_id text references ops.pipeline_runs(run_id) on delete set null,
  frequency text not null check (frequency in ('daily', 'weekly', 'monthly')),
  snapshot_time_utc timestamptz not null,
  name text,
  code text not null,
  expiry_date date,
  front_month boolean,
  last_price numeric,
  change_value numeric,
  high_value numeric,
  low_value numeric,
  open_value numeric,
  volume bigint,
  payload jsonb not null default '{}'::jsonb,
  ingested_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  check (high_value is null or low_value is null or high_value >= low_value)
);

create unique index if not exists uq_cme_quotes_natural_key
  on raw.cme_quotes (
    source,
    frequency,
    code,
    coalesce(expiry_date, '0001-01-01'::date),
    snapshot_time_utc
  );

create index if not exists idx_cme_quotes_ui
  on raw.cme_quotes (frequency, snapshot_time_utc desc, code);

create table if not exists raw.cme_probabilities (
  id bigint generated always as identity primary key,
  source text not null default 'cme_fedwatch',
  source_ref text,
  run_id text references ops.pipeline_runs(run_id) on delete set null,
  underlying text not null,
  asof_time_utc timestamptz not null,
  symbol text,
  contract_month text,
  rate_bin text,
  probability numeric(8,5) check (probability is null or (probability >= 0 and probability <= 100)),
  current_probability numeric(8,5) check (current_probability is null or (current_probability >= 0 and current_probability <= 100)),
  diff_probability numeric(8,5),
  payload jsonb not null default '{}'::jsonb,
  ingested_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create unique index if not exists uq_cme_probabilities_natural_key
  on raw.cme_probabilities (
    source,
    underlying,
    asof_time_utc,
    coalesce(symbol, ''),
    coalesce(contract_month, ''),
    coalesce(rate_bin, '')
  );

create index if not exists idx_cme_prob_ui
  on raw.cme_probabilities (underlying, asof_time_utc desc, contract_month);

create table if not exists features.mt5_price_features (
  id bigint generated always as identity primary key,
  source text not null default 'mt5',
  source_ref text,
  run_id text references ops.pipeline_runs(run_id) on delete set null,
  symbol text not null,
  timeframe text not null check (timeframe in ('m15', 'h4', 'd1', 'w1', 'm1')),
  bar_time_utc timestamptz not null,
  tr numeric(18,8),
  atr14 numeric(18,8),
  range_value numeric(18,8),
  body numeric(18,8),
  upper_wick numeric(18,8),
  lower_wick numeric(18,8),
  close_pos numeric(18,8),
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
  payload jsonb not null default '{}'::jsonb,
  ingested_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  unique (source, symbol, timeframe, bar_time_utc)
);

create index if not exists idx_mt5_features_symbol_tf_time
  on features.mt5_price_features (symbol, timeframe, bar_time_utc desc);

create table if not exists features.calendar_surprise (
  id bigint generated always as identity primary key,
  source text not null default 'calendar',
  source_ref text,
  run_id text references ops.pipeline_runs(run_id) on delete set null,
  event_id text,
  event_time_utc timestamptz not null,
  currency text,
  impact text,
  event_name text,
  actual_value numeric,
  forecast_value numeric,
  previous_value numeric,
  surprise_value numeric,
  surprise_zscore numeric,
  payload jsonb not null default '{}'::jsonb,
  ingested_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create unique index if not exists uq_calendar_surprise_natural_key
  on features.calendar_surprise (source, coalesce(event_id, event_name), event_time_utc);

create index if not exists idx_calendar_surprise_ui
  on features.calendar_surprise (event_time_utc desc, currency, impact);

drop trigger if exists trg_set_updated_at_pipeline_runs on ops.pipeline_runs;
create trigger trg_set_updated_at_pipeline_runs
before update on ops.pipeline_runs
for each row execute function ops.set_updated_at_utc();

drop trigger if exists trg_set_updated_at_calendar_events on raw.calendar_events;
create trigger trg_set_updated_at_calendar_events
before update on raw.calendar_events
for each row execute function ops.set_updated_at_utc();

drop trigger if exists trg_set_updated_at_fred_observations on raw.fred_observations;
create trigger trg_set_updated_at_fred_observations
before update on raw.fred_observations
for each row execute function ops.set_updated_at_utc();

drop trigger if exists trg_set_updated_at_mt5_ohlcv on raw.mt5_ohlcv;
create trigger trg_set_updated_at_mt5_ohlcv
before update on raw.mt5_ohlcv
for each row execute function ops.set_updated_at_utc();

drop trigger if exists trg_set_updated_at_cme_quotes on raw.cme_quotes;
create trigger trg_set_updated_at_cme_quotes
before update on raw.cme_quotes
for each row execute function ops.set_updated_at_utc();

drop trigger if exists trg_set_updated_at_cme_probabilities on raw.cme_probabilities;
create trigger trg_set_updated_at_cme_probabilities
before update on raw.cme_probabilities
for each row execute function ops.set_updated_at_utc();

drop trigger if exists trg_set_updated_at_mt5_price_features on features.mt5_price_features;
create trigger trg_set_updated_at_mt5_price_features
before update on features.mt5_price_features
for each row execute function ops.set_updated_at_utc();

drop trigger if exists trg_set_updated_at_calendar_surprise on features.calendar_surprise;
create trigger trg_set_updated_at_calendar_surprise
before update on features.calendar_surprise
for each row execute function ops.set_updated_at_utc();

create or replace view normalized.calendar_events_latest as
select distinct on (coalesce(event_id, event_name), event_time_utc)
  source,
  run_id,
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
  ingested_at_utc
from raw.calendar_events
order by coalesce(event_id, event_name), event_time_utc, ingested_at_utc desc;

create or replace view normalized.fred_latest as
select distinct on (series_id)
  source,
  run_id,
  series_id,
  frequency,
  observation_date,
  value_numeric,
  value_text,
  ingested_at_utc
from raw.fred_observations
order by series_id, observation_date desc, ingested_at_utc desc;

create or replace view normalized.mt5_latest as
select distinct on (symbol, timeframe)
  source,
  run_id,
  symbol,
  timeframe,
  bar_time_utc,
  open,
  high,
  low,
  close,
  tick_volume,
  ingested_at_utc
from raw.mt5_ohlcv
order by symbol, timeframe, bar_time_utc desc, ingested_at_utc desc;

create or replace view normalized.cme_probabilities_latest as
with latest_asof as (
  select underlying, max(asof_time_utc) as asof_time_utc
  from raw.cme_probabilities
  group by underlying
)
select p.*
from raw.cme_probabilities p
join latest_asof l
  on l.underlying = p.underlying
 and l.asof_time_utc = p.asof_time_utc;

commit;
