-- cbond.duckdb schema — idempotent (all CREATE TABLE IF NOT EXISTS)

CREATE TABLE IF NOT EXISTS universe (
  code          TEXT PRIMARY KEY,
  name          TEXT,
  ucode         TEXT,
  uname         TEXT,
  list_date     TEXT,
  maturity_date TEXT,
  updated_at    TEXT
);

CREATE TABLE IF NOT EXISTS valuation_daily (
  trade_date     TEXT,
  code           TEXT,
  price          DOUBLE,
  change_pct     DOUBLE,
  conv_prem_pct  DOUBLE,
  pure_prem_pct  DOUBLE,
  outstanding_yi DOUBLE,
  rating         TEXT,
  maturity_date  TEXT,
  PRIMARY KEY (trade_date, code)
);

ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS change_pct DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS conv_price DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS no_call_start TEXT;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS no_call_end TEXT;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS call_trigger_days INTEGER;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS call_trigger_ratio DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS has_down_revision TEXT;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS down_trigger_ratio DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS ths_industry TEXT;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS pb DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS redemp_stop_date TEXT;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS pe_ttm DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS total_mv_yi DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS implied_vol DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS pure_bond_ytm DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS ifind_doublelow DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS option_value DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS surplus_days INTEGER;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS surplus_years DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS accum_conv_ratio DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS dilution_ratio DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS bs_value DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS relative_value DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS bs_delta DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS bs_gamma DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS bs_theta DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS bs_vega DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS pure_bond_value DOUBLE;
ALTER TABLE valuation_daily ADD COLUMN IF NOT EXISTS maturity_call_price DOUBLE;

CREATE TABLE IF NOT EXISTS vol_daily (
  trade_date  TEXT,
  ucode       TEXT,
  vol_20d_pct DOUBLE,
  n_samples   INTEGER,
  PRIMARY KEY (trade_date, ucode)
);

CREATE TABLE IF NOT EXISTS underlying_profile (
  ucode         TEXT PRIMARY KEY,
  uname         TEXT,
  industry      TEXT,
  main_business TEXT,
  updated_at    TEXT
);

CREATE TABLE IF NOT EXISTS strategy_picks (
  trade_date       TEXT,
  code             TEXT,
  strategy         TEXT,
  rank_overall     DOUBLE,
  rank_conv_prem   INTEGER,
  rank_price       INTEGER,
  note             TEXT,
  PRIMARY KEY (trade_date, code, strategy)
);

CREATE TABLE IF NOT EXISTS themes (
  trade_date       TEXT,
  code             TEXT,
  theme_l1         TEXT,
  all_themes_json  TEXT,
  business_rewrite TEXT,
  industry         TEXT,
  PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS etl_runs (
  run_id      TEXT PRIMARY KEY,
  trade_date  TEXT,
  step        TEXT,
  started_at  TEXT,
  finished_at TEXT,
  row_count   INTEGER,
  status      TEXT,
  note        TEXT
);
