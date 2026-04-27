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
  conv_price DOUBLE,
  no_call_start TEXT,
  no_call_end TEXT,
  call_trigger_days INTEGER,
  call_trigger_ratio DOUBLE,
  has_down_revision TEXT,
  down_trigger_ratio DOUBLE,
  ths_industry TEXT,
  pb DOUBLE,
  redemp_stop_date TEXT,
  pe_ttm DOUBLE,
  total_mv_yi DOUBLE,
  implied_vol DOUBLE,
  pure_bond_ytm DOUBLE,
  ifind_doublelow DOUBLE,
  option_value DOUBLE,
  surplus_days INTEGER,
  surplus_years DOUBLE,
  accum_conv_ratio DOUBLE,
  dilution_ratio DOUBLE,
  bs_value DOUBLE,
  relative_value DOUBLE,
  bs_delta DOUBLE,
  bs_gamma DOUBLE,
  bs_theta DOUBLE,
  bs_vega DOUBLE,
  pure_bond_value DOUBLE,
  maturity_call_price DOUBLE,
  PRIMARY KEY (trade_date, code)
);

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
  run_id       TEXT PRIMARY KEY,
  trade_date   TEXT,
  step         TEXT,
  started_at   TEXT,
  finished_at  TEXT,
  row_count    INTEGER,
  status       TEXT,
  note         TEXT
);

-- Secondary indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_val_code_date ON valuation_daily(code, trade_date);
CREATE INDEX IF NOT EXISTS idx_strat_date_strat ON strategy_picks(trade_date, strategy);
CREATE INDEX IF NOT EXISTS idx_vol_ucode_date ON vol_daily(ucode, trade_date);
CREATE INDEX IF NOT EXISTS idx_etl_date ON etl_runs(trade_date, step);
