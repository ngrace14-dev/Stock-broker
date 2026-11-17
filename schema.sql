-- Master Blueprint v3.7: DEFCON 0 Production Schema
-- ACID PostgreSQL Ledger for Self-Replicating Trading Engine with v3.7 Injected Deltas

CREATE TYPE division_type AS ENUM ('VELOCITY_BETA', 'PRECIOUS_METALS', 'HARDWARE_TECH', 'GLOBAL_PRODUCE', 'DIVIDEND_INSURER');
CREATE TYPE cadence_class_type AS ENUM ('HIGH_VELOCITY', 'SWING', 'MACRO_CYCLICAL', 'PERMANENT_ANCHOR');
CREATE TYPE node_status_type AS ENUM ('ACTIVE', 'PROBATION', 'AUDIT_PENDING_FLAT', 'RETIRED', 'OVERNIGHT_HOLD_LOCK', 'COOLDOWN', 'WIPED_OUT');
CREATE TYPE execution_regime_type AS ENUM ('FRACTIONAL_SW', 'INTEGER_OCO');
CREATE TYPE audit_grade_type AS ENUM ('A', 'B', 'C');

CREATE TABLE global_ledger (
    id INT PRIMARY KEY CHECK (id = 1),
    alpaca_settled_cash NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    alpaca_unsettled_cash NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    daily_settled_cash_consumed NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    bank_savings_ready NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    ytd_tax_escrow NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    crystallized_irs_liability NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    cumulative_ytd_net_pnl NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    ytd_tax_hwm NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    insurance_liquid_cash_buffer NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    insurance_aristocrat_overflow_cash NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    trading_halted BOOLEAN NOT NULL DEFAULT FALSE,
    halt_reason TEXT,
    last_reconciled_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE nodes (
    node_id SERIAL PRIMARY KEY,
    parent_node_id INT REFERENCES nodes(node_id),
    division division_type NOT NULL,
    cadence_class cadence_class_type NOT NULL,
    assigned_ticker TEXT NOT NULL,
    backup_ticker TEXT NOT NULL,
    status node_status_type NOT NULL DEFAULT 'ACTIVE',
    tier INT NOT NULL CHECK (tier >= 0),
    tier_starting_active_capital NUMERIC(12, 2) NOT NULL,
    active_capital NUMERIC(12, 2) NOT NULL,
    cold_storage NUMERIC(12, 2) NOT NULL,
    drip_bucket NUMERIC(12, 2) NOT NULL DEFAULT 0.00 CHECK (drip_bucket >= 0.00),
    last_bailout_claimed_at TIMESTAMP WITH TIME ZONE,
    tolls_paid_lifetime NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    claims_drawn_lifetime NUMERIC(16, 2) NOT NULL DEFAULT 0.00,
    round_trips_in_cycle INT NOT NULL DEFAULT 0,
    daily_starting_capital NUMERIC(12, 2) NOT NULL,
    daily_settled_budget NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    daily_realized_pnl NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    consecutive_stop_outs INT NOT NULL DEFAULT 0,
    cooldown_until TIMESTAMP WITH TIME ZONE,
    probation_started_at TIMESTAMP WITH TIME ZONE,
    audit_due_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW() + INTERVAL '365 days',
    retire_flatten_deadline_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    next_audit_anniversary_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE UNIQUE INDEX idx_active_ticker_mutex ON nodes(assigned_ticker) WHERE status = 'ACTIVE';

CREATE TABLE node_positions (
    position_id SERIAL PRIMARY KEY,
    node_id INT NOT NULL REFERENCES nodes(node_id),
    ticker TEXT NOT NULL,
    regime execution_regime_type NOT NULL,
    filled_qty NUMERIC(12, 6) NOT NULL,
    filled_avg_price NUMERIC(12, 2) NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    hard_stop_price NUMERIC(12, 2),
    take_profit_price NUMERIC(12, 2),
    breakeven_ratchet_armed BOOLEAN DEFAULT FALSE,
    partial_75_taken BOOLEAN DEFAULT FALSE,
    partial_exit_pending BOOLEAN DEFAULT FALSE,
    trailing_stop_pct NUMERIC(5, 4),
    high_watermark_price NUMERIC(12, 2),
    overnight_hold_lock BOOLEAN NOT NULL DEFAULT FALSE,
    opened_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMP WITH TIME ZONE
);

CREATE UNIQUE INDEX idx_single_open_position_per_node ON node_positions(node_id) WHERE is_open = TRUE;
CREATE UNIQUE INDEX idx_single_open_position_per_ticker ON node_positions(ticker) WHERE is_open = TRUE;

CREATE TABLE orders (
    client_order_id TEXT PRIMARY KEY,
    alpaca_order_id TEXT,
    node_id INT NOT NULL REFERENCES nodes(node_id),
    ticker TEXT NOT NULL,
    regime execution_regime_type NOT NULL,
    side TEXT NOT NULL,
    qty NUMERIC(12, 6) NOT NULL,
    limit_price NUMERIC(12, 2),
    stop_price NUMERIC(12, 2),
    status TEXT NOT NULL,
    filled_qty NUMERIC(12, 6) DEFAULT 0.00,
    filled_avg_price NUMERIC(12, 2) DEFAULT 0.00,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE aristocrat_holdings (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    share_qty NUMERIC(12, 6) NOT NULL,
    cost_basis NUMERIC(16, 2) NOT NULL,
    trailing_yield_pct NUMERIC(5, 4),
    consecutive_div_growth_years INT,
    payout_ratio_pct NUMERIC(5, 2),
    last_div_recorded_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE wash_sale_ledger (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    realized_loss_amount NUMERIC(12, 2) NOT NULL,
    loss_realized_at TIMESTAMP WITH TIME ZONE NOT NULL,
    lockout_expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE audit_history (
    id SERIAL PRIMARY KEY,
    node_id INT REFERENCES nodes(node_id),
    audit_type TEXT NOT NULL,
    grade audit_grade_type,
    comment TEXT,
    performed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE reconciliation_log (
    id SERIAL PRIMARY KEY,
    reconciliation_type TEXT NOT NULL,
    alpaca_total_cash NUMERIC(16, 2) NOT NULL,
    virtual_wallets_sum NUMERIC(16, 2) NOT NULL,
    open_positions_basis_sum NUMERIC(16, 2) NOT NULL,
    discrepancy NUMERIC(16, 2) NOT NULL,
    action_taken TEXT NOT NULL,
    performed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
