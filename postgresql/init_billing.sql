-- ============================================================
-- Billing OLTP Schema for CDC Pipeline
-- Tables: billing_plans (gói cước) & subscribers (thuê bao)
-- ============================================================

-- Bảng gói cước Viettel
CREATE TABLE IF NOT EXISTS billing_plans (
    plan_id         VARCHAR(20)   PRIMARY KEY,
    plan_name       VARCHAR(100)  NOT NULL,
    plan_type       VARCHAR(20)   NOT NULL,    -- 'prepaid' | 'postpaid'
    monthly_fee     DECIMAL(12,2) NOT NULL DEFAULT 0,
    data_quota_gb   DECIMAL(8,2),
    voice_minutes   INTEGER,
    created_at      TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP     NOT NULL DEFAULT NOW()
);

-- Bảng thuê bao
CREATE TABLE IF NOT EXISTS subscribers (
    subscriber_id   VARCHAR(15)   PRIMARY KEY, -- MSISDN phone number
    full_name       VARCHAR(100)  NOT NULL,
    plan_id         VARCHAR(20)   NOT NULL REFERENCES billing_plans(plan_id),
    home_cell_id    VARCHAR(30),               -- BTS cell gần nhà thuê bao
    status          VARCHAR(20)   NOT NULL DEFAULT 'active', -- active/suspended/terminated
    activated_at    TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sub_plan    ON subscribers(plan_id);
CREATE INDEX IF NOT EXISTS idx_sub_cell    ON subscribers(home_cell_id);
CREATE INDEX IF NOT EXISTS idx_sub_updated ON subscribers(updated_at);
CREATE INDEX IF NOT EXISTS idx_bp_updated  ON billing_plans(updated_at);

-- Seed: 10 gói cước Viettel thực tế
INSERT INTO billing_plans (plan_id, plan_name, plan_type, monthly_fee, data_quota_gb, voice_minutes)
VALUES
    ('ST90',   'ST90 - Data 90K',      'prepaid',   90000,   4.0,   NULL),
    ('ST120',  'ST120 - Data 120K',     'prepaid',  120000,   7.0,   NULL),
    ('V200',   'V200 - Combo 200K',     'prepaid',  200000,  10.0,   500),
    ('V300',   'V300 - Combo 300K',     'prepaid',  300000,  20.0,  1000),
    ('ECO30',  'Economy 30',            'prepaid',   30000,   1.5,    100),
    ('MAX90',  'MaxSpeed 90',           'postpaid',  90000,   8.0,   NULL),
    ('MAX200', 'MaxSpeed 200',          'postpaid', 200000,  30.0,   NULL),
    ('BIZ300', 'Business 300',          'postpaid', 300000,  50.0,   2000),
    ('BIZ500', 'Business 500',          'postpaid', 500000, 100.0,   5000),
    ('VIP',    'Viettel VIP Unlimited', 'postpaid', 990000,   NULL,  NULL)
ON CONFLICT (plan_id) DO NOTHING;
