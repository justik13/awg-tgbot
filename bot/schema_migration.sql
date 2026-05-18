-- Schema Migration for Bot Architecture Update
-- Run this migration BEFORE deploying new code to production
-- Backup your database first!

-- ============================================================
-- STEP 1: Create idempotency_keys table
-- ============================================================
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key_hash TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    user_id INTEGER,
    result_data TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_idempotency_user ON idempotency_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON idempotency_keys(expires_at);
CREATE INDEX IF NOT EXISTS idx_idempotency_status ON idempotency_keys(status);

-- ============================================================
-- STEP 2: Add columns to existing payments table
-- ============================================================
-- Add idempotency_key column if not exists
SELECT CASE 
    WHEN COUNT(*) = 0 THEN 'ALTER TABLE payments ADD COLUMN idempotency_key TEXT;'
    ELSE 'SELECT 1;'
END FROM pragma_table_info('payments') WHERE name='idempotency_key';

-- Add expires_at column if not exists  
SELECT CASE 
    WHEN COUNT(*) = 0 THEN 'ALTER TABLE payments ADD COLUMN expires_at INTEGER;'
    ELSE 'SELECT 1;'
END FROM pragma_table_info('payments') WHERE name='expires_at';

-- Add processed_at column if not exists
SELECT CASE 
    WHEN COUNT(*) = 0 THEN 'ALTER TABLE payments ADD COLUMN processed_at INTEGER;'
    ELSE 'SELECT 1;'
END FROM pragma_table_info('payments') WHERE name='processed_at';

-- ============================================================
-- STEP 3: Create pending_payments table (replaces in-memory dict)
-- ============================================================
CREATE TABLE IF NOT EXISTS pending_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    tariff TEXT NOT NULL,
    payment_method TEXT NOT NULL CHECK(payment_method IN ('stars', 'sbp')),
    transaction_id TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'paid', 'expired', 'cancelled'))
);

CREATE INDEX IF NOT EXISTS idx_pending_user ON pending_payments(user_id);
CREATE INDEX IF NOT EXISTS idx_pending_expires ON pending_payments(expires_at);
CREATE INDEX IF NOT EXISTS idx_pending_status ON pending_payments(status);
CREATE INDEX IF NOT EXISTS idx_pending_txn ON pending_payments(transaction_id);

-- ============================================================
-- STEP 4: Create fsm_states table (for state recovery)
-- ============================================================
CREATE TABLE IF NOT EXISTS fsm_states (
    user_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    data TEXT,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fsm_state ON fsm_states(state);

-- ============================================================
-- STEP 5: Cleanup old data
-- ============================================================
-- Mark expired pending payments
UPDATE pending_payments 
SET status = 'expired' 
WHERE expires_at < strftime('%s', 'now') 
  AND status = 'pending';

-- Clean up old idempotency keys
DELETE FROM idempotency_keys 
WHERE expires_at < strftime('%s', 'now');

-- ============================================================
-- STEP 6: Add migration tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at INTEGER NOT NULL,
    description TEXT
);

INSERT OR IGNORE INTO schema_migrations (version, applied_at, description)
VALUES ('2024.01', strftime('%s', 'now'), 'Added idempotency, FSM states, and pending payments tables');

-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================
-- Run these to verify migration success:
-- SELECT COUNT(*) FROM idempotency_keys;
-- SELECT COUNT(*) FROM pending_payments;
-- SELECT COUNT(*) FROM fsm_states;
-- SELECT * FROM schema_migrations;
