# 🚀 Production Deployment Checklist

## ✅ Pre-Deployment Verification

### 1. Code Review
- [ ] All new files created:
  - [x] `bot/migration_utils.py`
  - [x] `bot/fsm_states.py`
  - [x] `bot/idempotency.py`
  - [x] `bot/callbacks.py`
  - [x] `bot/schema_migration.sql`
  - [x] `tests/test_production_migration.py`
  - [x] `deploy_production.sh`
- [ ] All modified files reviewed:
  - [x] `bot/payments.py` (idempotency + guards)
  - [x] `bot/handlers_user.py` (FSM integration)
  - [x] `bot/handlers_admin.py` (guards + FSM)
  - [x] `bot/keyboards.py` (new navigation)
  - [x] `bot/platega_webhook.py` (atomic operations)
  - [x] `bot/database.py` (new columns)

### 2. Syntax Validation
```bash
cd /workspace/bot && python3 -m py_compile *.py
```
- [ ] No syntax errors

### 3. Database Backup
```bash
cp bot/database.db bot/backups/db_backup_$(date +%Y%m%d_%H%M%S).db
```
- [ ] Backup created
- [ ] Backup verified (file size > 0)

### 4. Code Backup
```bash
git commit -am "Pre-deployment backup"
```
- [ ] Git commit created
- [ ] Can rollback with `git reset --hard HEAD~1`

---

## 📋 Deployment Steps

### Step 1: Staging Environment (REQUIRED)
```bash
./deploy_production.sh --staging
```
- [ ] Prerequisites checked
- [ ] Syntax validated
- [ ] Tests passed
- [ ] No errors in logs

### Step 2: Database Migration
```bash
sqlite3 bot/database.db < bot/schema_migration.sql
```
- [ ] Migration script executed successfully
- [ ] New tables created:
  - [ ] `idempotency_keys`
  - [ ] `pending_payments`
  - [ ] `fsm_states`
  - [ ] `schema_migrations`
- [ ] New columns added to `payments`:
  - [ ] `idempotency_key`
  - [ ] `expires_at`
  - [ ] `processed_at`

### Step 3: Canary Deployment (10% traffic)
```bash
./deploy_production.sh --canary
```
- [ ] Deployed to canary environment
- [ ] Monitoring enabled for 1 hour
- [ ] Check metrics:
  - [ ] Payment success rate > 95%
  - [ ] Error rate < 1%
  - [ ] Response time < 2s
  - [ ] No race conditions detected

### Step 4: Full Production Rollout
```bash
./deploy_production.sh --full
```
- [ ] All tests passed
- [ ] Database migrated
- [ ] Code deployed
- [ ] Bot restarted
- [ ] Health check passed

---

## 🔍 Post-Deployment Verification

### Critical User Flows
Test each flow manually:

#### Flow 1: Purchase Subscription (Stars)
- [ ] `/start` → Main menu
- [ ] Click "🛒 Подписки" → Catalog shown
- [ ] Select tariff → Payment method selection
- [ ] Choose "Telegram Stars" → Invoice sent
- [ ] Complete payment → Success message
- [ ] Config delivered → WireGuard config received
- [ ] Profile updated → Subscription active

#### Flow 2: Purchase Subscription (SBP)
- [ ] `/start` → Main menu
- [ ] Click "🛒 Подписки" → Catalog shown
- [ ] Select tariff → Payment method selection
- [ ] Choose "СБП" → Payment URL shown
- [ ] Click "Проверить статус" → Status pending
- [ ] Pay via SBP (test mode)
- [ ] Webhook received → Payment confirmed
- [ ] Config delivered → WireGuard config received

#### Flow 3: Duplicate Click Protection
- [ ] Rapidly click payment button 5 times
- [ ] Only ONE payment invoice created
- [ ] No duplicate charges
- [ ] Error message shown for duplicate attempts

#### Flow 4: FSM State Recovery
- [ ] Start payment flow
- [ ] Restart bot mid-flow
- [ ] Bot recovers state or offers to restart
- [ ] No "stuck" states

#### Flow 5: Admin Functions
- [ ] Admin can view users
- [ ] Admin can edit prices (only once per click)
- [ ] Broadcast protected from duplicates
- [ ] All admin actions logged

### Monitoring Metrics
Check these metrics for 24 hours:

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Payment Success Rate | > 95% | ___% | [ ] |
| Error Rate | < 1% | ___% | [ ] |
| Avg Response Time | < 2s | ___s | [ ] |
| Duplicate Payments | 0 | ___ | [ ] |
| Race Conditions | 0 | ___ | [ ] |
| FSM Dangling States | 0 | ___ | [ ] |
| Webhook Failures | < 0.1% | ___% | [ ] |

---

## 🔄 Rollback Plan

If issues detected, execute immediately:

### Quick Rollback (Code Only)
```bash
git reset --hard HEAD~1
./deploy_production.sh --rollback
```

### Full Rollback (Code + Database)
```bash
# Restore code
git reset --hard HEAD~1

# Restore database
gunzip -c bot/backups/db_backup_YYYYMMDD_HHMMSS.db.gz > bot/database.db

# Restart bot
systemctl restart bot.service
```

### Partial Rollback (Disable New Features)
```bash
# Set feature flag to disable new payment flow
echo "legacy_mode" > bot/.deploy_mode

# Restart bot
systemctl restart bot.service
```

---

## 📞 Emergency Contacts

| Role | Name | Contact |
|------|------|---------|
| DevOps | ___ | ___ |
| Backend Lead | ___ | ___ |
| Product Owner | ___ | ___ |
| Support Lead | ___ | ___ |

---

## ✅ Final Sign-Off

- [ ] All tests passed
- [ ] All user flows verified
- [ ] Monitoring metrics within targets
- [ ] Rollback plan tested
- [ ] Team notified of deployment
- [ ] Documentation updated

**Deployment approved by:** ________________  
**Date:** ________________  
**Time:** ________________  

---

## 📝 Notes

Add any issues, observations, or follow-up tasks here:

```
_______________________________________________________
_______________________________________________________
_______________________________________________________
```
