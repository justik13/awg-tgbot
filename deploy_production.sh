#!/bin/bash
# Production Deployment Script for Bot Migration
# Usage: ./deploy_production.sh [--staging|--canary|--full]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
BOT_DIR="/workspace/bot"
DB_FILE="${BOT_DIR}/database.db"
BACKUP_DIR="${BOT_DIR}/backups"
LOG_FILE="${BOT_DIR}/deploy.log"
DEPLOY_MODE="${1:---full}"

# Functions
log() {
    echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

error_exit() {
    log "${RED}ERROR: $1${NC}"
    exit 1
}

success() {
    log "${GREEN}✓ $1${NC}"
}

warning() {
    log "${YELLOW}⚠ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check Python version
    if ! command -v python3 &> /dev/null; then
        error_exit "Python 3 is not installed"
    fi
    
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
    if (( $(echo "$PYTHON_VERSION < 3.8" | bc -l) )); then
        error_exit "Python 3.8+ required, found $PYTHON_VERSION"
    fi
    
    # Check required packages
    python3 -c "import aiogram, aiosqlite, asyncio" || \
        error_exit "Required packages not installed. Run: pip install -r requirements.txt"
    
    success "Prerequisites checked"
}

# Backup database
backup_database() {
    log "Creating database backup..."
    
    mkdir -p "$BACKUP_DIR"
    BACKUP_FILE="${BACKUP_DIR}/db_backup_$(date +%Y%m%d_%H%M%S).db"
    
    if [ -f "$DB_FILE" ]; then
        cp "$DB_FILE" "$BACKUP_FILE"
        gzip "$BACKUP_FILE"
        success "Database backed up to ${BACKUP_FILE}.gz"
    else
        warning "No database file found, skipping backup"
    fi
}

# Backup code
backup_code() {
    log "Creating code backup..."
    
    CODE_BACKUP="${BACKUP_DIR}/code_backup_$(date +%Y%m%d_%H%M%S).tar.gz"
    tar -czf "$CODE_BACKUP" -C "$(dirname "$BOT_DIR")" "$(basename "$BOT_DIR")" --exclude="backups" --exclude="__pycache__" --exclude="*.pyc"
    
    success "Code backed up to $CODE_BACKUP"
}

# Run database migrations
run_migrations() {
    log "Running database migrations..."
    
    MIGRATION_FILE="${BOT_DIR}/schema_migration.sql"
    
    if [ ! -f "$MIGRATION_FILE" ]; then
        error_exit "Migration file not found: $MIGRATION_FILE"
    fi
    
    # Apply migrations using sqlite3
    if command -v sqlite3 &> /dev/null; then
        sqlite3 "$DB_FILE" < "$MIGRATION_FILE"
        success "Database migrations applied"
    else
        warning "sqlite3 not found, skipping automatic migration"
        warning "Please run migrations manually: sqlite3 $DB_FILE < $MIGRATION_FILE"
    fi
}

# Validate syntax
validate_syntax() {
    log "Validating Python syntax..."
    
    cd "$BOT_DIR"
    
    # Check all Python files
    for file in *.py; do
        if [ -f "$file" ]; then
            python3 -m py_compile "$file" || error_exit "Syntax error in $file"
        fi
    done
    
    success "All Python files validated"
}

# Run tests
run_tests() {
    log "Running production tests..."
    
    cd /workspace
    
    if [ -f "tests/test_production_migration.py" ]; then
        python3 -m pytest tests/test_production_migration.py -v --tb=short || \
            error_exit "Production tests failed"
        success "All tests passed"
    else
        warning "Test file not found, skipping tests"
    fi
}

# Deploy based on mode
deploy_staging() {
    log "Deploying to STAGING environment..."
    
    # Staging deployment: validate only, don't restart
    check_prerequisites
    validate_syntax
    run_tests
    
    success "Staging validation complete"
    warning "Review changes before deploying to production"
}

deploy_canary() {
    log "Deploying CANARY release (10% traffic)..."
    
    # Canary deployment: deploy but keep old version ready
    backup_database
    backup_code
    run_migrations
    validate_syntax
    run_tests
    
    # Set canary flag
    echo "canary" > "${BOT_DIR}/.deploy_mode"
    
    success "Canary deployment complete"
    warning "Monitor logs for 1 hour before full rollout"
}

deploy_full() {
    log "Deploying FULL production release..."
    
    # Full deployment
    backup_database
    backup_code
    run_migrations
    validate_syntax
    run_tests
    
    # Clear pycache
    find "$BOT_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    
    # Set production flag
    echo "production" > "${BOT_DIR}/.deploy_mode"
    
    success "Full production deployment complete"
}

# Rollback function
rollback() {
    log "Initiating rollback..."
    
    # Find latest backup
    LATEST_CODE=$(ls -t "${BACKUP_DIR}"/code_backup_*.tar.gz 2>/dev/null | head -1)
    LATEST_DB=$(ls -t "${BACKUP_DIR}"/db_backup_*.db.gz 2>/dev/null | head -1)
    
    if [ -z "$LATEST_CODE" ] || [ -z "$LATEST_DB" ]; then
        error_exit "No backups found for rollback"
    fi
    
    warning "Rolling back to: $LATEST_CODE and $LATEST_DB"
    
    # Restore code
    tar -xzf "$LATEST_CODE" -C "$(dirname "$BOT_DIR")"
    
    # Restore database
    gunzip -c "$LATEST_DB" > "$DB_FILE"
    
    success "Rollback complete"
}

# Health check
health_check() {
    log "Running health check..."
    
    # Check if bot process is running
    if pgrep -f "python.*app.py" > /dev/null; then
        success "Bot process is running"
    else
        warning "Bot process not running"
    fi
    
    # Check database connectivity
    if [ -f "$DB_FILE" ]; then
        sqlite3 "$DB_FILE" "SELECT 1;" > /dev/null && success "Database accessible"
    fi
    
    # Check disk space
    DISK_USAGE=$(df -h "$BOT_DIR" | awk 'NR==2 {print $5}' | cut -d'%' -f1)
    if [ "$DISK_USAGE" -lt 90 ]; then
        success "Disk usage OK (${DISK_USAGE}%)"
    else
        error_exit "Disk usage critical (${DISK_USAGE}%)"
    fi
}

# Main deployment
main() {
    log "=========================================="
    log "Bot Migration Deployment Script"
    log "Mode: $DEPLOY_MODE"
    log "=========================================="
    
    case "$DEPLOY_MODE" in
        --staging)
            deploy_staging
            ;;
        --canary)
            deploy_canary
            ;;
        --full)
            deploy_full
            ;;
        --rollback)
            rollback
            ;;
        --health)
            health_check
            ;;
        *)
            echo "Usage: $0 [--staging|--canary|--full|--rollback|--health]"
            exit 1
            ;;
    esac
    
    log "=========================================="
    log "Deployment completed successfully!"
    log "=========================================="
}

# Run main function
main
