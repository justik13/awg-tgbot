# awg-tgbot v2 - Distributed Multi-Server VPN Management System

## Architecture Overview

This is a refactored version of awg-tgbot implementing a Master-Slave pattern for distributed VPN management.

### Components

1. **Master (Bot Server)**
   - Telegram bot (aiogram 3.x)
   - Central database (PostgreSQL/SQLite)
   - SSH key management
   - Node orchestration via asyncssh

2. **Slave (VPN Nodes)**
   - Amnezia WireGuard server
   - SSH access for Master
   - Local peer management

### New File Structure

```
bot/
├── __init__.py
├── app.py                 # Main bot entry point
├── config.py              # Configuration loader
├── database/
│   ├── __init__.py
│   ├── models.py          # SQLAlchemy ORM models
│   ├── connection.py      # DB connection management
│   └── repositories.py    # Data access layer
├── core/
│   ├── __init__.py
│   ├── nodemanager.py     # Remote node management via SSH
│   ├── awg_config.py      # AWG config generation
│   └── scheduler.py       # Background tasks
├── handlers/
│   ├── __init__.py
│   ├── user.py            # User handlers
│   └── admin.py           # Admin handlers
├── keyboards/
│   ├── __init__.py
│   └── inline.py          # Inline keyboard builders
├── middlewares/
│   ├── __init__.py
│   └── auth.py            # Auth & rate limiting
└── utils/
    ├── __init__.py
    ├── ssh_keys.py        # SSH key utilities
    └── validators.py      # Input validators

docker-compose.yml         # Deployment configuration
install.sh                 # Universal installer
.env.example               # Environment template
requirements.txt           # Python dependencies
```

## Database Schema (v2)

### Tables

1. **users** - Telegram users
   - id (PK)
   - telegram_id (unique)
   - status (active/banned/deleted)
   - created_at

2. **subscriptions** - User subscriptions
   - id (PK)
   - user_id (FK -> users)
   - expires_at
   - max_devices (default: 3)
   - created_at

3. **servers** - VPN nodes
   - id (PK)
   - ip (unique)
   - country_code
   - city
   - ssh_port (default: 22)
   - ssh_user
   - awg_settings (JSON: Jc, Jmin, Jmax, S1, S2, H1-H4, etc.)
   - server_public_key
   - vpn_subnet_prefix
   - is_active
   - last_health_check
   - created_at

4. **devices** - VPN devices/configs
   - id (PK)
   - subscription_id (FK -> subscriptions)
   - server_id (FK -> servers)
   - pub_key (unique)
   - priv_key (encrypted)
   - psk_key (encrypted)
   - ip_address
   - conf_name
   - created_at
   - last_activity

5. **server_stats** - Node statistics
   - id (PK)
   - server_id (FK -> servers)
   - total_peers
   - rx_bytes
   - tx_bytes
   - recorded_at

## Installation Menu

```bash
./install.sh

AWG-TGBOT v2 Installer

[1] Main Master (Bot + DB + SSH-key gen)
[2] VPN Node (AWG install + add Master's public key)
[3] Hybrid (All-in-one for testing)

Select option: 
```

## Key Features

- **Multi-country support**: Users select country when adding device
- **Subscription-based**: 1 subscription = 3 device slots
- **Remote management**: Master manages nodes via SSH
- **Amnezia WG**: Full obfuscation parameters support (Jc, Jmin, Jmax, S1-S4, H1-H4, I1-I5)
- **Docker deployment**: Ready for production

## Implemented Modules

### Database Layer (`bot/database/`)

- **models.py**: SQLAlchemy ORM models for User, Subscription, Server, Device, ServerStats, Payment
- **connection.py**: Async database connection manager with SQLite/PostgreSQL support
- **repositories.py**: Data access layer with typed repository classes

### Core Layer (`bot/core/`)

- **nodemanager.py**: SSH-based remote node management using asyncssh
  - PeerInfo class for parsing `awg show` output
  - NodeConnection for SSH session management
  - NodeManager for high-level operations (add/remove peers, health checks)
  
- **awg_config.py**: Amnezia WireGuard configuration generator
  - AWGSettings dataclass with all obfuscation parameters
  - ClientConfig for complete client configuration
  - Support for both .conf and Amnezia JSON formats

## Next Steps

1. Create the universal `install.sh` script with menu
2. Implement handlers for country selection (CallbackData in aiogram 3.x)
3. Create Docker Compose configuration
4. Add migration script from v1 to v2 schema
5. Implement remaining modules (scheduler, handlers, keyboards, middlewares)
