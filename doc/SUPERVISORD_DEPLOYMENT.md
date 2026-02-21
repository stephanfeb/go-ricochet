# Ricochet Server - Supervisord Deployment Guide

This guide covers deploying Ricochet Server using Supervisord for process management, with automatic crash recovery and log rotation.

## Why Supervisord?

Supervisord provides several advantages for Ricochet deployment:

- **Simple Management**: Easy start/stop/restart commands
- **Auto-Recovery**: Automatic restart on crashes
- **Log Management**: Built-in log rotation and tail viewing
- **Process Monitoring**: Real-time status monitoring
- **Web Interface**: Optional HTTP API for monitoring
- **Container-Friendly**: Works well in Docker/LXC containers
- **Cross-Platform**: Consistent behavior across Linux distributions

## Installation

### Ubuntu/Debian

```bash
sudo apt update
sudo apt install -y supervisor
sudo systemctl enable supervisor
sudo systemctl start supervisor
```

### RHEL/CentOS

```bash
sudo yum install -y epel-release
sudo yum install -y supervisor
sudo systemctl enable supervisord
sudo systemctl start supervisord
```

## Using the Debian Package (Recommended)

The easiest way to deploy is using our pre-built Debian package:

### Build the Package

```bash
cd /path/to/ricochet
./build-deb.sh
```

### Install

```bash
sudo dpkg -i build/dist/ricochet-server_1.0.0.deb
sudo apt-get install -f  # Install any missing dependencies
```

The package automatically:
- Creates the `ricochet` system user
- Sets up directories with correct permissions
- Installs supervisor configuration
- Copies example configuration files

### Configure and Start

```bash
# 1. Set database password
sudo nano /etc/ricochet/env
# Add: DB_PASSWORD=your_password

# 2. Configure server (optional)
sudo nano /etc/ricochet/config.yaml

# 3. Initialize database
sudo -u postgres psql << EOF
CREATE USER ricochet WITH PASSWORD 'your_password';
CREATE DATABASE ricochet OWNER ricochet;
EOF
psql -U ricochet -d ricochet -f /opt/ricochet/schema.sql

# 4. Start service
sudo supervisorctl start ricochet
```

## Manual Installation

If you prefer manual installation without the package:

### 1. Build Ricochet

```bash
cd /path/to/ricochet
dart compile exe bin/ricochet.dart -o ricochet_server
```

### 2. Install Files

```bash
# Create directories
sudo mkdir -p /opt/ricochet
sudo mkdir -p /etc/ricochet
sudo mkdir -p /var/lib/ricochet
sudo mkdir -p /var/log/ricochet

# Copy files
sudo cp ricochet_server /opt/ricochet/
sudo cp schema.sql /opt/ricochet/
sudo cp deploy/run.sh /opt/ricochet/
sudo cp config.postgres.example.yaml /etc/ricochet/config.yaml
sudo cp deploy/env.example /etc/ricochet/env

# Set permissions
sudo chmod +x /opt/ricochet/ricochet_server
sudo chmod +x /opt/ricochet/run.sh

# Create user
sudo useradd -r -s /bin/false -d /var/lib/ricochet ricochet
sudo chown -R ricochet:ricochet /var/lib/ricochet /var/log/ricochet
```

### 3. Configure Supervisor

```bash
sudo cp deploy/supervisor/ricochet.conf /etc/supervisor/conf.d/

# Reload supervisor
sudo supervisorctl reread
sudo supervisorctl update
```

## Configuration

### Supervisor Configuration

The supervisor config (`/etc/supervisor/conf.d/ricochet.conf`) controls process management:

```ini
[program:ricochet]
command=/opt/ricochet/run.sh
directory=/var/lib/ricochet
user=ricochet

; Auto-restart settings
autostart=true          ; Start on supervisor boot
autorestart=true        ; Always restart on exit
startsecs=10           ; Must run 10s to be "started"
startretries=3         ; Retry 3 times before giving up

; Shutdown settings
stopsignal=TERM        ; Signal to use for shutdown
stopwaitsecs=30        ; Wait 30s before SIGKILL

; Logging
stdout_logfile=/var/log/ricochet/stdout.log
stderr_logfile=/var/log/ricochet/stderr.log
stdout_logfile_maxbytes=50MB
stdout_logfile_backups=10
```

### Auto-Restart Behavior

Supervisor will automatically restart Ricochet when:

1. **Process crashes** - Any unexpected exit
2. **Exit with error code** - Non-zero exit status
3. **Process fails to start** - Up to 3 retry attempts
4. **Process exits too quickly** - Exits before `startsecs` timeout

**Restart Delays:**
- Immediate restart after crash
- No exponential backoff by default
- After 3 failed starts, process enters FATAL state

### Preventing Restart Loops

If Ricochet repeatedly fails to start (e.g., database down), supervisor will stop trying after 3 attempts. This prevents infinite restart loops.

Check status:
```bash
sudo supervisorctl status ricochet
# Output: ricochet    FATAL    too many retries
```

To manually retry after fixing the issue:
```bash
sudo supervisorctl start ricochet
```

## Managing the Service

### Basic Commands

```bash
# Start
sudo supervisorctl start ricochet

# Stop
sudo supervisorctl stop ricochet

# Restart
sudo supervisorctl restart ricochet

# Status
sudo supervisorctl status ricochet
# Output: ricochet    RUNNING   pid 12345, uptime 1:23:45
```

### Viewing Logs

```bash
# Tail real-time logs (stdout + stderr)
sudo supervisorctl tail -f ricochet

# Tail only stderr
sudo supervisorctl tail -f ricochet stderr

# View last 100 lines
sudo supervisorctl tail -100 ricochet stdout

# View log files directly
sudo tail -f /var/log/ricochet/stdout.log
sudo tail -f /var/log/ricochet/stderr.log
```

### Reloading Configuration

After editing `/etc/ricochet/config.yaml`:

```bash
# Restart the process
sudo supervisorctl restart ricochet
```

After editing `/etc/supervisor/conf.d/ricochet.conf`:

```bash
# Reload supervisor config
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl restart ricochet
```

## Monitoring

### Status Monitoring

```bash
# Detailed status
sudo supervisorctl status ricochet
# Shows: name, state, PID, uptime

# All processes
sudo supervisorctl status
```

### Process States

- `RUNNING` - Process is running normally
- `STARTING` - Process is starting up
- `STOPPED` - Process was manually stopped
- `FATAL` - Process failed to start after retries
- `BACKOFF` - Process is being restarted after failure
- `EXITED` - Process exited (may be restarting)

### Health Checks

Create a health check script (`/opt/ricochet/health_check.sh`):

```bash
#!/bin/bash

# Check if process is running
if ! sudo supervisorctl status ricochet | grep -q RUNNING; then
    echo "CRITICAL: Ricochet not running"
    exit 2
fi

# Check database connectivity
if ! psql -U ricochet -d ricochet -c "SELECT 1" >/dev/null 2>&1; then
    echo "WARNING: Database connection issue"
    exit 1
fi

echo "OK: Ricochet is healthy"
exit 0
```

Schedule with cron:
```bash
# Check every 5 minutes
*/5 * * * * /opt/ricochet/health_check.sh || /opt/ricochet/alert.sh
```

## Log Management

### Log Rotation

Supervisor handles log rotation automatically:
- Max file size: 50MB
- Backups kept: 10
- Total log space: ~1GB per log type

Logs are at:
- `/var/log/ricochet/stdout.log` (+ backups: .1, .2, etc.)
- `/var/log/ricochet/stderr.log` (+ backups: .1, .2, etc.)

### Searching Logs

```bash
# Search recent logs
sudo grep -r "error" /var/log/ricochet/

# Search all backups
sudo zgrep "database connection" /var/log/ricochet/stderr.log*

# Count errors
sudo grep -c "ERROR" /var/log/ricochet/stderr.log
```

## Supervisor Web Interface (Optional)

Enable the web interface for remote monitoring:

Edit `/etc/supervisor/supervisord.conf`:
```ini
[inet_http_server]
port=127.0.0.1:9001
username=admin
password=your_secure_password
```

Restart supervisor:
```bash
sudo systemctl restart supervisor
```

Access at: http://localhost:9001

**Security Note**: Only bind to localhost or use a reverse proxy with SSL.

## Troubleshooting

### Service Won't Start

```bash
# Check detailed logs
sudo supervisorctl tail ricochet stderr

# Check if binary exists and is executable
ls -l /opt/ricochet/ricochet_server

# Check if run.sh can execute
sudo -u ricochet /opt/ricochet/run.sh
```

### Service Keeps Restarting

```bash
# Check crash logs
sudo tail -100 /var/log/ricochet/stderr.log

# Check system resources
free -h
df -h

# Check database connectivity
psql -U ricochet -d ricochet -c "SELECT 1"
```

### Permission Errors

```bash
# Reset all permissions
sudo chown -R ricochet:ricochet /var/lib/ricochet
sudo chown -R ricochet:ricochet /var/log/ricochet
sudo chmod 750 /var/lib/ricochet /var/log/ricochet
sudo chmod 640 /etc/ricochet/env
```

### Supervisor Not Responding

```bash
# Check supervisor status
sudo systemctl status supervisor

# Restart supervisor
sudo systemctl restart supervisor

# Check supervisor logs
sudo tail -f /var/log/supervisor/supervisord.log
```

## Performance Tuning

### Adjust Auto-Restart Behavior

For more aggressive restart attempts:

```ini
[program:ricochet]
startretries=10        ; Try 10 times instead of 3
startsecs=5           ; Consider started after 5s instead of 10s
```

### Resource Limits

Add resource limits to prevent runaway processes:

```ini
[program:ricochet]
; Inherit environment (for ulimit)
environment=RICOCHET_MAX_MEMORY="2G"
```

## Upgrading

### With Debian Package

```bash
# Install new version
sudo dpkg -i ricochet-server_1.0.1.deb

# Service will auto-restart
# Configuration is preserved
```

### Manual Upgrade

```bash
# Stop service
sudo supervisorctl stop ricochet

# Backup current binary
sudo cp /opt/ricochet/ricochet_server /opt/ricochet/ricochet_server.bak

# Install new binary
sudo cp new_ricochet_server /opt/ricochet/ricochet_server
sudo chmod +x /opt/ricochet/ricochet_server

# Start service
sudo supervisorctl start ricochet
```

## Production Checklist

- [ ] Supervisor installed and running
- [ ] Ricochet package installed or files manually deployed
- [ ] Database password set in `/etc/ricochet/env`
- [ ] Database schema initialized
- [ ] Service starts successfully
- [ ] Auto-restart tested (kill process, watch it restart)
- [ ] Logs are being written
- [ ] Health checks configured
- [ ] Monitoring alerts set up
- [ ] Firewall allows P2P ports (4001)
- [ ] Backup strategy in place

## Comparison: Supervisord vs Systemd

| Feature | Supervisord | Systemd |
|---------|-------------|---------|
| Installation | Package install | Built-in |
| Configuration | Simple INI files | Unit files |
| Log viewing | `supervisorctl tail` | `journalctl` |
| Web UI | Yes (optional) | Via Cockpit |
| Containers | Works great | Problematic |
| Process groups | Native support | Needs slices |
| Learning curve | Low | Medium |
| Cross-platform | Yes | Linux only |

## See Also

- Main deployment guide: `doc/LINUX_POSTGRES_DEPLOYMENT.md`
- Package README: `deploy/README.md`
- Configuration examples: `config.postgres.example.yaml`

