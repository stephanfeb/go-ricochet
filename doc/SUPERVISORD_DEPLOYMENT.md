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

The build runs inside a Docker container (Ubuntu 22.04 with the Go toolchain
and `dpkg-deb`), so it needs Docker on the machine that builds and nothing
else. The package lands in `build/dist/`.

```bash
cd /path/to/ricochet
./build-deb.sh            # VERSION=1.0.1 ./build-deb.sh to set the version
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
- Installs the server, `run.sh`, `health_check.sh` and `schema.sql` under `/opt/ricochet`
- Installs `/etc/ricochet/config.yaml` (`deploy/config.yaml`: comments only, so the server runs on the production preset until you change something) with every key documented beside it in `config.yaml.example`

### Configure and Start

```bash
# 1. Set the database password and public IP. The package created this file
#    root:ricochet 640; run.sh passes the password to the server through the
#    environment, never on the command line.
sudo nano /etc/ricochet/env
# Set: DB_PASSWORD=your_password
#      EXTERNAL_IP=<the address clients reach this host on>
#      DB_SSLMODE=disable  only if PostgreSQL is on this host or a private
#                          network you trust: the package runs the server in
#                          production mode, which requires TLS to the database

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
go build -o ricochet_server ./cmd/ricochet
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
sudo cp deploy/health_check.sh /opt/ricochet/
sudo cp deploy/config.yaml /etc/ricochet/config.yaml
sudo cp config.example.yaml /etc/ricochet/config.yaml.example
sudo cp deploy/env.example /etc/ricochet/env

# Set permissions
sudo chmod +x /opt/ricochet/ricochet_server
sudo chmod +x /opt/ricochet/run.sh
sudo chmod +x /opt/ricochet/health_check.sh
sudo chown root:ricochet /etc/ricochet/env
sudo chmod 640 /etc/ricochet/env

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

The supervisor config (`/etc/supervisor/conf.d/ricochet.conf`, shipped as
`deploy/supervisor/ricochet.conf`) controls process management. The settings
that matter:

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

`stopwaitsecs=30` leaves room for a graceful stop. On TERM the server stops
admitting requests, lets the ones in flight finish for up to
`ops.shutdown_timeout` (5s by default) after `ops.drain_delay`, then closes
the network and the database pool. A second TERM while that is still running
makes it exit at once, and supervisor's KILL after 30s is the backstop behind
that.

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

The server serves its own health endpoints on the operator HTTP surface, bound
to loopback on port 9090 by default (see `ops` in `config.example.yaml`). Ask
the server rather than inferring its state from the outside:

- `/healthz` — the process is up. It touches no dependency on purpose, so a
  database outage does not get the process restarted for a fault a restart
  cannot fix.
- `/readyz` — the server can actually serve. It pings PostgreSQL and returns
  503 when the database is unreachable or when the server is shutting down.
  The body also carries admission and connection-pool figures, which is where
  to look first when the question is "why is it slow".

The package installs `/opt/ricochet/health_check.sh` (`deploy/health_check.sh`
in the repository). It asks both endpoints and exits with the Nagios codes,
so it drops into cron, Nagios or an external load-balancer check unchanged:

```bash
/opt/ricochet/health_check.sh
# OK: Ricochet is healthy            exit 0
# WARNING: Ricochet is up but not ready   exit 1, followed by the /readyz body
# CRITICAL: Ricochet not responding  exit 2
```

It reads the operator surface at `http://127.0.0.1:9090`; set `OPS` in its
environment if the `ops` section binds elsewhere.

Behind a load balancer, point the pool's health check at `/readyz` and set
`ops.drain_delay` to a couple of probe intervals. Without that delay the
instance stops answering in the same moment it reports itself unready, and the
load balancer discovers the shutdown through failed requests instead.

Schedule with cron, and put your own alerting command after the `||`:
```bash
# Check every 5 minutes; a failure lands in syslog as daemon.err
*/5 * * * * /opt/ricochet/health_check.sh >/dev/null || logger -t ricochet -p daemon.err "health check failed"
```

### Investigating storage

The same listener serves the operator's view of what the server is holding.
None of it needs a peer identity, a key or the client library, and it works for
mailboxes the caller does not own — which is the point, since an operator
chasing a complaint has a peer ID and nothing else.

```bash
OPS=http://127.0.0.1:9090

# Which mailboxes are closest to their cap, across every owner?
curl -s "$OPS/ops/mailboxes/top?n=20" | jq '.mailboxes[] | select(.full)'

# Everything one peer is storing.
curl -s "$OPS/ops/mailboxes?owner=12D3Koo..." | jq

# Who is using the space, and how much room is left?
curl -s "$OPS/ops/storage" | jq '{server, top: (.owners[:5])}'

# Did the limit I changed actually take effect?
curl -s "$OPS/ops/limits" | jq .admission
```

Two knobs matter here: `storage.near_capacity_ratio` sets how full a mailbox
must be before it counts toward `mailboxesNearCapacity` and the matching
Prometheus gauge — that is the number to alert on, so pick it from how quickly
you can act rather than leaving it at 0.9 — and `ops.query_timeout` bounds each
of these queries, which scan every mailbox.

Two things to know when reading the output. `/ops/storage` reports per-owner
totals live but takes the server-wide block from a sample refreshed on the
maintenance tick, so that block carries `sampledAt` and `ageSeconds` and is
`null` until the first sample lands — figures with an invisible age are worse
than an admitted absence. And `/ops/limits` reports *effective* values: a bound
the server derived rather than read is shown with the number it derived and a
flag saying so.

These views name peer IDs and disclose what each tenant is storing. Keep the
`ops` listener on loopback, or put something in front of it that authenticates.

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

The server has no memory knob; it bounds work, and memory follows. The bounds
are in `/etc/ricochet/config.yaml`: `performance.max_concurrent_connections`
(a hard cap, enforced by the libp2p resource manager), `admission_control`
(requests in flight against the database), `database.pool_size` and
`storage.mailbox_cache_size`. `curl -s http://127.0.0.1:9090/ops/limits` shows
the values in effect, including the ones the server derived rather than read.

Supervisor itself cannot cap memory. For a hard ceiling put the cgroup around
supervisord: `MemoryMax=` on the `supervisor` systemd unit covers every
process it starts.

## Upgrading

### With Debian Package

```bash
# Install new version
sudo dpkg -i ricochet-server_1.0.1.deb

# Configuration is preserved: config.yaml and env are conffiles and are
# never overwritten. The package stops the running server before the files
# change and starts it again on the new binary afterwards; a server that
# was not running stays stopped.
sudo supervisorctl status ricochet
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
- [ ] Firewall allows UDP 55223 in (or `LISTEN_PORT` from `/etc/ricochet/env`), and does not expose the operator port 9090, which stays on loopback
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

- Project overview and client library: `README.md`
- Every configuration key, with its default: `config.example.yaml`
- What each mailbox type allows: `doc/MAILBOX_LIFECYCLE_AND_ACLS.md`
- Capacity and scaling notes: `doc/SCALABILITY.md`

