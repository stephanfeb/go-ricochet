package core

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// ServerConfig holds all server configuration.
type ServerConfig struct {
	// Network
	Port              int      `yaml:"port" json:"port"`
	ListenAddresses   []string `yaml:"listen_addresses" json:"listenAddresses"`
	ExternalAddresses []string `yaml:"external_addresses" json:"externalAddresses"`
	BootstrapPeers    []string `yaml:"bootstrap_peers" json:"bootstrapPeers"`

	// Storage
	Storage               StorageBackendConfig `yaml:"storage" json:"storage"`
	DataDirectory         string               `yaml:"data_directory" json:"dataDirectory"`
	MaxStorageBytes       int64                `yaml:"max_storage_bytes" json:"maxStorageBytes"`
	RetentionPolicy       time.Duration        `yaml:"retention_policy" json:"retentionPolicy"`
	MaxMessagesPerMailbox int                  `yaml:"max_messages_per_mailbox" json:"maxMessagesPerMailbox"`
	// MaxMailboxes caps mailboxes across the whole server; a delivery or
	// create that would exceed it is refused.
	MaxMailboxes int `yaml:"max_mailboxes" json:"maxMailboxes"`
	// MaxMailboxesPerOwner caps the folders one identity may hold. Delivery
	// creates a folder on the sender's say-so, so without this any peer
	// could give any other peer an unbounded number of them.
	MaxMailboxesPerOwner int `yaml:"max_mailboxes_per_owner" json:"maxMailboxesPerOwner"`
	// MaxEntriesPerFeed caps entries in one feed. A feed's own max_entries
	// is a rolling window the owner chooses; this is the ceiling under it.
	MaxEntriesPerFeed int `yaml:"max_entries_per_feed" json:"maxEntriesPerFeed"`

	// NearCapacityRatio is the fill fraction at which a mailbox counts as
	// near capacity, driving ricochet_mailboxes_near_capacity and the figure
	// /ops/storage reports. It is the threshold an operator alerts on, so it
	// belongs to them rather than to the code: how much warning you want
	// before a mailbox starts rejecting depends on how quickly you can act.
	// Zero means the built-in 0.9.
	NearCapacityRatio float64 `yaml:"near_capacity_ratio" json:"nearCapacityRatio"`

	// Performance
	MaxConcurrentConnections int           `yaml:"max_concurrent_connections" json:"maxConcurrentConnections"`
	ConnectionTimeout        time.Duration `yaml:"connection_timeout" json:"connectionTimeout"`
	MessageTimeout           time.Duration `yaml:"message_timeout" json:"messageTimeout"`
	WorkerThreads            int           `yaml:"worker_threads" json:"workerThreads"`

	// Regional
	ServerRegion     string   `yaml:"server_region" json:"serverRegion"`
	SupportedRegions []string `yaml:"supported_regions" json:"supportedRegions"`

	// Service Coordination
	ServiceAnnouncementInterval time.Duration `yaml:"service_announcement_interval" json:"serviceAnnouncementInterval"`
	HealthCheckInterval         time.Duration `yaml:"health_check_interval" json:"healthCheckInterval"`
	CleanupInterval             time.Duration `yaml:"cleanup_interval" json:"cleanupInterval"`
	PresenceCheckInterval       time.Duration `yaml:"presence_check_interval" json:"presenceCheckInterval"`

	// Features
	EnableForwarding         bool `yaml:"enable_forwarding" json:"enableForwarding"`
	EnablePushDelivery       bool `yaml:"enable_push_delivery" json:"enablePushDelivery"`
	EnablePresenceMonitoring bool `yaml:"enable_presence_monitoring" json:"enablePresenceMonitoring"`
	EnablePresenceBroadcast  bool `yaml:"enable_presence_broadcast" json:"enablePresenceBroadcast"`
	EnableMetrics            bool `yaml:"enable_metrics" json:"enableMetrics"`
	EnableRelay              bool `yaml:"enable_relay" json:"enableRelay"`
	EnableRelayService       bool `yaml:"enable_relay_service" json:"enableRelayService"`
	EnableAutoRelay          bool `yaml:"enable_auto_relay" json:"enableAutoRelay"`
	EnableHolePunching       bool `yaml:"enable_hole_punching" json:"enableHolePunching"`
	EnableAutoNAT            bool `yaml:"enable_autonat" json:"enableAutoNAT"`

	// Presence broadcasting
	PresenceHeartbeatInterval time.Duration `yaml:"presence_heartbeat_interval" json:"presenceHeartbeatInterval"`
	PresenceTimeoutDuration   time.Duration `yaml:"presence_timeout_duration" json:"presenceTimeoutDuration"`
	PresenceBatchWindow       time.Duration `yaml:"presence_batch_window" json:"presenceBatchWindow"`

	// Security
	EnableAuthentication bool       `yaml:"enable_authentication" json:"enableAuthentication"`
	TrustedPeers         []string   `yaml:"trusted_peers" json:"trustedPeers"`
	RateLimits           RateLimits `yaml:"rate_limits" json:"rateLimits"`

	// Capacity
	Admission AdmissionControl `yaml:"admission_control" json:"admissionControl"`

	// Operator HTTP surface
	Ops OpsConfig `yaml:"ops" json:"ops"`

	// Relay limits
	RelayLimits RelayLimits `yaml:"relay_limits" json:"relayLimits"`

	// Identity
	IdentityFile string `yaml:"identity_file" json:"identityFile,omitempty"`
}

// EffectiveNearCapacityRatio returns the configured threshold, or 0.9.
func (c *ServerConfig) EffectiveNearCapacityRatio() float64 {
	if c.NearCapacityRatio <= 0 {
		return 0.9
	}
	return c.NearCapacityRatio
}

// AdmissionControl bounds how much work is in flight at once instead of
// capping how many requests a peer may make per unit of time.
//
// This is the mechanism that governs throughput. It has no ceiling expressed
// in documents or requests per minute: work is admitted as fast as it can be
// completed, so throughput rises with the hardware and falls on its own when
// the database slows. A request that cannot be admitted before AcquireTimeout
// is shed with a 503, which is a signal to add capacity rather than a verdict
// on the client.
type AdmissionControl struct {
	Enabled bool `yaml:"enabled" json:"enabled"`

	// MaxInFlight bounds concurrent requests server-wide. Zero derives it
	// from the database pool size, which is the resource it is protecting.
	MaxInFlight int `yaml:"max_in_flight" json:"maxInFlight"`

	// MaxInFlightPerPeer stops one client occupying every slot. This is what
	// makes it safe to run with per-peer rate limiting switched off. Zero
	// disables the per-peer bound.
	MaxInFlightPerPeer int `yaml:"max_in_flight_per_peer" json:"maxInFlightPerPeer"`

	// AcquireTimeout is how long a request waits for a slot before being
	// shed. Waiting is normal and is how a client is paced to the database;
	// only a genuinely saturated server should reach this.
	AcquireTimeout time.Duration `yaml:"acquire_timeout" json:"acquireTimeout"`
}

// inFlightPerPoolConnection is how many requests may be in flight per database
// connection. Above one because not every request is inside a query for its
// whole life -- there is decoding, validation and encoding either side -- so a
// little oversubscription keeps the pool busy without queueing on it.
const inFlightPerPoolConnection = 4

// DefaultAdmissionControl returns the built-in admission settings.
func DefaultAdmissionControl() AdmissionControl {
	return AdmissionControl{
		Enabled:            true,
		MaxInFlight:        0, // derived from the pool size
		MaxInFlightPerPeer: 64,
		AcquireTimeout:     5 * time.Second,
	}
}

// EffectiveMaxInFlight returns the configured global bound, deriving one from
// the database pool size when it is not set explicitly.
func (a AdmissionControl) EffectiveMaxInFlight(poolSize int) int {
	if a.MaxInFlight > 0 {
		return a.MaxInFlight
	}
	if poolSize <= 0 {
		poolSize = 25
	}
	return poolSize * inFlightPerPoolConnection
}

// EffectiveAcquireTimeout returns the configured timeout, or five seconds.
func (a AdmissionControl) EffectiveAcquireTimeout() time.Duration {
	if a.AcquireTimeout <= 0 {
		return 5 * time.Second
	}
	return a.AcquireTimeout
}

// OpsConfig configures the operator HTTP surface: liveness, readiness, and
// optionally pprof and Prometheus metrics.
//
// It binds to loopback by default. The surface exposes peer identifiers,
// storage figures and internal topology, so reaching it from off-host is an
// explicit decision rather than the default.
type OpsConfig struct {
	Enabled bool `yaml:"enabled" json:"enabled"`

	// Bind is the interface to listen on. Empty means loopback.
	Bind string `yaml:"bind" json:"bind"`

	// Port is the TCP port. Zero binds an ephemeral port, which is useful in
	// tests; the chosen port is logged at startup.
	Port int `yaml:"port" json:"port"`

	// EnablePprof mounts /debug/pprof. It is off by default: the profiles
	// include goroutine stacks and heap contents.
	EnablePprof bool `yaml:"enable_pprof" json:"enablePprof"`

	// ReadinessTimeout bounds how long /readyz will wait on its checks. A
	// readiness probe that hangs is worse than one that fails.
	ReadinessTimeout time.Duration `yaml:"readiness_timeout" json:"readinessTimeout"`

	// QueryTimeout bounds each /ops/* query. Those endpoints scan every
	// mailbox, and how long that takes is a property of the database, not
	// something a constant can know. Zero means ten seconds.
	QueryTimeout time.Duration `yaml:"query_timeout" json:"queryTimeout"`

	// ShutdownTimeout bounds how long Stop waits for in-flight ops requests.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdownTimeout"`

	// DrainDelay is how long shutdown keeps serving after /readyz starts
	// reporting unready. Without it the instance disappears in the same
	// instant it announces its withdrawal, and a load balancer never observes
	// the 503 — it discovers the shutdown through failed requests instead.
	// Set it to a couple of probe intervals. Zero, the default, skips the
	// wait, which is right when nothing is probing.
	DrainDelay time.Duration `yaml:"drain_delay" json:"drainDelay"`
}

// DefaultOpsConfig returns the built-in operator surface settings.
func DefaultOpsConfig() OpsConfig {
	return OpsConfig{
		Enabled:          true,
		Bind:             "127.0.0.1",
		Port:             9090,
		EnablePprof:      false,
		ReadinessTimeout: 2 * time.Second,
		QueryTimeout:     10 * time.Second,
		ShutdownTimeout:  5 * time.Second,
		DrainDelay:       0,
	}
}

// EffectiveBind returns the configured bind address, defaulting to loopback.
func (o OpsConfig) EffectiveBind() string {
	if o.Bind == "" {
		return "127.0.0.1"
	}
	return o.Bind
}

// Address returns the host:port the operator surface listens on.
func (o OpsConfig) Address() string {
	return net.JoinHostPort(o.EffectiveBind(), strconv.Itoa(o.Port))
}

// EffectiveReadinessTimeout returns the readiness deadline, or two seconds.
// EffectiveQueryTimeout returns the configured bound on an /ops/* query, or
// ten seconds.
func (o OpsConfig) EffectiveQueryTimeout() time.Duration {
	if o.QueryTimeout <= 0 {
		return 10 * time.Second
	}
	return o.QueryTimeout
}

func (o OpsConfig) EffectiveReadinessTimeout() time.Duration {
	if o.ReadinessTimeout <= 0 {
		return 2 * time.Second
	}
	return o.ReadinessTimeout
}

// EffectiveShutdownTimeout returns the shutdown deadline, or five seconds.
func (o OpsConfig) EffectiveShutdownTimeout() time.Duration {
	if o.ShutdownTimeout <= 0 {
		return 5 * time.Second
	}
	return o.ShutdownTimeout
}

// Protocol keys for per-protocol rate limits. Each names one limiter.
const (
	RateLimitMSA      = "msa"       // message submission
	RateLimitMSABatch = "msa_batch" // batch message submission
	RateLimitMAA      = "maa"       // message access
	RateLimitMMA      = "mma"       // mailbox administration
	RateLimitSDA      = "sda"       // documents
	RateLimitSFA      = "sfa"       // feeds
	RateLimitSCA      = "sca"       // collections
	RateLimitMTA      = "mta"       // routing backstop, charged per message
)

// Limit is one bucket's allowance: a sustained Rate replenished over
// RateLimits.Window, plus a Burst that a peer may accumulate while idle and
// spend at once.
//
// A negative Rate (RateUnlimited) disables the bucket, which is the default.
// A Rate of zero means "not specified" when merging a config file over the
// defaults, so it is not a way to switch a limit off. A Burst of zero defaults
// to Rate, reproducing the classic "N requests per window" behaviour; a Burst
// below Rate paces a peer without lowering its sustained throughput.
type Limit struct {
	Rate  int `yaml:"rate" json:"rate"`
	Burst int `yaml:"burst" json:"burst"`
}

// RateUnlimited is the Rate value that switches a bucket off.
const RateUnlimited = -1

// ProtocolLimits configures one protocol's limiter.
//
// Protocols with a single bucket (MSA, MAA, MMA, and the MTA backstop) use
// Requests. Protocols that separate cheap reads from expensive writes (SDA,
// SFA, SCA) use Read and Write instead.
type ProtocolLimits struct {
	Requests Limit `yaml:"requests" json:"requests"`
	Read     Limit `yaml:"read" json:"read"`
	Write    Limit `yaml:"write" json:"write"`
}

// RateLimits configures optional per-peer rate limiting.
//
// These are off by default. Throughput is governed by AdmissionControl, which
// bounds concurrent work rather than requests per minute and therefore has no
// ceiling to outgrow. A per-peer rate limit is a constant somebody guessed: it
// caps a well-behaved client long before the hardware is busy, and it is the
// wrong tool for the one thing it looks like it is for, since a saturated
// server needs to shed whatever is arriving rather than whatever a peer's
// budget says.
//
// They remain available for deployments that need a per-tenant cap for
// non-capacity reasons -- billing tiers, untrusted peers, containing a client
// known to loop. Enable one by giving it a positive rate.
//
// When enabled, limits are counted per request, not per unit of work: one
// BATCH_PUT costs the same as one PUT regardless of how many documents it
// carries.
type RateLimits struct {
	Window    time.Duration             `yaml:"window" json:"window"`
	Protocols map[string]ProtocolLimits `yaml:"protocols" json:"protocols"`
}

// DefaultRateLimits returns the built-in per-protocol limits, which are off.
//
// Every protocol is listed explicitly with a negative rate rather than left
// out of the map. The distinction matters: a missing entry would also read as
// "no limit", but silently, and there would be no way to tell a deliberate
// choice from a protocol somebody forgot to add.
func DefaultRateLimits() RateLimits {
	off := Limit{Rate: RateUnlimited}
	unlimited := ProtocolLimits{Requests: off, Read: off, Write: off}

	return RateLimits{
		Window: 1 * time.Minute,
		Protocols: map[string]ProtocolLimits{
			RateLimitMSA:      unlimited,
			RateLimitMSABatch: unlimited,
			RateLimitMAA:      unlimited,
			RateLimitMMA:      unlimited,
			RateLimitSDA:      unlimited,
			RateLimitSFA:      unlimited,
			RateLimitSCA:      unlimited,
			RateLimitMTA:      unlimited,
		},
	}
}

// For returns the limits configured for a protocol, falling back to the
// built-in default for any protocol a partial configuration omits. Falling
// back rather than returning a zero value matters: a zero Rate disables
// limiting, so an unlisted protocol would otherwise be silently unprotected.
func (r RateLimits) For(protocol string) ProtocolLimits {
	if limits, ok := r.Protocols[protocol]; ok {
		return limits
	}
	return DefaultRateLimits().Protocols[protocol]
}

// EffectiveWindow returns the configured window, or one minute if unset.
func (r RateLimits) EffectiveWindow() time.Duration {
	if r.Window <= 0 {
		return 1 * time.Minute
	}
	return r.Window
}

// RelayLimits configures circuit relay v2 service resource limits.
// Zero values mean "use go-libp2p defaults".
type RelayLimits struct {
	MaxReservations        int           `yaml:"max_reservations" json:"maxReservations"`
	MaxCircuits            int           `yaml:"max_circuits" json:"maxCircuits"`
	BufferSize             int           `yaml:"buffer_size" json:"bufferSize"`
	MaxReservationsPerPeer int           `yaml:"max_reservations_per_peer" json:"maxReservationsPerPeer"`
	MaxReservationsPerIP   int           `yaml:"max_reservations_per_ip" json:"maxReservationsPerIP"`
	MaxReservationsPerASN  int           `yaml:"max_reservations_per_asn" json:"maxReservationsPerASN"`
	ReservationTTL         time.Duration `yaml:"reservation_ttl" json:"reservationTTL"`
	ConnectionDuration     time.Duration `yaml:"connection_duration" json:"connectionDuration"`
	ConnectionData         int64         `yaml:"connection_data" json:"connectionData"`
}

// StorageBackendConfig configures the storage backend.
type StorageBackendConfig struct {
	Backend  string          `yaml:"backend" json:"backend"`
	Postgres *PostgresConfig `yaml:"postgres,omitempty" json:"postgres,omitempty"`
}

// UsePostgres returns true if the backend is PostgreSQL.
func (c *StorageBackendConfig) UsePostgres() bool {
	return c.Backend == "postgres"
}

// PostgresConfig holds PostgreSQL connection configuration.
type PostgresConfig struct {
	Host           string        `yaml:"host" json:"host"`
	Port           int           `yaml:"port" json:"port"`
	Database       string        `yaml:"database" json:"database"`
	Username       string        `yaml:"username" json:"username"`
	Password       string        `yaml:"password" json:"password"`
	PoolSize       int           `yaml:"pool_size" json:"poolSize"`
	SSLMode        string        `yaml:"ssl_mode" json:"sslMode"`
	ConnectTimeout time.Duration `yaml:"connect_timeout" json:"connectTimeout"`
}

// ConnectionURI returns the PostgreSQL connection URI.
func (c *PostgresConfig) ConnectionURI() string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		c.Username, c.Password, c.Host, c.Port, c.Database, c.SSLMode)
}

// DefaultConfig returns the default server configuration.
func DefaultConfig() *ServerConfig {
	return &ServerConfig{
		Port:                  55223,
		ListenAddresses:       []string{"/ip4/0.0.0.0/udp/55223/udx"},
		DataDirectory:         "./sf_storage",
		MaxStorageBytes:       10 * 1024 * 1024 * 1024, // 10GB
		RetentionPolicy:       30 * 24 * time.Hour,     // 30 days
		MaxMessagesPerMailbox: 1000,
		MaxMailboxes:          100000,
		MaxMailboxesPerOwner:  100,
		MaxEntriesPerFeed:     10000,
		NearCapacityRatio:     0.9,

		Storage: StorageBackendConfig{
			Backend: "postgres",
			Postgres: &PostgresConfig{
				Host:           "localhost",
				Port:           5432,
				Database:       "ricochet",
				Username:       "ricochet",
				Password:       "",
				PoolSize:       25,
				SSLMode:        "require",
				ConnectTimeout: 30 * time.Second,
			},
		},

		MaxConcurrentConnections: 10000,
		ConnectionTimeout:        30 * time.Second,
		MessageTimeout:           10 * time.Second,
		WorkerThreads:            4,

		ServerRegion:     "default",
		SupportedRegions: []string{"default"},

		ServiceAnnouncementInterval: 1 * time.Hour,
		HealthCheckInterval:         5 * time.Minute,
		CleanupInterval:             5 * time.Minute,
		PresenceCheckInterval:       30 * time.Second,

		EnablePushDelivery:       true,
		EnablePresenceMonitoring: true,
		EnablePresenceBroadcast:  true,
		EnableMetrics:            true,
		EnableAutoNAT:            true,

		PresenceHeartbeatInterval: 60 * time.Second,
		PresenceTimeoutDuration:   120 * time.Second,
		PresenceBatchWindow:       2 * time.Second,

		EnableRelay:        true,
		EnableRelayService: true,

		RateLimits: DefaultRateLimits(),
		Admission:  DefaultAdmissionControl(),
		Ops:        DefaultOpsConfig(),
	}
}

// DevelopmentConfig returns a development-friendly configuration.
func DevelopmentConfig() *ServerConfig {
	cfg := DefaultConfig()
	cfg.MaxStorageBytes = 1 * 1024 * 1024 * 1024 // 1GB
	cfg.MaxConcurrentConnections = 100
	cfg.ServiceAnnouncementInterval = 1 * time.Minute
	cfg.HealthCheckInterval = 30 * time.Second
	cfg.CleanupInterval = 1 * time.Minute
	cfg.Storage.Postgres.SSLMode = "disable"
	return cfg
}

// ProductionConfig returns a production configuration.
func ProductionConfig() *ServerConfig {
	cfg := DefaultConfig()
	cfg.MaxStorageBytes = 50 * 1024 * 1024 * 1024 // 50GB
	cfg.MaxConcurrentConnections = 10000
	cfg.Storage.Postgres.PoolSize = 50
	cfg.EnableAuthentication = true
	return cfg
}

// HighCapacityConfig returns a high-capacity configuration.
func HighCapacityConfig() *ServerConfig {
	cfg := DefaultConfig()
	cfg.MaxStorageBytes = 100 * 1024 * 1024 * 1024 // 100GB
	cfg.MaxConcurrentConnections = 50000
	cfg.EnableForwarding = true
	cfg.EnableAuthentication = true
	cfg.WorkerThreads = 8
	return cfg
}

// Validate checks the configuration for errors.
func (c *ServerConfig) Validate() error {
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Port)
	}
	if c.MaxStorageBytes <= 0 {
		return fmt.Errorf("max_storage_bytes must be positive")
	}
	if c.MaxMessagesPerMailbox <= 0 {
		return fmt.Errorf("max_messages_per_mailbox must be positive")
	}
	if c.MaxMailboxes <= 0 {
		return fmt.Errorf("max_mailboxes must be positive")
	}
	if c.MaxMailboxesPerOwner <= 0 {
		return fmt.Errorf("max_mailboxes_per_owner must be positive")
	}
	if c.MaxEntriesPerFeed <= 0 {
		return fmt.Errorf("max_entries_per_feed must be positive")
	}
	// A ratio above 1 would make the near-capacity count silently unreachable
	// for a mailbox that is merely full, which is the state it exists to warn
	// about. Zero means "use the default" and is handled downstream.
	if c.NearCapacityRatio < 0 || c.NearCapacityRatio > 1 {
		return fmt.Errorf("storage.near_capacity_ratio must be between 0 and 1, got %v", c.NearCapacityRatio)
	}
	if c.Admission.MaxInFlight < 0 {
		return fmt.Errorf("admission_control.max_in_flight must not be negative")
	}
	if c.Admission.MaxInFlightPerPeer < 0 {
		return fmt.Errorf("admission_control.max_in_flight_per_peer must not be negative")
	}
	if c.Ops.Port < 0 || c.Ops.Port > 65535 {
		return fmt.Errorf("ops.port out of range: %d", c.Ops.Port)
	}
	if c.Ops.ReadinessTimeout < 0 {
		return fmt.Errorf("ops.readiness_timeout must not be negative")
	}
	if c.Ops.ShutdownTimeout < 0 {
		return fmt.Errorf("ops.shutdown_timeout must not be negative")
	}
	if c.Ops.DrainDelay < 0 {
		return fmt.Errorf("ops.drain_delay must not be negative")
	}
	if c.RateLimits.Window < 0 {
		return fmt.Errorf("rate limit window must not be negative")
	}
	for name, limits := range c.RateLimits.Protocols {
		for bucket, limit := range map[string]Limit{
			"requests": limits.Requests,
			"read":     limits.Read,
			"write":    limits.Write,
		} {
			if limit.Burst < 0 {
				return fmt.Errorf("rate limit %s.%s: burst must not be negative", name, bucket)
			}
		}
	}
	if c.Storage.UsePostgres() && c.Storage.Postgres == nil {
		return fmt.Errorf("postgres config required when backend is 'postgres'")
	}
	if c.EnableAutoRelay && !c.EnableRelay {
		return fmt.Errorf("enable_auto_relay requires enable_relay")
	}
	if c.EnableHolePunching && !c.EnableRelay {
		return fmt.Errorf("enable_hole_punching requires enable_relay")
	}
	if c.EnableRelayService && !c.EnableRelay {
		return fmt.Errorf("enable_relay_service requires enable_relay")
	}
	return nil
}

// MaxStorageHuman returns a human-readable storage limit.
func (c *ServerConfig) MaxStorageHuman() string {
	gb := float64(c.MaxStorageBytes) / (1024 * 1024 * 1024)
	if gb >= 1 {
		return fmt.Sprintf("%.0fGB", gb)
	}
	mb := float64(c.MaxStorageBytes) / (1024 * 1024)
	return fmt.Sprintf("%.0fMB", mb)
}

// yamlFileConfig mirrors the nested YAML config file structure.
type yamlFileConfig struct {
	Server struct {
		Port              int      `yaml:"port"`
		Region            string   `yaml:"region"`
		IdentityFile      string   `yaml:"identity_file"`
		ListenAddresses   []string `yaml:"listen_addresses"`
		ExternalAddresses []string `yaml:"external_addresses"`
		BootstrapPeers    []string `yaml:"bootstrap_peers"`
	} `yaml:"server"`

	Storage struct {
		DataDirectory string `yaml:"data_directory"`
		MaxStorageGB  int    `yaml:"max_storage_gb"`
		RetentionDays int    `yaml:"retention_days"`
		MaxMessages   int    `yaml:"max_messages_per_mailbox"`
		MaxMailboxes  int    `yaml:"max_mailboxes"`

		MaxMailboxesPerOwner int `yaml:"max_mailboxes_per_owner"`
		MaxEntriesPerFeed    int `yaml:"max_entries_per_feed"`

		NearCapacityRatio float64 `yaml:"near_capacity_ratio"`
	} `yaml:"storage"`

	Database struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Name     string `yaml:"name"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		SSLMode  string `yaml:"sslmode"`
		PoolSize int    `yaml:"pool_size"`
	} `yaml:"database"`

	Performance struct {
		MaxConcurrentConnections int `yaml:"max_concurrent_connections"`
		ConnectionTimeoutSec     int `yaml:"connection_timeout_sec"`
		MessageTimeoutSec        int `yaml:"message_timeout_sec"`
		WorkerThreads            int `yaml:"worker_threads"`
	} `yaml:"performance"`

	Intervals struct {
		ServiceAnnouncementMin int `yaml:"service_announcement_min"`
		HealthCheckMin         int `yaml:"health_check_min"`
		CleanupMin             int `yaml:"cleanup_min"`
		PresenceCheckSec       int `yaml:"presence_check_sec"`
		PresenceHeartbeatSec   int `yaml:"presence_heartbeat_sec"`
		PresenceTimeoutSec     int `yaml:"presence_timeout_sec"`
	} `yaml:"intervals"`

	Features struct {
		EnableForwarding         bool `yaml:"enable_forwarding"`
		EnablePushDelivery       bool `yaml:"enable_push_delivery"`
		EnablePresenceMonitoring bool `yaml:"enable_presence_monitoring"`
		EnablePresenceBroadcast  bool `yaml:"enable_presence_broadcast"`
		EnableMetrics            bool `yaml:"enable_metrics"`
		EnableAuthentication     bool `yaml:"enable_authentication"`
		EnableRelay              bool `yaml:"enable_relay"`
		EnableRelayService       bool `yaml:"enable_relay_service"`
		EnableAutoRelay          bool `yaml:"enable_auto_relay"`
		EnableHolePunching       bool `yaml:"enable_hole_punching"`
		EnableAutoNAT            bool `yaml:"enable_autonat"`
	} `yaml:"features"`

	RelayLimits struct {
		MaxReservations        int   `yaml:"max_reservations"`
		MaxCircuits            int   `yaml:"max_circuits"`
		BufferSize             int   `yaml:"buffer_size"`
		MaxReservationsPerPeer int   `yaml:"max_reservations_per_peer"`
		MaxReservationsPerIP   int   `yaml:"max_reservations_per_ip"`
		MaxReservationsPerASN  int   `yaml:"max_reservations_per_asn"`
		ReservationTTLMin      int   `yaml:"reservation_ttl_min"`
		ConnectionDurationSec  int   `yaml:"connection_duration_sec"`
		ConnectionData         int64 `yaml:"connection_data"`
	} `yaml:"relay_limits"`

	RateLimiting struct {
		// Legacy keys, still honoured. max_requests_per_window fed only the
		// MTA router and continues to; per-protocol entries below supersede it.
		WindowMinutes        int `yaml:"window_minutes"`
		MaxRequestsPerWindow int `yaml:"max_requests_per_window"`

		Window    string                    `yaml:"window"`
		Protocols map[string]ProtocolLimits `yaml:"protocols"`
	} `yaml:"rate_limiting"`

	AdmissionControl struct {
		Enabled            *bool  `yaml:"enabled"`
		MaxInFlight        int    `yaml:"max_in_flight"`
		MaxInFlightPerPeer *int   `yaml:"max_in_flight_per_peer"`
		AcquireTimeout     string `yaml:"acquire_timeout"`
	} `yaml:"admission_control"`

	Ops struct {
		Enabled          *bool  `yaml:"enabled"`
		Bind             string `yaml:"bind"`
		Port             *int   `yaml:"port"`
		EnablePprof      *bool  `yaml:"enable_pprof"`
		ReadinessTimeout string `yaml:"readiness_timeout"`
		QueryTimeout     string `yaml:"query_timeout"`
		ShutdownTimeout  string `yaml:"shutdown_timeout"`
		DrainDelay       string `yaml:"drain_delay"`
	} `yaml:"ops"`
}

// LoadConfigFromFile reads a YAML config file and applies its values on top of
// the provided base config. Only non-zero/non-empty values from the file
// override the base.
func LoadConfigFromFile(path string, base *ServerConfig) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}

	var yc yamlFileConfig
	if err := yaml.Unmarshal(data, &yc); err != nil {
		return fmt.Errorf("parse config file: %w", err)
	}

	// Server section
	if yc.Server.Port > 0 {
		base.Port = yc.Server.Port
	}
	if yc.Server.Region != "" {
		base.ServerRegion = yc.Server.Region
	}
	if yc.Server.IdentityFile != "" {
		base.IdentityFile = yc.Server.IdentityFile
	}
	if len(yc.Server.ListenAddresses) > 0 {
		base.ListenAddresses = yc.Server.ListenAddresses
	}
	if len(yc.Server.ExternalAddresses) > 0 {
		base.ExternalAddresses = yc.Server.ExternalAddresses
	}
	if len(yc.Server.BootstrapPeers) > 0 {
		base.BootstrapPeers = yc.Server.BootstrapPeers
	}

	// Storage section
	if yc.Storage.DataDirectory != "" {
		base.DataDirectory = yc.Storage.DataDirectory
	}
	if yc.Storage.MaxStorageGB > 0 {
		base.MaxStorageBytes = int64(yc.Storage.MaxStorageGB) * 1024 * 1024 * 1024
	}
	if yc.Storage.RetentionDays > 0 {
		base.RetentionPolicy = time.Duration(yc.Storage.RetentionDays) * 24 * time.Hour
	}
	if yc.Storage.MaxMessages > 0 {
		base.MaxMessagesPerMailbox = yc.Storage.MaxMessages
	}
	if yc.Storage.MaxMailboxes > 0 {
		base.MaxMailboxes = yc.Storage.MaxMailboxes
	}
	if yc.Storage.MaxMailboxesPerOwner > 0 {
		base.MaxMailboxesPerOwner = yc.Storage.MaxMailboxesPerOwner
	}
	if yc.Storage.MaxEntriesPerFeed > 0 {
		base.MaxEntriesPerFeed = yc.Storage.MaxEntriesPerFeed
	}
	if yc.Storage.NearCapacityRatio != 0 {
		base.NearCapacityRatio = yc.Storage.NearCapacityRatio
	}

	// Database section
	if base.Storage.Postgres == nil {
		base.Storage.Postgres = &PostgresConfig{}
	}
	if yc.Database.Host != "" {
		base.Storage.Postgres.Host = yc.Database.Host
	}
	if yc.Database.Port > 0 {
		base.Storage.Postgres.Port = yc.Database.Port
	}
	if yc.Database.Name != "" {
		base.Storage.Postgres.Database = yc.Database.Name
	}
	if yc.Database.Username != "" {
		base.Storage.Postgres.Username = yc.Database.Username
	}
	if yc.Database.Password != "" {
		base.Storage.Postgres.Password = yc.Database.Password
	}
	if yc.Database.SSLMode != "" {
		base.Storage.Postgres.SSLMode = yc.Database.SSLMode
	}
	if yc.Database.PoolSize > 0 {
		base.Storage.Postgres.PoolSize = yc.Database.PoolSize
	}

	// Performance section
	if yc.Performance.MaxConcurrentConnections > 0 {
		base.MaxConcurrentConnections = yc.Performance.MaxConcurrentConnections
	}
	if yc.Performance.ConnectionTimeoutSec > 0 {
		base.ConnectionTimeout = time.Duration(yc.Performance.ConnectionTimeoutSec) * time.Second
	}
	if yc.Performance.MessageTimeoutSec > 0 {
		base.MessageTimeout = time.Duration(yc.Performance.MessageTimeoutSec) * time.Second
	}
	if yc.Performance.WorkerThreads > 0 {
		base.WorkerThreads = yc.Performance.WorkerThreads
	}

	// Intervals section
	if yc.Intervals.ServiceAnnouncementMin > 0 {
		base.ServiceAnnouncementInterval = time.Duration(yc.Intervals.ServiceAnnouncementMin) * time.Minute
	}
	if yc.Intervals.HealthCheckMin > 0 {
		base.HealthCheckInterval = time.Duration(yc.Intervals.HealthCheckMin) * time.Minute
	}
	if yc.Intervals.CleanupMin > 0 {
		base.CleanupInterval = time.Duration(yc.Intervals.CleanupMin) * time.Minute
	}
	if yc.Intervals.PresenceCheckSec > 0 {
		base.PresenceCheckInterval = time.Duration(yc.Intervals.PresenceCheckSec) * time.Second
	}
	if yc.Intervals.PresenceHeartbeatSec > 0 {
		base.PresenceHeartbeatInterval = time.Duration(yc.Intervals.PresenceHeartbeatSec) * time.Second
	}
	if yc.Intervals.PresenceTimeoutSec > 0 {
		base.PresenceTimeoutDuration = time.Duration(yc.Intervals.PresenceTimeoutSec) * time.Second
	}

	// Features section — booleans are applied unconditionally since we can't
	// distinguish "not set" from "set to false" in YAML. The file should only
	// contain values the operator intends.
	base.EnableForwarding = yc.Features.EnableForwarding
	base.EnablePushDelivery = yc.Features.EnablePushDelivery
	base.EnablePresenceMonitoring = yc.Features.EnablePresenceMonitoring
	base.EnablePresenceBroadcast = yc.Features.EnablePresenceBroadcast
	base.EnableMetrics = yc.Features.EnableMetrics
	base.EnableAuthentication = yc.Features.EnableAuthentication
	base.EnableRelay = yc.Features.EnableRelay
	base.EnableRelayService = yc.Features.EnableRelayService
	base.EnableAutoRelay = yc.Features.EnableAutoRelay
	base.EnableHolePunching = yc.Features.EnableHolePunching
	base.EnableAutoNAT = yc.Features.EnableAutoNAT

	// Relay limits section
	if yc.RelayLimits.MaxReservations > 0 {
		base.RelayLimits.MaxReservations = yc.RelayLimits.MaxReservations
	}
	if yc.RelayLimits.MaxCircuits > 0 {
		base.RelayLimits.MaxCircuits = yc.RelayLimits.MaxCircuits
	}
	if yc.RelayLimits.BufferSize > 0 {
		base.RelayLimits.BufferSize = yc.RelayLimits.BufferSize
	}
	if yc.RelayLimits.MaxReservationsPerPeer > 0 {
		base.RelayLimits.MaxReservationsPerPeer = yc.RelayLimits.MaxReservationsPerPeer
	}
	if yc.RelayLimits.MaxReservationsPerIP > 0 {
		base.RelayLimits.MaxReservationsPerIP = yc.RelayLimits.MaxReservationsPerIP
	}
	if yc.RelayLimits.MaxReservationsPerASN > 0 {
		base.RelayLimits.MaxReservationsPerASN = yc.RelayLimits.MaxReservationsPerASN
	}
	if yc.RelayLimits.ReservationTTLMin > 0 {
		base.RelayLimits.ReservationTTL = time.Duration(yc.RelayLimits.ReservationTTLMin) * time.Minute
	}
	if yc.RelayLimits.ConnectionDurationSec > 0 {
		base.RelayLimits.ConnectionDuration = time.Duration(yc.RelayLimits.ConnectionDurationSec) * time.Second
	}
	if yc.RelayLimits.ConnectionData > 0 {
		base.RelayLimits.ConnectionData = yc.RelayLimits.ConnectionData
	}

	// Rate limiting section
	if err := applyRateLimits(yc, base); err != nil {
		return err
	}

	// Admission control section
	if err := applyAdmissionControl(yc, base); err != nil {
		return err
	}

	// Operator HTTP surface
	if err := applyOps(yc, base); err != nil {
		return err
	}

	return nil
}

// applyOps folds the file's ops section into base.
//
// Enabled, Port and EnablePprof are pointers in the YAML struct because their
// zero values are meaningful: the surface is on by default, and port 0 is a
// legitimate request for an ephemeral port.
func applyOps(yc yamlFileConfig, base *ServerConfig) error {
	ops := &base.Ops

	if yc.Ops.Enabled != nil {
		ops.Enabled = *yc.Ops.Enabled
	}
	if yc.Ops.Bind != "" {
		ops.Bind = yc.Ops.Bind
	}
	if yc.Ops.Port != nil {
		if *yc.Ops.Port < 0 || *yc.Ops.Port > 65535 {
			return fmt.Errorf("ops.port out of range: %d", *yc.Ops.Port)
		}
		ops.Port = *yc.Ops.Port
	}
	if yc.Ops.EnablePprof != nil {
		ops.EnablePprof = *yc.Ops.EnablePprof
	}
	if yc.Ops.ReadinessTimeout != "" {
		d, err := time.ParseDuration(yc.Ops.ReadinessTimeout)
		if err != nil {
			return fmt.Errorf("parse ops.readiness_timeout: %w", err)
		}
		if d <= 0 {
			return fmt.Errorf("ops.readiness_timeout must be positive, got %s", yc.Ops.ReadinessTimeout)
		}
		ops.ReadinessTimeout = d
	}
	if yc.Ops.QueryTimeout != "" {
		d, err := time.ParseDuration(yc.Ops.QueryTimeout)
		if err != nil {
			return fmt.Errorf("parse ops.query_timeout: %w", err)
		}
		if d <= 0 {
			return fmt.Errorf("ops.query_timeout must be positive, got %s", yc.Ops.QueryTimeout)
		}
		ops.QueryTimeout = d
	}
	if yc.Ops.ShutdownTimeout != "" {
		d, err := time.ParseDuration(yc.Ops.ShutdownTimeout)
		if err != nil {
			return fmt.Errorf("parse ops.shutdown_timeout: %w", err)
		}
		if d <= 0 {
			return fmt.Errorf("ops.shutdown_timeout must be positive, got %s", yc.Ops.ShutdownTimeout)
		}
		ops.ShutdownTimeout = d
	}
	if yc.Ops.DrainDelay != "" {
		d, err := time.ParseDuration(yc.Ops.DrainDelay)
		if err != nil {
			return fmt.Errorf("parse ops.drain_delay: %w", err)
		}
		if d < 0 {
			return fmt.Errorf("ops.drain_delay must not be negative, got %s", yc.Ops.DrainDelay)
		}
		ops.DrainDelay = d
	}

	return nil
}

// applyAdmissionControl folds the file's admission_control section into base.
//
// Enabled and MaxInFlightPerPeer are pointers in the YAML struct so that an
// explicit "false" or "0" is distinguishable from an absent key. Both have
// non-zero defaults, so treating zero as "unset" would make them impossible
// to switch off from a config file.
func applyAdmissionControl(yc yamlFileConfig, base *ServerConfig) error {
	ac := &base.Admission

	if yc.AdmissionControl.Enabled != nil {
		ac.Enabled = *yc.AdmissionControl.Enabled
	}
	if yc.AdmissionControl.MaxInFlight > 0 {
		ac.MaxInFlight = yc.AdmissionControl.MaxInFlight
	}
	if yc.AdmissionControl.MaxInFlightPerPeer != nil {
		if *yc.AdmissionControl.MaxInFlightPerPeer < 0 {
			return fmt.Errorf("admission_control.max_in_flight_per_peer must not be negative")
		}
		ac.MaxInFlightPerPeer = *yc.AdmissionControl.MaxInFlightPerPeer
	}
	if yc.AdmissionControl.AcquireTimeout != "" {
		d, err := time.ParseDuration(yc.AdmissionControl.AcquireTimeout)
		if err != nil {
			return fmt.Errorf("parse admission_control.acquire_timeout: %w", err)
		}
		if d <= 0 {
			return fmt.Errorf("admission_control.acquire_timeout must be positive, got %s", yc.AdmissionControl.AcquireTimeout)
		}
		ac.AcquireTimeout = d
	}

	return nil
}

// applyRateLimits folds the file's rate_limiting section into base, keeping
// the built-in default for anything the file does not mention.
func applyRateLimits(yc yamlFileConfig, base *ServerConfig) error {
	rl := &base.RateLimits
	if rl.Protocols == nil {
		rl.Protocols = DefaultRateLimits().Protocols
	}

	if yc.RateLimiting.WindowMinutes > 0 {
		rl.Window = time.Duration(yc.RateLimiting.WindowMinutes) * time.Minute
	}
	if yc.RateLimiting.Window != "" {
		d, err := time.ParseDuration(yc.RateLimiting.Window)
		if err != nil {
			return fmt.Errorf("parse rate_limiting.window: %w", err)
		}
		if d <= 0 {
			return fmt.Errorf("rate_limiting.window must be positive, got %s", yc.RateLimiting.Window)
		}
		rl.Window = d
	}

	// The legacy key only ever fed the MTA router, so it still only does.
	if yc.RateLimiting.MaxRequestsPerWindow > 0 {
		mta := rl.For(RateLimitMTA)
		mta.Requests = Limit{
			Rate:  yc.RateLimiting.MaxRequestsPerWindow,
			Burst: 2 * yc.RateLimiting.MaxRequestsPerWindow,
		}
		rl.Protocols[RateLimitMTA] = mta
	}

	for name, override := range yc.RateLimiting.Protocols {
		if _, known := DefaultRateLimits().Protocols[name]; !known {
			// A typo here would silently leave the protocol at its default,
			// which is the failure mode that cost the sumi team days.
			return fmt.Errorf("unknown protocol %q in rate_limiting.protocols", name)
		}
		limits := rl.For(name)
		limits.Requests = mergeLimit(limits.Requests, override.Requests)
		limits.Read = mergeLimit(limits.Read, override.Read)
		limits.Write = mergeLimit(limits.Write, override.Write)
		rl.Protocols[name] = limits
	}

	return nil
}

// mergeLimit overlays the non-zero fields of override onto base. Zero means
// "not specified"; a negative rate explicitly disables the bucket.
func mergeLimit(base, override Limit) Limit {
	if override.Rate != 0 {
		base.Rate = override.Rate
	}
	if override.Burst != 0 {
		base.Burst = override.Burst
	}
	return base
}
