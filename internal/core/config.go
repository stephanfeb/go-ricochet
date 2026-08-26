package core

import (
	"fmt"
	"os"
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
	Storage              StorageBackendConfig `yaml:"storage" json:"storage"`
	DataDirectory        string               `yaml:"data_directory" json:"dataDirectory"`
	MaxStorageBytes      int64                `yaml:"max_storage_bytes" json:"maxStorageBytes"`
	RetentionPolicy      time.Duration        `yaml:"retention_policy" json:"retentionPolicy"`
	MaxMessagesPerMailbox int                  `yaml:"max_messages_per_mailbox" json:"maxMessagesPerMailbox"`
	MaxMailboxes         int                  `yaml:"max_mailboxes" json:"maxMailboxes"`

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
	EnableAuthentication bool          `yaml:"enable_authentication" json:"enableAuthentication"`
	TrustedPeers         []string      `yaml:"trusted_peers" json:"trustedPeers"`
	RateLimitWindow      time.Duration `yaml:"rate_limit_window" json:"rateLimitWindow"`
	MaxRequestsPerWindow int           `yaml:"max_requests_per_window" json:"maxRequestsPerWindow"`

	// Relay limits
	RelayLimits RelayLimits `yaml:"relay_limits" json:"relayLimits"`

	// Identity
	IdentityFile string `yaml:"identity_file" json:"identityFile,omitempty"`
}

// RelayLimits configures circuit relay v2 service resource limits.
// Zero values mean "use go-libp2p defaults".
type RelayLimits struct {
	MaxReservations       int           `yaml:"max_reservations" json:"maxReservations"`
	MaxCircuits           int           `yaml:"max_circuits" json:"maxCircuits"`
	BufferSize            int           `yaml:"buffer_size" json:"bufferSize"`
	MaxReservationsPerPeer int          `yaml:"max_reservations_per_peer" json:"maxReservationsPerPeer"`
	MaxReservationsPerIP  int           `yaml:"max_reservations_per_ip" json:"maxReservationsPerIP"`
	MaxReservationsPerASN int           `yaml:"max_reservations_per_asn" json:"maxReservationsPerASN"`
	ReservationTTL        time.Duration `yaml:"reservation_ttl" json:"reservationTTL"`
	ConnectionDuration    time.Duration `yaml:"connection_duration" json:"connectionDuration"`
	ConnectionData        int64         `yaml:"connection_data" json:"connectionData"`
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
		Port:            55223,
		ListenAddresses: []string{"/ip4/0.0.0.0/udp/55223/udx"},
		DataDirectory:   "./sf_storage",
		MaxStorageBytes: 10 * 1024 * 1024 * 1024, // 10GB
		RetentionPolicy: 30 * 24 * time.Hour,      // 30 days
		MaxMessagesPerMailbox: 1000,
		MaxMailboxes:         100000,

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

		RateLimitWindow:      1 * time.Minute,
		MaxRequestsPerWindow: 100,
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
	if c.MaxRequestsPerWindow <= 0 {
		return fmt.Errorf("max_requests_per_window must be positive")
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
		MaxReservations       int    `yaml:"max_reservations"`
		MaxCircuits           int    `yaml:"max_circuits"`
		BufferSize            int    `yaml:"buffer_size"`
		MaxReservationsPerPeer int   `yaml:"max_reservations_per_peer"`
		MaxReservationsPerIP  int    `yaml:"max_reservations_per_ip"`
		MaxReservationsPerASN int    `yaml:"max_reservations_per_asn"`
		ReservationTTLMin     int    `yaml:"reservation_ttl_min"`
		ConnectionDurationSec int    `yaml:"connection_duration_sec"`
		ConnectionData        int64  `yaml:"connection_data"`
	} `yaml:"relay_limits"`

	RateLimiting struct {
		WindowMinutes        int `yaml:"window_minutes"`
		MaxRequestsPerWindow int `yaml:"max_requests_per_window"`
	} `yaml:"rate_limiting"`
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
	if yc.RateLimiting.WindowMinutes > 0 {
		base.RateLimitWindow = time.Duration(yc.RateLimiting.WindowMinutes) * time.Minute
	}
	if yc.RateLimiting.MaxRequestsPerWindow > 0 {
		base.MaxRequestsPerWindow = yc.RateLimiting.MaxRequestsPerWindow
	}

	return nil
}
