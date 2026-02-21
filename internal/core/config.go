package core

import (
	"fmt"
	"time"
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
	EnableAutoRelay          bool `yaml:"enable_auto_relay" json:"enableAutoRelay"`
	EnableHolePunching       bool `yaml:"enable_hole_punching" json:"enableHolePunching"`

	// Presence broadcasting
	PresenceHeartbeatInterval time.Duration `yaml:"presence_heartbeat_interval" json:"presenceHeartbeatInterval"`
	PresenceTimeoutDuration   time.Duration `yaml:"presence_timeout_duration" json:"presenceTimeoutDuration"`
	PresenceBatchWindow       time.Duration `yaml:"presence_batch_window" json:"presenceBatchWindow"`

	// Security
	EnableAuthentication bool          `yaml:"enable_authentication" json:"enableAuthentication"`
	TrustedPeers         []string      `yaml:"trusted_peers" json:"trustedPeers"`
	RateLimitWindow      time.Duration `yaml:"rate_limit_window" json:"rateLimitWindow"`
	MaxRequestsPerWindow int           `yaml:"max_requests_per_window" json:"maxRequestsPerWindow"`

	// Identity
	IdentityFile string `yaml:"identity_file" json:"identityFile,omitempty"`
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
				PoolSize:       10,
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

		PresenceHeartbeatInterval: 60 * time.Second,
		PresenceTimeoutDuration:   120 * time.Second,
		PresenceBatchWindow:       2 * time.Second,

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
