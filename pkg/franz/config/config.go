// Package config loads Franz configuration from a checked-in config.yaml,
// overlaid by FRANZ_-prefixed environment variables (ADR-API-005 D4).
//
//	FRANZ_HTTP_PORT=9090        -> http_port
//	FRANZ_DB__PASSWORD=secret   -> db.password   (double underscore = nesting)
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config is the fully-resolved Franz configuration.
type Config struct {
	HTTPPort       int    `koanf:"http_port"`
	GRPCPort       int    `koanf:"grpc_port"`
	LogLevel       string `koanf:"log_level"`
	BootstrapRealm string `koanf:"bootstrap_realm"`
	// ResourcePrefix is the FRN prefix (003.1). Default "frn"; fixed at bootstrap.
	ResourcePrefix string          `koanf:"resource_prefix"`
	DB             DBConfig        `koanf:"db"`
	Placement      PlacementConfig `koanf:"placement"`
}

// PlacementConfig tunes the channel → cluster placement retry sweep (003.7).
type PlacementConfig struct {
	// SweepInterval is how often Franz re-runs selection for every ACTIVE channel
	// with fewer async-channel shard rows than its declared `channel_partitions`.
	// 003.7 asks for ~30s (OQ5). Set FRANZ_PLACEMENT__SWEEP_INTERVAL to a Go
	// duration ("1m", "10s") to change it; 0 or negative disables the sweep, in
	// which case only the create / update triggers place shards.
	SweepInterval time.Duration `koanf:"sweep_interval"`
}

// DBConfig is the PostgreSQL connection configuration.
type DBConfig struct {
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	Name     string `koanf:"name"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	SSLMode  string `koanf:"sslmode"`
	// AutoMigrate runs the embedded (Flyway-compatible) migrations on boot. On by
	// default for local/dev; set false where Flyway owns schema changes (003.12).
	AutoMigrate bool `koanf:"auto_migrate"`
}

// DSN renders a lib/pq-style connection string.
func (d DBConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		d.User, d.Password, d.Host, d.Port, d.Name, d.SSLMode,
	)
}

var defaults = map[string]any{
	"http_port":       8080,
	"grpc_port":       9090,
	"log_level":       "info",
	"bootstrap_realm": "default",
	"resource_prefix": "frn",
	"db.host":         "localhost",
	"db.port":         5432,
	"db.name":         "franz",
	"db.user":         "franz",
	"db.password":     "franz",
	"db.sslmode":      "disable",
	"db.auto_migrate": true,

	"placement.sweep_interval": "30s",
}

// Load builds a Config from: built-in defaults, then the YAML file at path (if it
// exists), then FRANZ_-prefixed env vars (highest precedence).
func Load(path string) (Config, error) {
	k := koanf.New(".")

	if err := k.Load(confmap.Provider(defaults, "."), nil); err != nil {
		return Config{}, fmt.Errorf("config defaults: %w", err)
	}

	if path != "" {
		if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return Config{}, fmt.Errorf("config file %q: %w", path, err)
			}
		}
	}

	envCB := func(s string) string {
		s = strings.TrimPrefix(s, "FRANZ_")
		s = strings.ToLower(s)
		return strings.ReplaceAll(s, "__", ".")
	}
	if err := k.Load(env.Provider("FRANZ_", ".", envCB), nil); err != nil {
		return Config{}, fmt.Errorf("config env: %w", err)
	}

	var c Config
	if err := k.Unmarshal("", &c); err != nil {
		return Config{}, fmt.Errorf("config unmarshal: %w", err)
	}
	return c, nil
}
