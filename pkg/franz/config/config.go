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

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config is the fully-resolved Franz configuration.
type Config struct {
	HTTPPort       int      `koanf:"http_port"`
	GRPCPort       int      `koanf:"grpc_port"`
	LogLevel       string   `koanf:"log_level"`
	BootstrapRealm string   `koanf:"bootstrap_realm"`
	DB             DBConfig `koanf:"db"`
}

// DBConfig is the PostgreSQL connection configuration.
type DBConfig struct {
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	Name     string `koanf:"name"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	SSLMode  string `koanf:"sslmode"`
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
	"db.host":         "localhost",
	"db.port":         5432,
	"db.name":         "franz",
	"db.user":         "franz",
	"db.password":     "franz",
	"db.sslmode":      "disable",
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
