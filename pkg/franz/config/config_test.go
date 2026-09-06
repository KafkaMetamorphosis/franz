package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	c, err := Load("does-not-exist.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.HTTPPort != 8080 || c.GRPCPort != 9090 {
		t.Fatalf("default ports: got http=%d grpc=%d", c.HTTPPort, c.GRPCPort)
	}
	if c.BootstrapRealm != "default" {
		t.Fatalf("default realm: got %q", c.BootstrapRealm)
	}
	if c.DB.Name != "franz" {
		t.Fatalf("default db name: got %q", c.DB.Name)
	}
}

func TestLoadEnvOverride(t *testing.T) {
	t.Setenv("FRANZ_HTTP_PORT", "18080")
	t.Setenv("FRANZ_DB__PASSWORD", "s3cr3t")
	t.Setenv("FRANZ_BOOTSTRAP_REALM", "acme")

	c, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.HTTPPort != 18080 {
		t.Fatalf("FRANZ_HTTP_PORT override: got %d", c.HTTPPort)
	}
	if c.DB.Password != "s3cr3t" {
		t.Fatalf("FRANZ_DB__PASSWORD override: got %q", c.DB.Password)
	}
	if c.BootstrapRealm != "acme" {
		t.Fatalf("FRANZ_BOOTSTRAP_REALM override: got %q", c.BootstrapRealm)
	}
}

func TestDSN(t *testing.T) {
	d := DBConfig{Host: "h", Port: 5432, Name: "n", User: "u", Password: "p", SSLMode: "disable"}
	want := "postgres://u:p@h:5432/n?sslmode=disable"
	if got := d.DSN(); got != want {
		t.Fatalf("DSN: got %q want %q", got, want)
	}
}
