// Package migrations embeds the Flyway SQL migrations so Franz can also apply
// them on boot (see adapters/out/postgres.DB.Migrate). Flyway ignores this file;
// it only reads V*.sql / R*.sql.
package migrations

import "embed"

// FS holds every migration file, ordered lexically by name (V1, V2, …).
//
//go:embed *.sql
var FS embed.FS
