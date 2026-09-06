// Package shared holds domain-agnostic helpers reused across Franz and the
// in-module agents (label selectors, ORN, logging, …).
package shared

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger returns a JSON slog.Logger at the given level ("debug", "info",
// "warn", "error"; anything else -> info).
func NewLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
