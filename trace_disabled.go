// +build !sqlite_trace,trace

package sqlite

import (
	"log"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// WithTracing enables SQLite tracing to the logger
// Tracing must be enabled by using the build tag "trace" or "sqlite_trace"
func WithTracing(logger *log.Logger) Optional {
	log.Println(`tracing must be enabled by using the build tag "trace" or "sqlite_trace"`)
	return func(_ *Config) {
	}
}

// TraceHook enables SQLite tracing to the logger
// Tracing must be enabled by using the build tag "trace" or "sqlite_trace"
func TraceHook(logger *log.Logger) Hook {
	log.Println(`tracing must be enabled by using the build tag "trace" or "sqlite_trace"`)
	hook := func(conn *sqlite3.SQLiteConn) error {
		return nil
	}
	return hook
}
