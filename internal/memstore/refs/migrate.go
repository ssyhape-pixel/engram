package refs

import (
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the pgx5:// migrate driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed all:migrations
var migrationsFS embed.FS

// Migrate applies all up migrations to the database at dsn. Idempotent.
func Migrate(dsn string) (err error) {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("refs: migration source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, "pgx5://"+stripScheme(dsn))
	if err != nil {
		return fmt.Errorf("refs: migrate init: %w", err)
	}
	defer func() {
		// Close releases the source and DB handles on every return path.
		// Ignore the source error; only surface a DB-handle close failure,
		// and only if we'd otherwise be returning success.
		if _, dbErr := m.Close(); dbErr != nil && err == nil {
			err = fmt.Errorf("refs: migrate close: %w", dbErr)
		}
	}()
	if upErr := m.Up(); upErr != nil && !errors.Is(upErr, migrate.ErrNoChange) {
		return fmt.Errorf("refs: migrate up: %w", upErr)
	}
	return nil
}

// stripScheme turns "postgres://..." / "postgresql://..." / "pgx://..." into the
// host/db part so it can be reattached as pgx5://. The scheme is matched
// case-insensitively; the remainder (host, db, query) is returned unchanged to
// preserve case-sensitive credentials and parameters.
func stripScheme(dsn string) string {
	for _, p := range []string{"postgres://", "postgresql://", "pgx5://", "pgx://"} {
		if len(dsn) >= len(p) {
			if _, ok := strings.CutPrefix(strings.ToLower(dsn[:len(p)]), p); ok {
				return dsn[len(p):]
			}
		}
	}
	return dsn
}
