package testcontainers

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type TestPostgres struct {
	Client    *pgxpool.Pool
	URL       string
	Container *dockertest.Resource
}

func StartPostgres(tb testing.TB) *TestPostgres {
	tb.Helper()

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		tb.Fatalf("Could not construct pool: %v", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		tb.Fatalf("Could not connect to Docker: %v", err)
	}

	// pulls an image, creates a container based on it and runs it
	res, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "pgvector/pgvector",
		Tag:        "pg15",
		Env: []string{
			"POSTGRES_USER=user_name",
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_DB=dbname",
			"listen_addresses='*'",
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		tb.Fatalf("Could not start resource: %v", err)
	}

	addCleanup(tb, func() {
		_ = pool.Purge(res)
	})

	hostPort := res.GetHostPort("5432/tcp")
	url := fmt.Sprintf("postgres://user_name:secret@%s/dbname?sslmode=disable", hostPort)

	db := WaitForDB(tb, url, 30*time.Second)
	testPostgres := TestPostgres{
		Client:    db,
		URL:       url,
		Container: res,
	}

	return &testPostgres
}

// WaitForDB waits for the database to be ready (or hit timeout).
func WaitForDB(tb testing.TB, url string, timeout time.Duration) *pgxpool.Pool {
	tb.Helper()

	conf, err := pgxpool.ParseConfig(url)
	if err != nil {
		tb.Fatalf("failed to parse url to config: %v", err)
	}

	timer := time.NewTimer(timeout)

	ctx := context.Background()
	dbpool, err := pgxpool.NewWithConfig(ctx, conf)
	if err == nil {
		err = dbpool.Ping(ctx)
	}
	for err != nil {
		select {
		case <-timer.C:
			tb.Fatal("timed out")
		default:
			time.Sleep(time.Millisecond * 100)
			dbpool, err = pgxpool.NewWithConfig(ctx, conf)
			if err == nil {
				err = dbpool.Ping(ctx)
			}
		}
	}
	return dbpool
}

// Init applies the migrations to the database (down and up).
func (db *TestPostgres) Init(tb testing.TB, migrationsPath string) {
	tb.Helper()
	client, err := sql.Open("pgx", db.URL)
	if err != nil {
		tb.Fatalf("error open connection to apply migration: %s", err)
	}

	driver, err := postgres.WithInstance(client, &postgres.Config{})
	if err != nil {
		tb.Fatalf("could not init driver: %s", err)
	}

	mig, err := migrate.NewWithDatabaseInstance(
		"file://"+migrationsPath,
		"pgx", driver)
	if err != nil {
		tb.Fatalf("could not apply the migration: %s", err)
	}

	if err := mig.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		tb.Fatalf("could not apply the migration Down: %s", err)
	}
	if err := mig.Up(); err != nil {
		tb.Fatalf("could not apply the migration Up: %s", err)
	}
}

func (db *TestPostgres) InitFromEmbedFS(tb testing.TB, embedFS embed.FS) {
	tb.Helper()
	client, err := sql.Open("pgx", db.URL)
	if err != nil {
		tb.Fatalf("error open connection to apply migration: %s", err)
	}

	driver, err := postgres.WithInstance(client, &postgres.Config{})
	if err != nil {
		tb.Fatalf("could not init driver: %s", err)
	}

	d, err := iofs.New(embedFS, ".")
	if err != nil {
		tb.Fatalf("could not create iofs driver: %s", err)
	}

	mig, err := migrate.NewWithInstance("iofs", d, "pgx", driver)
	if err != nil {
		tb.Fatalf("could not create migrate instance: %s", err)
	}

	if err := mig.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		tb.Fatalf("could not apply the migration Down: %s", err)
	}
	if err := mig.Up(); err != nil {
		tb.Fatalf("could not apply the migration Up: %s", err)
	}
}
