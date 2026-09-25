package repo

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
	order_id   TEXT PRIMARY KEY,
	item       TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
	outbox_id  TEXT PRIMARY KEY,
	order_id   TEXT NOT NULL,
	payload    TEXT NOT NULL,
	status     TEXT NOT NULL DEFAULT 'pending',
	created_at TIMESTAMPTZ NOT NULL
);`

func NewDBConn() (*sqlx.DB, error) {
	dsn := "host=localhost port=5432 user=kafka_user password=kafka_pass dbname=kafkadb sslmode=disable"
	db, err := sqlx.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	if _, err = db.Exec(schema); err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return db, nil
}

func TxClosure[T any](ctx context.Context, db *sqlx.DB, fn func(context.Context, *sqlx.Tx) (T, error)) (T, error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	result, err := fn(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		return result, err
	}
	if err = tx.Commit(); err != nil {
		return result, fmt.Errorf("commit tx: %w", err)
	}
	return result, nil
}
