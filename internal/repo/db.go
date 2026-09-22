package repo

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const schema = `
CREATE TABLE IF NOT EXISTS events (
	event_id   TEXT PRIMARY KEY,
	created_at DATETIME NOT NULL
);`

func NewDBConn() (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite3", "./events.db")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if _, err = db.Exec(schema); err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return db, nil
}

type EventRepo struct {
	db *sqlx.DB
}

func NewEventRepo(db *sqlx.DB) *EventRepo {
	return &EventRepo{db: db}
}

func (r *EventRepo) Get(ctx context.Context, tx *sqlx.Tx, eventId string) *Event {
	var event Event
	err := tx.GetContext(ctx, &event, `SELECT event_id, created_at FROM events WHERE event_id = ?`, eventId)
	if err != nil {
		return nil
	}
	return &event
}

func (r *EventRepo) Insert(ctx context.Context, tx *sqlx.Tx, event *Event) (string, error) {
	_, err := tx.ExecContext(ctx, `INSERT INTO events (event_id, created_at) VALUES (?, ?)`, event.EventId, event.CreatedAt)
	if err != nil {
		return "", fmt.Errorf("insert event: %w", err)
	}
	return event.EventId, nil
}

func TxClosure[T any](ctx context.Context, r *EventRepo, fn func(context.Context, *sqlx.Tx) (T, error)) (T, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
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
