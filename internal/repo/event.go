package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Event struct {
	EventId   string    `db:"event_id" json:"event_id"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

func NewEvent() *Event {
	return &Event{
		EventId:   uuid.NewString(),
		CreatedAt: time.Now(),
	}
}

type EventRepo struct {
	db *sqlx.DB
}

func NewEventRepo(db *sqlx.DB) *EventRepo {
	return &EventRepo{db: db}
}

func (r *EventRepo) DB() *sqlx.DB { return r.db }

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
