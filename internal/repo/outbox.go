package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type OutboxStatus string

const (
	OutboxStatus_Pending  OutboxStatus = "pending"
	OutboxStatus_Produced OutboxStatus = "produced"
)

type OutboxEvent struct {
	OutboxId  string       `db:"outbox_id"  json:"outbox_id"`
	OrderId   string       `db:"order_id"   json:"order_id"`
	Payload   string       `db:"payload"    json:"payload"`
	Status    OutboxStatus `db:"status"     json:"status"`
	CreatedAt time.Time    `db:"created_at" json:"created_at"`
}

func NewOutboxEvent(orderId, payload string) *OutboxEvent {
	return &OutboxEvent{
		OutboxId:  uuid.NewString(),
		OrderId:   orderId,
		Payload:   payload,
		Status:    OutboxStatus_Pending,
		CreatedAt: time.Now(),
	}
}

type OutboxRepo struct {
	db *sqlx.DB
}

func NewOutboxRepo(db *sqlx.DB) *OutboxRepo {
	return &OutboxRepo{db: db}
}

func (r *OutboxRepo) DB() *sqlx.DB { return r.db }

func (r *OutboxRepo) Insert(ctx context.Context, tx *sqlx.Tx, event *OutboxEvent) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (outbox_id, order_id, payload, status, created_at) VALUES (?, ?, ?, ?, ?)`,
		event.OutboxId, event.OrderId, event.Payload, event.Status, event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func (r *OutboxRepo) GetAllPending(ctx context.Context, tx *sqlx.Tx) ([]*OutboxEvent, error) {
	rows, err := tx.QueryxContext(ctx, `SELECT outbox_id, order_id, payload, status, created_at FROM outbox WHERE status = ?`, OutboxStatus_Pending)
	if err != nil {
		return nil, fmt.Errorf("get pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []*OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.StructScan(&e); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, &e)
	}
	return events, nil
}

func (r *OutboxRepo) UpdateStatusByIds(ctx context.Context, tx *sqlx.Tx, ids []string, status OutboxStatus) error {
	if len(ids) == 0 {
		return nil
	}
	query, args, err := sqlx.In(`UPDATE outbox SET status = ? WHERE outbox_id IN (?)`, status, ids)
	if err != nil {
		return fmt.Errorf("build update query: %w", err)
	}
	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update outbox status: %w", err)
	}
	return nil
}
