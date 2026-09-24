package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Order struct {
	OrderId   string    `db:"order_id" json:"order_id"`
	Item      string    `db:"item" json:"item"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
}

func NewOrder(item string) *Order {
	return &Order{
		OrderId:   uuid.NewString(),
		Item:      item,
		CreatedAt: time.Now(),
	}
}

type OrderRepo struct {
	db *sqlx.DB
}

func NewOrderRepo(db *sqlx.DB) *OrderRepo {
	return &OrderRepo{db: db}
}

func (r *OrderRepo) DB() *sqlx.DB { return r.db }

func (r *OrderRepo) Insert(ctx context.Context, tx *sqlx.Tx, order *Order) (string, error) {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO orders (order_id, item, created_at) VALUES (?, ?, ?)`,
		order.OrderId, order.Item, order.CreatedAt,
	)
	if err != nil {
		return "", fmt.Errorf("insert order: %w", err)
	}
	return order.OrderId, nil
}
