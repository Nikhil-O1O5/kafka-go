package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Nikhil-O1O5/kafka-go/internal/repo"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

type OrderService struct {
	orderRepo  *repo.OrderRepo
	outboxRepo *repo.OutboxRepo
}

func NewOrderService(orderRepo *repo.OrderRepo, outboxRepo *repo.OutboxRepo) *OrderService {
	return &OrderService{
		orderRepo:  orderRepo,
		outboxRepo: outboxRepo,
	}
}

func (s *OrderService) Create(ctx context.Context, item string) (string, error) {
	order := repo.NewOrder(item)

	payload, err := json.Marshal(order)
	if err != nil {
		return "", fmt.Errorf("marshal order: %w", err)
	}

	outboxEvent := repo.NewOutboxEvent(order.OrderId, string(payload))

	_, err = repo.TxClosure(ctx, s.orderRepo.DB(), func(ctx context.Context, tx *sqlx.Tx) (string, error) {
		if _, err := s.orderRepo.Insert(ctx, tx, order); err != nil {
			return "", err
		}
		if err := s.outboxRepo.Insert(ctx, tx, outboxEvent); err != nil {
			return "", err
		}
		return order.OrderId, nil
	})
	if err != nil {
		return "", err
	}

	logrus.WithFields(logrus.Fields{
		"order_id":  order.OrderId,
		"outbox_id": outboxEvent.OutboxId,
	}).Info("order created with outbox event")

	return order.OrderId, nil
}
