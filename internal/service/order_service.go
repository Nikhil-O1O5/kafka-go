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
	outboxEvent := repo.NewOutboxEvent(order.OrderId, "")

	// use outbox_id as event_id so the consumer has a stable deduplication key
	kafkaEvent := repo.NewEventWithId(outboxEvent.OutboxId)
	payload, err := json.Marshal(kafkaEvent)
	if err != nil {
		return "", fmt.Errorf("marshal kafka event: %w", err)
	}
	outboxEvent.Payload = string(payload)

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
