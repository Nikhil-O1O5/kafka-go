package service

import (
	"context"
	"time"

	"github.com/Nikhil-O1O5/kafka-go/internal/producer"
	"github.com/Nikhil-O1O5/kafka-go/internal/repo"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

type OutboxService struct {
	outboxRepo *repo.OutboxRepo
	producer   *producer.KafkaProducer
	pollDur    time.Duration
	exitCH     chan struct{}
}

func NewOutboxService(outboxRepo *repo.OutboxRepo, producer *producer.KafkaProducer) *OutboxService {
	return &OutboxService{
		outboxRepo: outboxRepo,
		producer:   producer,
		pollDur:    10 * time.Second,
		exitCH:     make(chan struct{}),
	}
}

func (s *OutboxService) Start() {
	ticker := time.NewTicker(s.pollDur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.publishPending()
		case <-s.exitCH:
			return
		}
	}
}

func (s *OutboxService) Stop() {
	close(s.exitCH)
}

func (s *OutboxService) publishPending() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := repo.TxClosure(ctx, s.outboxRepo.DB(), func(ctx context.Context, tx *sqlx.Tx) (int, error) {
		events, err := s.outboxRepo.GetAllPending(ctx, tx)
		if err != nil {
			return 0, err
		}
		if len(events) == 0 {
			return 0, nil
		}

		published := []string{}
		for _, e := range events {
			s.producer.Produce([]byte(e.OrderId), []byte(e.Payload))
			published = append(published, e.OutboxId)
			logrus.WithFields(logrus.Fields{
				"outbox_id": e.OutboxId,
				"order_id":  e.OrderId,
			}).Info("outbox event produced")
		}

		if err := s.outboxRepo.UpdateStatusByIds(ctx, tx, published, repo.OutboxStatus_Produced); err != nil {
			return 0, err
		}

		return len(published), nil
	})

	if err != nil {
		logrus.WithError(err).Error("outbox publish cycle failed")
	}
}
