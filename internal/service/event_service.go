package service

import (
	"context"

	"github.com/Nikhil-O1O5/kafka-go/internal/repo"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

type EventService struct {
	eventRepo *repo.EventRepo
}

func NewEventService(eventRepo *repo.EventRepo) *EventService {
	return &EventService{eventRepo: eventRepo}
}

func (s *EventService) Process(ctx context.Context, event *repo.Event) error {
	_, err := repo.TxClosure(ctx, s.eventRepo.DB(), func(ctx context.Context, tx *sqlx.Tx) (string, error) {
		if existing := s.eventRepo.Get(ctx, tx, event.EventId); existing != nil {
			logrus.WithField("event_id", event.EventId).Info("duplicate event — skipping")
			return "", nil
		}
		id, err := s.eventRepo.Insert(ctx, tx, event)
		if err != nil {
			return "", err
		}
		logrus.WithField("event_id", id).Info("event inserted")
		return id, nil
	})
	return err
}
