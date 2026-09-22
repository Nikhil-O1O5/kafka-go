package service

import (
	"context"
	"fmt"

	"github.com/Nikhil-O1O5/kafka-go/internal/repo"
	"github.com/jmoiron/sqlx"
)

type EventService struct {
	eventRepo *repo.EventRepo
}

func NewEventService(eventRepo *repo.EventRepo) *EventService {
	return &EventService{eventRepo: eventRepo}
}

func (s *EventService) Process(ctx context.Context, event *repo.Event) error {
	_, err := repo.TxClosure(ctx, s.eventRepo, func(ctx context.Context, tx *sqlx.Tx) (string, error) {
		if existing := s.eventRepo.Get(ctx, tx, event.EventId); existing != nil {
			fmt.Printf("duplicate event_id=%s — skipping\n", event.EventId)
			return "", nil
		}
		id, err := s.eventRepo.Insert(ctx, tx, event)
		if err != nil {
			return "", err
		}
		fmt.Printf("inserted event_id=%s\n", id)
		return id, nil
	})
	return err
}
