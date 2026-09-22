package repo

import (
	"time"

	"github.com/google/uuid"
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
