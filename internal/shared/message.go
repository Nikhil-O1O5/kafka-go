package shared

import (
	"encoding/json"
	"fmt"

	"github.com/Nikhil-O1O5/kafka-go/internal/repo"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Message struct {
	Metadata *kafka.TopicPartition
	Event    *repo.Event
}

func NewMessage(tp *kafka.TopicPartition, data []byte) *Message {
	var event repo.Event
	if err := json.Unmarshal(data, &event); err != nil {
		panic(fmt.Sprintf("failed to unmarshal event: %v", err))
	}
	return &Message{
		Metadata: tp,
		Event:    &event,
	}
}
