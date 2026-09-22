package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Nikhil-O1O5/kafka-go/internal/consumer"
	"github.com/Nikhil-O1O5/kafka-go/internal/producer"
	"github.com/Nikhil-O1O5/kafka-go/internal/repo"
	"github.com/Nikhil-O1O5/kafka-go/internal/shared"
)

type Server struct {
	producer *producer.KafkaProducer
	consumer *consumer.KafkaConsumer
	msgCH    chan *shared.Message
}

func NewServer() *Server {
	msgCH := make(chan *shared.Message, 64)
	c, err := consumer.NewKafkaConsumer(msgCH)
	if err != nil {
		panic(err)
	}
	return &Server{
		producer: producer.NewKafkaProducer(""),
		consumer: c,
		msgCH:    msgCH,
	}
}

func (s *Server) produceMsg() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		event := repo.NewEvent()
		b, err := json.Marshal(event)
		if err != nil {
			fmt.Printf("marshal error: %v\n", err)
			continue
		}
		s.producer.Produce(b)
	}
}

func (s *Server) handleMsg(msg *shared.Message) {
	fmt.Printf("received offset=%d event_id=%s\n", msg.Metadata.Offset, msg.Event.EventId)
	s.consumer.MarkAsComplete(msg.Metadata)
}

func main() {
	s := NewServer()
	go s.produceMsg()
	for msg := range s.msgCH {
		go s.handleMsg(msg)
	}
}
