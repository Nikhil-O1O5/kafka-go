package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Nikhil-O1O5/kafka-go/internal/consumer"
	"github.com/Nikhil-O1O5/kafka-go/internal/producer"
	"github.com/Nikhil-O1O5/kafka-go/internal/repo"
	"github.com/Nikhil-O1O5/kafka-go/internal/service"
	"github.com/Nikhil-O1O5/kafka-go/internal/shared"
)

type Server struct {
	producer     *producer.KafkaProducer
	consumer     *consumer.KafkaConsumer
	msgCH        chan *shared.Message
	eventService *service.EventService
}

func NewServer(eventService *service.EventService) *Server {
	msgCH := make(chan *shared.Message, 64)
	c, err := consumer.NewKafkaConsumer(msgCH)
	if err != nil {
		panic(err)
	}
	return &Server{
		producer:     producer.NewKafkaProducer(""),
		consumer:     c,
		msgCH:        msgCH,
		eventService: eventService,
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
	ctx := context.Background()
	defer s.consumer.MarkAsComplete(msg.Metadata)
	if err := s.eventService.Process(ctx, msg.Event); err != nil {
		fmt.Printf("process error offset=%d: %v\n", msg.Metadata.Offset, err)
	}
}

func main() {
	db, err := repo.NewDBConn()
	if err != nil {
		panic(fmt.Sprintf("db init failed: %v", err))
	}
	eventService := service.NewEventService(repo.NewEventRepo(db))
	s := NewServer(eventService)
	go s.produceMsg()
	for msg := range s.msgCH {
		go s.handleMsg(msg)
	}
}
