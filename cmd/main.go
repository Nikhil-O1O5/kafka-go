package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Nikhil-O1O5/kafka-go/internal/consumer"
	"github.com/Nikhil-O1O5/kafka-go/internal/producer"
	"github.com/Nikhil-O1O5/kafka-go/internal/repo"
	"github.com/Nikhil-O1O5/kafka-go/internal/service"
	"github.com/Nikhil-O1O5/kafka-go/internal/shared"
	"github.com/sirupsen/logrus"
)

type Server struct {
	producer     *producer.KafkaProducer
	consumer     *consumer.KafkaConsumer
	msgCH        chan *shared.Message
	eventService *service.EventService
	stopCH       chan struct{}
}

func NewServer(eventService *service.EventService) (*Server, error) {
	msgCH := make(chan *shared.Message, 64)
	c, err := consumer.NewKafkaConsumer(msgCH)
	if err != nil {
		return nil, err
	}
	p, err := producer.NewKafkaProducer("")
	if err != nil {
		return nil, err
	}
	return &Server{
		producer:     p,
		consumer:     c,
		msgCH:        msgCH,
		eventService: eventService,
		stopCH:       make(chan struct{}),
	}, nil
}

func (s *Server) produceMsg() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			event := repo.NewEvent()
			b, err := json.Marshal(event)
			if err != nil {
				logrus.WithError(err).Error("marshal failed")
				continue
			}
			s.producer.Produce(b)
		case <-s.stopCH:
			return
		}
	}
}

func (s *Server) handleMsg(msg *shared.Message) {
	ctx := context.Background()
	defer s.consumer.MarkAsComplete(msg.Metadata)
	if err := s.eventService.Process(ctx, msg.Event); err != nil {
		logrus.WithError(err).WithField("offset", msg.Metadata.Offset).Error("process failed")
	}
}

func (s *Server) stop() {
	close(s.stopCH)
	s.consumer.Close()
}

func main() {
	db, err := repo.NewDBConn()
	if err != nil {
		logrus.WithError(err).Fatal("db init failed")
	}

	s, err := NewServer(service.NewEventService(repo.NewEventRepo(db)))
	if err != nil {
		logrus.WithError(err).Fatal("server init failed")
	}

	go s.produceMsg()
	go func() {
		for msg := range s.msgCH {
			go s.handleMsg(msg)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logrus.Info("shutting down")
	s.stop()
}
