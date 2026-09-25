package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nikhil-O1O5/kafka-go/internal/consumer"
	"github.com/Nikhil-O1O5/kafka-go/internal/debezium"
	nethttp "github.com/Nikhil-O1O5/kafka-go/internal/http"
	"github.com/Nikhil-O1O5/kafka-go/internal/producer"
	"github.com/Nikhil-O1O5/kafka-go/internal/repo"
	"github.com/Nikhil-O1O5/kafka-go/internal/service"
	"github.com/Nikhil-O1O5/kafka-go/internal/shared"
	"github.com/sirupsen/logrus"
)

type Server struct {
	consumer     *consumer.KafkaConsumer
	cdcConsumer  *consumer.CDCConsumer
	msgCH        chan *shared.Message
	eventService *service.EventService
	outboxSvc    *service.OutboxService
	httpServer   *http.Server
}

func NewServer(
	eventService *service.EventService,
	outboxSvc *service.OutboxService,
	cdcService *service.CDCService,
	orderHandler *nethttp.OrderHandler,
) (*Server, error) {
	msgCH := make(chan *shared.Message, 64)
	c, err := consumer.NewKafkaConsumer(msgCH)
	if err != nil {
		return nil, err
	}

	cdcConsumer, err := consumer.NewCDCConsumer(cdcService.HandleEnvelope)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	orderHandler.RegisterRoutes(mux)

	return &Server{
		consumer:     c,
		cdcConsumer:  cdcConsumer,
		msgCH:        msgCH,
		eventService: eventService,
		outboxSvc:    outboxSvc,
		httpServer:   &http.Server{Addr: ":8080", Handler: mux},
	}, nil
}

func (s *Server) handleMsg(msg *shared.Message) {
	ctx := context.Background()
	defer s.consumer.MarkAsComplete(msg.Metadata)
	if err := s.eventService.Process(ctx, msg.Event); err != nil {
		logrus.WithError(err).WithField("offset", msg.Metadata.Offset).Error("process failed")
	}
}

func (s *Server) start() {
	go func() {
		logrus.WithField("addr", s.httpServer.Addr).Info("http server started")
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.WithError(err).Fatal("http server error")
		}
	}()

	go s.outboxSvc.Start()

	go func() {
		for msg := range s.msgCH {
			go s.handleMsg(msg)
		}
	}()
}

func (s *Server) stop() {
	s.outboxSvc.Stop()
	s.consumer.Close()
	s.cdcConsumer.Close()
	if err := s.httpServer.Shutdown(context.Background()); err != nil {
		logrus.WithError(err).Error("http server shutdown error")
	}
}

func main() {
	db, err := repo.NewDBConn()
	if err != nil {
		logrus.WithError(err).Fatal("db init failed")
	}

	if err := debezium.RegisterConnector(); err != nil {
		logrus.WithError(err).Fatal("debezium connector registration failed")
	}

	p, err := producer.NewKafkaProducer("")
	if err != nil {
		logrus.WithError(err).Fatal("producer init failed")
	}

	eventRepo := repo.NewEventRepo(db)
	orderRepo := repo.NewOrderRepo(db)
	outboxRepo := repo.NewOutboxRepo(db)

	eventService := service.NewEventService(eventRepo)
	orderService := service.NewOrderService(orderRepo, outboxRepo)
	outboxSvc := service.NewOutboxService(outboxRepo, p)
	cdcService := service.NewCDCService()
	orderHandler := nethttp.NewOrderHandler(orderService)

	s, err := NewServer(eventService, outboxSvc, cdcService, orderHandler)
	if err != nil {
		logrus.WithError(err).Fatal("server init failed")
	}

	s.start()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logrus.Info("shutting down")
	s.stop()
}
