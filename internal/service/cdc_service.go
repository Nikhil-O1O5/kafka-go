package service

import (
	orders "github.com/Nikhil-O1O5/kafka-go/internal/gen/orders"
	"github.com/sirupsen/logrus"
)

type CDCService struct{}

func NewCDCService() *CDCService {
	return &CDCService{}
}

func (s *CDCService) HandleEnvelope(env *orders.Envelope) {
	entry := logrus.WithFields(logrus.Fields{
		"op":    env.GetOp(),
		"ts_ms": env.GetTsMs(),
	})

	switch env.GetOp() {
	case "c", "r":
		entry.WithField("order_id", env.GetAfter().GetOrderId()).Info("cdc: order created")
	case "u":
		entry.WithFields(logrus.Fields{
			"order_id":    env.GetAfter().GetOrderId(),
			"item_before": env.GetBefore().GetItem(),
			"item_after":  env.GetAfter().GetItem(),
		}).Info("cdc: order updated")
	case "d":
		entry.WithField("order_id", env.GetBefore().GetOrderId()).Info("cdc: order deleted")
	default:
		entry.Info("cdc: unknown op")
	}
}
