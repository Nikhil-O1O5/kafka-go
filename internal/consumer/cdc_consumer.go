package consumer

import (
	"fmt"
	"time"

	orders "github.com/Nikhil-O1O5/kafka-go/internal/gen/orders"
	"github.com/Nikhil-O1O5/kafka-go/internal/shared"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/sirupsen/logrus"
)

const cdcTopic = "cdc.public.orders"

type CDCConsumer struct {
	consumer *kafka.Consumer
	deser    *protobuf.Deserializer
	handler  func(*orders.Envelope)
	exitCH   chan struct{}
}

func NewCDCConsumer(handler func(*orders.Envelope)) (*CDCConsumer, error) {
	cfg := shared.NewKafkaConfig()

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(cfg.SchemaRegistryURL))
	if err != nil {
		return nil, fmt.Errorf("schema registry client: %w", err)
	}

	deser, err := protobuf.NewDeserializer(srClient, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, fmt.Errorf("protobuf deserializer: %w", err)
	}
	deser.ProtoRegistry.RegisterMessage(new(orders.Envelope).ProtoReflect().Type())

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Host,
		"group.id":           "cdc-consumer-group",
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("new cdc consumer: %w", err)
	}

	if err = c.Subscribe(cdcTopic, nil); err != nil {
		c.Close()
		return nil, fmt.Errorf("subscribe cdc topic: %w", err)
	}

	cc := &CDCConsumer{
		consumer: c,
		deser:    deser,
		handler:  handler,
		exitCH:   make(chan struct{}),
	}

	go cc.readLoop()
	return cc, nil
}

func (c *CDCConsumer) Close() {
	close(c.exitCH)
}

func (c *CDCConsumer) readLoop() {
	defer c.consumer.Close()
	for {
		select {
		case <-c.exitCH:
			return
		default:
		}

		msg, err := c.consumer.ReadMessage(time.Second)
		if err != nil && err.(kafka.Error).IsTimeout() {
			continue
		}
		if err != nil {
			logrus.WithError(err).Error("cdc consumer read error")
			continue
		}

		var env orders.Envelope
		if err = c.deser.DeserializeInto(cdcTopic, msg.Value, &env); err != nil {
			logrus.WithError(err).
				WithField("offset", msg.TopicPartition.Offset).
				Error("cdc deserialize failed")
			c.consumer.CommitMessage(msg)
			continue
		}

		c.handler(&env)
		c.consumer.CommitMessage(msg)
		logrus.WithFields(logrus.Fields{
			"topic":     cdcTopic,
			"partition": msg.TopicPartition.Partition,
			"offset":    msg.TopicPartition.Offset,
		}).Info("cdc: offset committed")
	}
}
