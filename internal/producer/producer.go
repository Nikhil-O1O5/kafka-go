package producer

import (
	"github.com/Nikhil-O1O5/kafka-go/internal/shared"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaProducer(topic string) (*KafkaProducer, error) {
	cfg := shared.NewKafkaConfig()
	if topic == "" {
		topic = cfg.Topic
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})
	if err != nil {
		return nil, err
	}

	go func() {
		for e := range p.Events() {
			if msg, ok := e.(*kafka.Message); ok {
				if msg.TopicPartition.Error != nil {
					logrus.WithError(msg.TopicPartition.Error).Error("delivery failed")
				} else {
					logrus.WithField("offset", msg.TopicPartition.Offset).Info("delivery successful")
				}
			}
		}
	}()

	return &KafkaProducer{producer: p, topic: topic}, nil
}

func (p *KafkaProducer) Produce(key, msg []byte) {
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          msg,
	}, nil)
	if err != nil {
		logrus.WithError(err).Error("produce failed")
	}
}
