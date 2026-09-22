package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Nikhil-O1O5/kafka-go/internal/shared"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

type KafkaConsumer struct {
	consumer *kafka.Consumer
	topic    string
	msgCH    chan<- *shared.Message
	readyCH  chan struct{}
	exitCH   chan struct{}
	isReady  bool

	msgsStateMap map[kafka.Offset]bool
	mu           *sync.RWMutex
	lastCommited kafka.Offset
	maxReceived  *kafka.TopicPartition
	commitDur    time.Duration
}

func NewKafkaConsumer(msgCH chan<- *shared.Message) (*KafkaConsumer, error) {
	cfg := shared.NewKafkaConfig()
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Host,
		"group.id":           cfg.ConsumerGroup,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	tp := kafka.TopicPartition{Topic: &cfg.Topic, Partition: 0}

	committed, err := c.Committed([]kafka.TopicPartition{tp}, 5000)
	if err != nil {
		return nil, err
	}

	startOffset := kafka.OffsetBeginning
	if len(committed) > 0 && committed[0].Offset != kafka.OffsetInvalid {
		startOffset = committed[0].Offset
	}
	logrus.WithField("start_offset", startOffset).Info("consumer starting position")

	consumer := &KafkaConsumer{
		consumer:     c,
		topic:        cfg.Topic,
		msgCH:        msgCH,
		readyCH:      make(chan struct{}),
		exitCH:       make(chan struct{}),
		isReady:      false,
		mu:           new(sync.RWMutex),
		msgsStateMap: map[kafka.Offset]bool{},
		lastCommited: startOffset,
		maxReceived:  &kafka.TopicPartition{Topic: &cfg.Topic, Partition: 0, Offset: startOffset},
		commitDur:    15 * time.Second,
	}

	if err = consumer.initializeKafkaTopic(cfg.Host, cfg.Topic); err != nil {
		return nil, err
	}

	if err = c.Assign([]kafka.TopicPartition{
		{Topic: &consumer.topic, Partition: 0, Offset: startOffset},
	}); err != nil {
		return nil, err
	}

	go consumer.commitOffsetLoop()
	go consumer.checkReadyToAccept()
	go consumer.readMsgLoop()

	return consumer, nil
}

func (c *KafkaConsumer) Close() {
	close(c.exitCH)
}

func (c *KafkaConsumer) readMsgLoop() {
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
			logrus.WithError(err).Error("consumer read error")
			continue
		}

		c.appendMsgState(&msg.TopicPartition)
		c.msgCH <- shared.NewMessage(&msg.TopicPartition, msg.Value)
	}
}

func (c *KafkaConsumer) appendMsgState(tp *kafka.TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgsStateMap[tp.Offset] = false
	if c.maxReceived.Offset < tp.Offset {
		c.maxReceived = &kafka.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    tp.Offset,
		}
	}
}

func (c *KafkaConsumer) MarkAsComplete(tp *kafka.TopicPartition) {
	logrus.WithField("offset", tp.Offset).Info("MarkAsComplete")
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgsStateMap[tp.Offset] = true
}

func (c *KafkaConsumer) commitOffsetLoop() {
	ticker := time.NewTicker(c.commitDur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			if c.lastCommited == c.maxReceived.Offset {
				c.mu.Unlock()
				continue
			}

			safeCommit := *c.maxReceived
			for offset := c.lastCommited; offset < c.maxReceived.Offset; offset++ {
				completed, exists := c.msgsStateMap[offset]
				if !exists {
					continue
				}
				if completed {
					delete(c.msgsStateMap, offset)
					continue
				}
				safeCommit.Offset = offset
				break
			}
			c.mu.Unlock()

			if safeCommit.Offset == c.lastCommited {
				continue
			}

			_, err := c.consumer.CommitOffsets([]kafka.TopicPartition{safeCommit})
			if err != nil {
				logrus.WithError(err).Errorf("failed to commit offset %d", safeCommit.Offset)
				continue
			}

			c.mu.Lock()
			c.lastCommited = safeCommit.Offset
			c.mu.Unlock()
			logrus.WithField("offset", safeCommit.Offset).Warn("committed offset")

		case <-c.exitCH:
			return
		}
	}
}

func (c *KafkaConsumer) initializeKafkaTopic(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	logrus.WithField("topic", topicName).Info("creating topic")
	topicSpec := kafka.TopicSpecification{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, []kafka.TopicSpecification{topicSpec})
	if err != nil {
		return err
	}

	for _, result := range results {
		if result.Error.Code() == kafka.ErrTopicAlreadyExists {
			logrus.WithField("topic", result.Topic).Info("topic already exists")
			continue
		}
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("failed to create topic: %v", result.Error)
		}
		logrus.WithField("topic", result.Topic).Info("topic created")
	}

	return c.waitForTopicReady(brokers, topicName)
}

func (c *KafkaConsumer) waitForTopicReady(brokers, topicName string) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	for {
		time.Sleep(time.Second)
		metadata, err := adminClient.GetMetadata(&topicName, false, 5000)
		if err != nil {
			logrus.WithError(err).Error("metadata fetch failed")
			continue
		}

		topicMeta, exists := metadata.Topics[topicName]
		if !exists {
			continue
		}

		allReady := true
		for _, partition := range topicMeta.Partitions {
			if partition.Error.Code() != kafka.ErrNoError || partition.Leader == -1 {
				allReady = false
				break
			}
		}

		logrus.WithField("ready", allReady).Info("topic readiness check")
		if allReady {
			return nil
		}
	}
}

func (c *KafkaConsumer) checkReadyToAccept() error {
	defer func() { c.isReady = true }()
	for {
		select {
		case <-c.readyCH:
			return nil
		default:
			time.Sleep(time.Second)
			isReady, err := c.readyCheck()
			if err != nil {
				logrus.WithError(err).Error("consumer ready check failed")
				return err
			}
			logrus.WithField("ready", isReady).Info("consumer assignment check")
			if isReady {
				return nil
			}
		}
	}
}

func (c *KafkaConsumer) readyCheck() (bool, error) {
	assignment, err := c.consumer.Assignment()
	if err != nil {
		return false, err
	}
	return len(assignment) > 0, nil
}
