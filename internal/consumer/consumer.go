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

type partitionState struct {
	msgsStateMap map[kafka.Offset]bool
	lastCommited kafka.Offset
	maxReceived  kafka.Offset
}

type KafkaConsumer struct {
	consumer  *kafka.Consumer
	topic     string
	msgCH     chan<- *shared.Message
	readyCH   chan struct{}
	exitCH    chan struct{}
	isReady   bool
	commitDur time.Duration

	mu         *sync.RWMutex
	partitions map[int32]*partitionState
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

	consumer := &KafkaConsumer{
		consumer:   c,
		topic:      cfg.Topic,
		msgCH:      msgCH,
		readyCH:    make(chan struct{}),
		exitCH:     make(chan struct{}),
		isReady:    false,
		commitDur:  15 * time.Second,
		mu:         new(sync.RWMutex),
		partitions: map[int32]*partitionState{},
	}

	if err = consumer.initializeKafkaTopic(cfg.Host, cfg.Topic); err != nil {
		return nil, err
	}

	allPartitions, err := consumer.fetchAllPartitions(cfg.Host, cfg.Topic)
	if err != nil {
		return nil, err
	}

	if err = consumer.assignPartitions(allPartitions); err != nil {
		return nil, err
	}

	go consumer.commitOffsetLoop()
	go consumer.checkReadyToAccept()
	go consumer.readMsgLoop()

	return consumer, nil
}

func (c *KafkaConsumer) fetchAllPartitions(brokers, topicName string) ([]int32, error) {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		return nil, err
	}
	defer adminClient.Close()

	metadata, err := adminClient.GetMetadata(&topicName, false, 5000)
	if err != nil {
		return nil, err
	}

	topicMeta, exists := metadata.Topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic %s not found in metadata", topicName)
	}

	ids := make([]int32, 0, len(topicMeta.Partitions))
	for _, p := range topicMeta.Partitions {
		ids = append(ids, p.ID)
	}
	return ids, nil
}

func (c *KafkaConsumer) assignPartitions(partitionIDs []int32) error {
	tps := make([]kafka.TopicPartition, 0, len(partitionIDs))
	for _, id := range partitionIDs {
		tps = append(tps, kafka.TopicPartition{Topic: &c.topic, Partition: id})
	}

	committed, err := c.consumer.Committed(tps, 5000)
	if err != nil {
		return err
	}

	assignTPs := make([]kafka.TopicPartition, 0, len(committed))
	for _, tp := range committed {
		startOffset := kafka.OffsetBeginning
		if tp.Offset != kafka.OffsetInvalid {
			startOffset = tp.Offset
		}

		logrus.WithFields(logrus.Fields{
			"partition":    tp.Partition,
			"start_offset": startOffset,
		}).Info("consumer starting position")

		c.partitions[tp.Partition] = &partitionState{
			msgsStateMap: map[kafka.Offset]bool{},
			lastCommited: startOffset,
			maxReceived:  startOffset,
		}

		assignTPs = append(assignTPs, kafka.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    startOffset,
		})
	}

	return c.consumer.Assign(assignTPs)
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

	ps, ok := c.partitions[tp.Partition]
	if !ok {
		return
	}
	ps.msgsStateMap[tp.Offset] = false
	if tp.Offset > ps.maxReceived {
		ps.maxReceived = tp.Offset
	}
}

func (c *KafkaConsumer) MarkAsComplete(tp *kafka.TopicPartition) {
	logrus.WithFields(logrus.Fields{
		"partition": tp.Partition,
		"offset":    tp.Offset,
	}).Info("MarkAsComplete")

	c.mu.Lock()
	defer c.mu.Unlock()

	ps, ok := c.partitions[tp.Partition]
	if !ok {
		return
	}
	ps.msgsStateMap[tp.Offset] = true
}

func (c *KafkaConsumer) commitOffsetLoop() {
	ticker := time.NewTicker(c.commitDur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			toCommit := make([]kafka.TopicPartition, 0)
			for partID, ps := range c.partitions {
				if ps.lastCommited == ps.maxReceived {
					continue
				}

				safeOffset := ps.maxReceived
				for offset := ps.lastCommited; offset < ps.maxReceived; offset++ {
					completed, exists := ps.msgsStateMap[offset]
					if !exists {
						continue
					}
					if completed {
						delete(ps.msgsStateMap, offset)
						continue
					}
					safeOffset = offset
					break
				}

				if safeOffset == ps.lastCommited {
					continue
				}

				toCommit = append(toCommit, kafka.TopicPartition{
					Topic:     &c.topic,
					Partition: partID,
					Offset:    safeOffset,
				})
			}
			c.mu.Unlock()

			if len(toCommit) == 0 {
				continue
			}

			_, err := c.consumer.CommitOffsets(toCommit)
			if err != nil {
				logrus.WithError(err).Error("failed to commit offsets")
				continue
			}

			c.mu.Lock()
			for _, tp := range toCommit {
				c.partitions[tp.Partition].lastCommited = tp.Offset
				logrus.WithFields(logrus.Fields{
					"partition": tp.Partition,
					"offset":    tp.Offset,
				}).Info("committed offset")
			}
			c.mu.Unlock()

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
		NumPartitions:     3,
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
