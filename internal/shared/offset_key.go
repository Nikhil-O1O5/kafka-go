package shared

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type OffsetKey struct {
	Partition int32
	Offset    kafka.Offset
}
