package shared

type KafkaConfig struct {
	Topic             string
	ConsumerGroup     string
	Host              string
	SchemaRegistryURL string
}

func NewKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Topic:             "local_topic",
		ConsumerGroup:     "local_cg",
		Host:              "localhost",
		SchemaRegistryURL: "http://localhost:8081",
	}
}
