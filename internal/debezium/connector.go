package debezium

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	connectURL    = "http://localhost:8083"
	connectorName = "orders-connector"
)

type connectorConfig struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

func RegisterConnector() error {
	if err := waitForConnect(); err != nil {
		return err
	}

	exists, err := connectorExists()
	if err != nil {
		return err
	}
	if exists {
		logrus.WithField("connector", connectorName).Info("connector already registered")
		return nil
	}

	cfg := connectorConfig{
		Name: connectorName,
		Config: map[string]string{
			"connector.class":                          "io.debezium.connector.postgresql.PostgresConnector",
			"database.hostname":                        "postgres",
			"database.port":                            "5432",
			"database.user":                            "kafka_user",
			"database.password":                        "kafka_pass",
			"database.dbname":                          "kafkadb",
			"topic.prefix":                             "cdc",
			"table.include.list":                       "public.orders",
			"plugin.name":                              "pgoutput",
			"key.converter":                            "org.apache.kafka.connect.storage.StringConverter",
			"value.converter":                          "io.confluent.connect.protobuf.ProtobufConverter",
			"value.converter.schema.registry.url":      "http://schema-registry:8081",
			"value.converter.auto.register.schemas":    "true",
		},
	}

	b, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal connector config: %w", err)
	}

	resp, err := http.Post(connectURL+"/connectors", "application/json", bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("post connector config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status from connect: %s", resp.Status)
	}

	logrus.WithField("connector", connectorName).Info("connector registered")
	return nil
}

func connectorExists() (bool, error) {
	resp, err := http.Get(connectURL + "/connectors/" + connectorName)
	if err != nil {
		return false, fmt.Errorf("check connector: %w", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK, nil
}

func waitForConnect() error {
	logrus.Info("waiting for kafka connect...")
	for i := range 30 {
		resp, err := http.Get(connectURL + "/connectors")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			logrus.Info("kafka connect ready")
			return nil
		}
		logrus.WithField("attempt", i+1).Info("kafka connect not ready, retrying")
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("kafka connect not ready after 30 attempts")
}
