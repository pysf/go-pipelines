package pipeline

import (
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

func kafkaWriter(topic string) (*kafka.Writer, error) {

	username, exist := os.LookupEnv("KAFKA_USERNAME")
	if !exist {
		panic(fmt.Errorf("sendToKafka: KAFKA_USERNAME is empty "))
	}

	password, exist := os.LookupEnv("KAFKA_PASSWORD")
	if !exist {
		panic(fmt.Errorf("sendToKafka: KAFKA_PASSWORD is empty "))
	}

	brokers, exist := os.LookupEnv("KAFKA_BROKERS")
	if !exist {
		panic(fmt.Errorf("sendToKafka: KAFKA_BROKERS is empty "))
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: username,
			Password: password,
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:   strings.Split(brokers, ","),
		Topic:     topic,
		BatchSize: 100,
		Balancer:  &kafka.Hash{},
		Dialer:    dialer,
	}), nil
}
