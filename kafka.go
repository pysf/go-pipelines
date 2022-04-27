package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const stage string = "kafka"

func sendRowToKafka(ctx context.Context, fileProcessorCh chan fileRow) chan error {

	resultCh := make(chan error)

	sendResult := func(e error) bool {
		select {
		case <-ctx.Done():
			return true
		case resultCh <- e:
			return false
		}
	}

	go func() {
		defer close(resultCh)
		defer fmt.Println("kafka closing...")

		kafkaProducer, err := createKafkaValueProducer(ctx, sendResult)
		if err != nil {
			sendResult(err)
			return
		}

		fpEvents := []fileRow{}

		for {
			select {
			case <-ctx.Done():
				kafkaProducer(fpEvents)
				return
			case fpEvent, ok := <-fileProcessorCh:
				if !ok {
					kafkaProducer(fpEvents)
					return
				}

				fpEvents = append(fpEvents, fpEvent)
				if len(fpEvents) >= 100 {
					kafkaProducer(fpEvents)
					fpEvents = []fileRow{}
				}
			}
		}

	}()

	return resultCh
}

func createKafkaValueProducer(ctx context.Context, sendResult func(r error) bool) (func(rows []fileRow), error) {

	topic, exist := os.LookupEnv("KAFKA_TOPIC")
	if !exist {
		return nil, fmt.Errorf("sendToKafka: KAFKA_TOPIC is empty ")
	}

	kw, err := kafkaWriter(topic)
	if err != nil {
		return nil, err
	}

	return func(rows []fileRow) {

		kafkaMessages := []kafka.Message{}

		for _, fpEvent := range rows {

			if fpEvent.getError() != nil {
				sendResult(fpEvent.getError())
				continue
			}

			row := (fpEvent.getData()).(map[string]string)
			jsonStr, err := json.Marshal(row)
			if err != nil {
				e := wrapError(fmt.Errorf("sendMessageToKafka: json marshal failed %w", err))
				e.Misc = map[string]interface{}{
					"file":       fpEvent.fileName,
					"lineNumber": fpEvent.lineNumber(),
					"stage":      stage,
				}
				sendResult(e)
				continue
			}

			kafkaMessages = append(kafkaMessages, kafka.Message{
				Key:   []byte(fpEvent.fileName()),
				Value: jsonStr,
			})
		}

		err := kw.WriteMessages(ctx, kafkaMessages...)
		if err != nil {
			sendResult(fmt.Errorf("sendMessageToKafka: failed %w", err))
			return
		}

	}, nil

}

func kafkaWriter(topic string) (*kafka.Writer, error) {

	username, exist := os.LookupEnv("KAFKA_USERNAME")
	if !exist {
		return nil, fmt.Errorf("sendToKafka: KAFKA_USERNAME is empty ")
	}

	password, exist := os.LookupEnv("KAFKA_PASSWORD")
	if !exist {
		return nil, fmt.Errorf("sendToKafka: KAFKA_PASSWORD is empty ")
	}

	brokers, exist := os.LookupEnv("KAFKA_BROKERS")
	if !exist {
		return nil, fmt.Errorf("sendToKafka: KAFKA_BROKERS is empty ")
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
