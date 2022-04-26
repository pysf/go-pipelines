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

type output struct {
	err  error
	sent int
}

const messageBulkSize int = 60

func sendToKafka(ctx context.Context, fileProcessorCh chan fileProcessorEvent) chan output {

	resultCh := make(chan output)

	sendResult := func(r output) bool {
		select {
		case <-ctx.Done():
			return true
		case resultCh <- r:
			return false
		}
	}

	go func() {
		defer close(resultCh)
		defer fmt.Println("kafka closing...")

		sendMessage, err := createKafkaProducer(ctx, sendResult)
		if err != nil {
			sendResult(output{
				err: err,
			})
			return
		}

		fpEvents := []fileProcessorEvent{}

		for {
			select {
			case <-ctx.Done():
				sendMessage(fpEvents)
				return
			case fpEvent, ok := <-fileProcessorCh:
				if !ok {
					sendMessage(fpEvents)
					return
				}

				fpEvents = append(fpEvents, fpEvent)
				if len(fpEvents) >= messageBulkSize {
					sendMessage(fpEvents[:])
					fpEvents = []fileProcessorEvent{}
				}
			}
		}

	}()

	return resultCh
}

func createKafkaProducer(ctx context.Context, sendResult func(r output) bool) (func(fpe []fileProcessorEvent), error) {

	kw, err := kafkaWriter()
	if err != nil {
		return nil, err
	}

	return func(fpEvents []fileProcessorEvent) {

		kafkaMessages := []kafka.Message{}

		for _, fpEvent := range fpEvents {

			if fpEvent.getError() != nil {
				sendResult(output{
					err: wrapError(fpEvent.getError()),
				})
				continue
			}

			row := (fpEvent.getData()).(map[string]string)
			jsonStr, err := json.Marshal(row)
			if err != nil {
				sendResult(output{
					err: wrapError(fmt.Errorf("sendMessageToKafka: json unmalshal failed %w", err)),
				})
				continue
			}
			// fmt.Println(string(jsonStr))
			kafkaMessages = append(kafkaMessages, kafka.Message{
				Key:   []byte(""),
				Value: jsonStr,
			})
		}

		err := kw.WriteMessages(ctx, kafkaMessages...)
		if err != nil {
			sendResult(output{
				err: wrapError(fmt.Errorf("sendMessageToKafka: %w", err)),
			})
			return
		}

		sendResult(output{
			sent: len(fpEvents),
		})

	}, nil

}

func kafkaWriter() (*kafka.Writer, error) {
	topic, exist := os.LookupEnv("KAFKA_TOPIC")
	if !exist {
		return nil, fmt.Errorf("sendToKafka: KAFKA_TOPIC is empty ")
	}

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
