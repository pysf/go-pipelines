package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type output struct {
	done bool
	err  error
}

func sendToKafka(ctx context.Context, fileProcessorCh chan fileProcessorEvent) chan output {

	resultCh := make(chan output)
	go func() {

		sendResult := func(r output) {
			for {
				select {
				case <-ctx.Done():
					return
				case resultCh <- r:
				}
			}
		}

		kw, err := kafkaWriter()
		if err != nil {
			sendResult(output{
				done: false,
				err:  err,
			})
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case fpe := <-fileProcessorCh:
				sendMessageToKafka(ctx, kw, fpe)
				sendResult(output{
					done: true,
				})
			}
		}

	}()

	return resultCh
}

func kafkaWriter() (*kafka.Writer, error) {
	topic, exist := os.LookupEnv("KAFKA_TOPIC")
	if !exist {
		return nil, errors.New("sendToKafka: KAFKA_TOPIC is empty ")
	}

	username, exist := os.LookupEnv("KAFKA_USERNAME")
	if !exist {
		return nil, errors.New("sendToKafka: KAFKA_USERNAME is empty ")
	}

	password, exist := os.LookupEnv("KAFKA_PASSWORD")
	if !exist {
		return nil, errors.New("sendToKafka: KAFKA_PASSWORD is empty ")
	}

	brokers, exist := os.LookupEnv("KAFKA_BROKERS")
	if !exist {
		return nil, errors.New("sendToKafka: KAFKA_BROKERS is empty ")
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

func sendMessageToKafka(ctx context.Context, kw *kafka.Writer, fpe fileProcessorEvent) error {

	row := (fpe.getData()).(map[string]string)

	// ctx := context.Background()
	jsonStr, err := json.Marshal(row)
	if err != nil {
		return err
	}
	fmt.Println("sendToKafka", string(jsonStr))

	// err = kw.WriteMessages(ctx, kafka.Message{
	// 	Key:   []byte(""),
	// 	Value: jsonStr,
	// })

	// if err != nil {
	// 	fmt.Println("sendToKafka", err)
	// 	return err
	// }

	return nil
}
