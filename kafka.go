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

func createKafkaValueMsg(ctx context.Context, fileRowCh chan fileRow) chan kafkaMessageInt {

	resultCh := make(chan kafkaMessageInt)
	const stage string = "kafka"

	go func() {
		defer close(resultCh)
		defer fmt.Println("createKafkaValueMsg closing...")

		sendResult := func(e *kafkaMessage) {
			select {
			case <-ctx.Done():
			case resultCh <- e:
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case rowEvent, ok := <-fileRowCh:
				if !ok {
					return
				}

				if rowEvent.getError() != nil {
					sendResult(&kafkaMessage{
						err: rowEvent.getError(),
					})
					break
				}

				row := (rowEvent.getData()).(map[string]string)
				jsonStr, err := json.Marshal(row)
				if err != nil {
					e := wrapError(fmt.Errorf("createKafkaMessage: json marshal failed %w", err))
					e.Misc = map[string]interface{}{
						"file":       rowEvent.fileName,
						"lineNumber": rowEvent.lineNumber(),
						"stage":      stage,
					}
					sendResult(&kafkaMessage{
						err: e,
					})
					break
				}

				sendResult(&kafkaMessage{
					msg: &kafka.Message{
						Key:   []byte(rowEvent.fileName()),
						Value: jsonStr,
					},
				})

				if rowEvent.getOnDone() != nil {
					f := *rowEvent.getOnDone()
					f()
				}

			}
		}

	}()

	return resultCh
}

type kafkaMessage struct {
	msg  *kafka.Message
	done *func()
	err  error
}

func (km *kafkaMessage) getOnDone() *func() {
	return km.done
}

func (km *kafkaMessage) message() *kafka.Message {
	return km.msg
}

func (km *kafkaMessage) getError() error {
	return km.err
}

func createKafkaErrMsg(ctx context.Context, genericEventCh chan genericEventInt) chan kafkaMessageInt {

	resultCh := make(chan kafkaMessageInt)

	go func() {
		defer close(resultCh)
		defer fmt.Println("createKafkaErrMsg closing...")

		sendResult := func(e *kafkaMessage) {
			select {
			case <-ctx.Done():
			case resultCh <- e:
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case gEvnt, ok := <-genericEventCh:

				if !ok {
					return
				}

				if gEvnt.getError() != nil {

					var kafkaErrMsg *kafkaMessage
					var appErr *appError

					if errors.As(gEvnt.getError(), &appErr) {
						data, err := appErr.toJSON()
						if err != nil {
							panic(err)
						}

						kafkaErrMsg = &kafkaMessage{
							msg: &kafka.Message{
								Key:   []byte(""),
								Value: data,
							},
						}

					} else {

						kafkaErrMsg = &kafkaMessage{
							msg: &kafka.Message{
								Key:   []byte(""),
								Value: []byte(gEvnt.getError().Error()),
							},
						}

					}

					sendResult(kafkaErrMsg)

					if gEvnt.getOnDone() != nil {
						f := *gEvnt.getOnDone()
						f()
					}
				}

			}
		}

	}()
	return resultCh
}

func sendToKafka(ctx context.Context, kafkaMessageCh chan kafkaMessageInt, topic string) chan genericEventInt {

	resultCh := make(chan genericEventInt)

	kw, err := kafkaWriter(topic)
	if err != nil {
		panic(err)
	}

	go func() {
		defer close(resultCh)
		defer fmt.Println("sendToKafka closing...")

		sendResult := func(r *genericEvent) {
			select {
			case <-ctx.Done():
				return
			case resultCh <- r:
			}

		}

		messageChunk := []kafka.Message{}
		for {
			select {
			case <-ctx.Done():
				return
			case kafkaMSG, ok := <-kafkaMessageCh:

				if !ok {
					if err := kw.WriteMessages(ctx, messageChunk...); err != nil {
						panic(err)
					}
					return
				}

				if kafkaMSG.getError() != nil {
					sendResult(&genericEvent{
						err: kafkaMSG.getError(),
					})
					break
				}

				messageChunk = append(messageChunk, *kafkaMSG.message())
				if len(messageChunk) >= 100 {
					if err := kw.WriteMessages(ctx, messageChunk...); err != nil {
						panic(err)
					}
					messageChunk = []kafka.Message{}
				}

				if kafkaMSG.getOnDone() != nil {
					f := *kafkaMSG.getOnDone()
					f()
				}

			}
		}

	}()
	return resultCh
}

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
