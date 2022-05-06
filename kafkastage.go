package pipeline

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

func NewKafkaStege(valueTopic, errorTopic string) *kafkaStage {
	kr, err := kafkaWriter()
	if err != nil {
		panic(fmt.Errorf("failed to create kafka client %w", err))
	}

	messageBatch := messageBatch{
		client: kr,
	}

	return &kafkaStage{
		valueTopic:   valueTopic,
		errorTopic:   errorTopic,
		messageBatch: messageBatch,
	}
}

type kafkaStage struct {
	errorTopic string
	valueTopic string
	messageBatch
}

func (kafkaStage *kafkaStage) CreateMessage(ctx context.Context, fileRowCh chan FileRow) chan KafkaMessageInt {

	resultCh := make(chan KafkaMessageInt)

	go func() {
		defer close(resultCh)

		sendResult := func(msg *kafkaMessage) {
			select {
			case <-ctx.Done():
			case resultCh <- msg:
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case fileRow, ok := <-fileRowCh:
				if !ok {
					return
				}

				var errEvent *kafkaMessage
				if fileRow.GetError() != nil {

					var appErr *AppError

					if errors.As(fileRow.GetError(), &appErr) {
						data, err := appErr.toJSON()
						if err != nil {
							panic(fmt.Errorf("CreateKafkaMessage: error toJson failed %w", err))
						}

						errEvent = &kafkaMessage{
							msg: &kafka.Message{
								Topic: kafkaStage.errorTopic,
								Key:   []byte(fileRow.FileName()),
								Value: data,
							},
						}

					} else {

						errEvent = &kafkaMessage{
							msg: &kafka.Message{
								Topic: kafkaStage.errorTopic,
								Key:   []byte(fileRow.FileName()),
								Value: []byte(fileRow.GetError().Error()),
							},
						}

					}

					sendResult(errEvent)

				} else {
					row := (fileRow.Data()).(map[string]string)
					jsonStr, err := json.Marshal(row)
					if err != nil {
						panic(fmt.Errorf("CreateKafkaMessage: json marshal failed %w", err))
					}

					sendResult(&kafkaMessage{
						msg: &kafka.Message{
							Topic: kafkaStage.valueTopic,
							Key:   []byte(fileRow.FileName()),
							Value: jsonStr,
						},
					})

				}

				if fileRow.GetOnDone() != nil {
					f := *fileRow.GetOnDone()
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

func (km *kafkaMessage) GetOnDone() *func() {
	return km.done
}

func (km *kafkaMessage) Message() *kafka.Message {
	return km.msg
}

func (km *kafkaMessage) GetError() error {
	return km.err
}

func (ks *kafkaStage) SendMessage(ctx context.Context, kafkaMessageCh chan KafkaMessageInt, topic string) chan GenericEventInt {

	resultCh := make(chan GenericEventInt)

	go func() {
		defer close(resultCh)
		defer fmt.Println("sendToKafka closing...")

		sendResult := func(r *GenericEvent) {
			select {
			case <-ctx.Done():
				return
			case resultCh <- r:
			}

		}

		for {
			select {
			case <-ctx.Done():
				return
			case kafkaMSG, ok := <-kafkaMessageCh:

				if !ok {
					if err := ks.messageBatch.send(ctx); err != nil {
						panic(err)
					}
					return
				}

				if kafkaMSG.GetError() != nil {
					sendResult(&GenericEvent{
						Err: kafkaMSG.GetError(),
					})
					break
				}

				ks.messageBatch.add(*kafkaMSG.Message())
				if ks.messageBatch.size() >= 100 {
					if err := ks.messageBatch.send(ctx); err != nil {
						panic(err)
					}
					ks.messageBatch.flush()
				}

				if kafkaMSG.GetOnDone() != nil {
					f := *kafkaMSG.GetOnDone()
					f()
				}

			}
		}

	}()
	return resultCh
}

func kafkaWriter() (*kafka.Writer, error) {

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
		BatchSize: 100,
		Balancer:  &kafka.Hash{},
		Dialer:    dialer,
	}), nil
}

type messageBatch struct {
	client   *kafka.Writer
	messages []kafka.Message
}

func (mb *messageBatch) size() int {
	return len(mb.messages)
}

func (mb *messageBatch) add(m kafka.Message) {
	mb.messages = append(mb.messages, m)
}

func (mb *messageBatch) flush() {
	mb.messages = make([]kafka.Message, 0)
}

func (mb *messageBatch) send(ctx context.Context) error {
	return mb.client.WriteMessages(ctx, mb.messages...)
}
