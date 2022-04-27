package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/segmentio/kafka-go"
)

func sendErrorToKafka(ctx context.Context, errorCh chan error) chan finalStageEvent {

	resultCh := make(chan finalStageEvent)

	go func() {
		defer close(resultCh)
		defer fmt.Println("sendErrorToKafka closing...")

		sendResult := func(e finalStageEvent) bool {
			select {
			case <-ctx.Done():
				return true
			case resultCh <- e:
				return false
			}
		}

		kafkaErrProducer, err := createKafkaErrProducer(ctx, sendResult)
		if err != nil {
			sendResult(&kafkaErrEvent{
				err: err,
			})
			return
		}

		errorEvents := []error{}
		for {
			select {
			case <-ctx.Done():
				kafkaErrProducer(errorEvents)
				return
			case e, ok := <-errorCh:

				if !ok {
					kafkaErrProducer(errorEvents)
					return
				}

				errorEvents = append(errorEvents, e)
				if len(errorEvents) >= 100 {
					kafkaErrProducer(errorEvents)
				}
			}
		}

	}()
	return resultCh
}

func createKafkaErrProducer(ctx context.Context, sendResult func(finalStageEvent) bool) (func(errs []error), error) {

	topic, exist := os.LookupEnv("KAFKA_ERROR_TOPIC")
	if !exist {
		return nil, fmt.Errorf("createKafkaErrorProducer: KAFKA_ERROR_TOPIC in empty")
	}

	kw, err := kafkaWriter(topic)
	if err != nil {
		return nil, err
	}

	return func(errs []error) {
		var kafkaMessages []kafka.Message
		for _, e := range errs {

			var pe *pipelineError
			var kafkaMessage kafka.Message
			if errors.As(e, &pe) {
				data, err := pe.toJSON()
				if err != nil {
					sendResult(&kafkaErrEvent{
						err: err,
					})
					continue
				}

				kafkaMessage = kafka.Message{
					Key:   []byte(""),
					Value: data,
				}

			} else {
				kafkaMessage = kafka.Message{
					Key:   []byte(""),
					Value: []byte(e.Error()),
				}
			}
			kafkaMessages = append(kafkaMessages, kafkaMessage)
		}

		err := kw.WriteMessages(ctx, kafkaMessages...)
		if err != nil {
			sendResult(&kafkaErrEvent{
				err: err,
			})
		}

	}, nil
}

type kafkaErrEvent struct {
	err error
}

func (kfo *kafkaErrEvent) getError() error {
	return kfo.err
}
