package kafka

import (
	"github.com/segmentio/kafka-go"
)

type KafkaMessage struct {
	msg  *kafka.Message
	done *func()
	err  error
}

func (km *KafkaMessage) GetOnDone() *func() {
	return km.done
}

func (km *KafkaMessage) Message() *kafka.Message {
	return km.msg
}

func (km *KafkaMessage) GetError() error {
	return km.err
}
