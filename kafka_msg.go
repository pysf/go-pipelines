package pipeline

import (
	"github.com/segmentio/kafka-go"
)

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
