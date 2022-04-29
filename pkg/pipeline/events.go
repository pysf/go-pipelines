package pipeline

import (
	"github.com/segmentio/kafka-go"
	"os"
)

type GenericEventInt interface {
	GetError() error
	GetOnDone() *func()
}

type GenericEvent struct {
	Err  error
	Done *func()
}

func (ee *GenericEvent) GetError() error {
	return ee.Err
}

func (ge *GenericEvent) GetOnDone() *func() {
	return ge.Done
}

type KafkaMessageInt interface {
	GenericEventInt
	GetError() error
	Message() *kafka.Message
}

type FileInfo interface {
	GenericEventInt
	File() *os.File
}

type FileRow interface {
	LineNumber() int
	FileName() string
	GetData() interface{}
	GenericEventInt
}

type S3Notification interface {
	Bucket() string
	Key() string
	GenericEventInt
}
