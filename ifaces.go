package pipeline

import (
	"io"

	"github.com/segmentio/kafka-go"
)

type GenericEventInt interface {
	GetError() error
	GetOnDone() *func()
}

type KafkaMessageInt interface {
	GenericEventInt
	GetError() error
	Message() *kafka.Message
}

type FileInfo interface {
	GenericEventInt
	File() io.Reader
	FileName() string
}

type FileRow interface {
	FileName() string
	LineNumber() int
	Data() interface{}
	GenericEventInt
}

type S3Notification interface {
	Bucket() string
	Key() string
	GenericEventInt
}
