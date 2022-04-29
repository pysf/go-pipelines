package main

import (
	"github.com/segmentio/kafka-go"
	"os"
)

type genericEventInt interface {
	getError() error
	getOnDone() *func()
}

type genericEvent struct {
	err  error
	done *func()
}

func (ee *genericEvent) getError() error {
	return ee.err
}

func (ge *genericEvent) getOnDone() *func() {
	return ge.done
}

type kafkaMessageInt interface {
	genericEventInt
	getError() error
	message() *kafka.Message
}

type fileInfo interface {
	genericEventInt
	file() *os.File
}

type fileRow interface {
	lineNumber() int
	fileName() string
	getData() interface{}
	genericEventInt
}

type s3Notification interface {
	bucket() string
	key() string
	genericEventInt
}
