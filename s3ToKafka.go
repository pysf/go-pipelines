package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/pysf/go-pipelines/pkg/csv"
	"github.com/pysf/go-pipelines/pkg/kafka"
	"github.com/pysf/go-pipelines/pkg/pipeline"
	"github.com/pysf/go-pipelines/pkg/s3"
	"github.com/pysf/go-pipelines/pkg/sqs"
)

var QUEUE string = "s3-events"

func main() {

	valueTopic, exist := os.LookupEnv("KAFKA_VALUE_TOPIC")
	if !exist {
		panic("createKafkaErrorProducer: KAFKA_VALUE_TOPIC in empty")
	}

	errTopic, exist := os.LookupEnv("KAFKA_ERROR_TOPIC")
	if !exist {
		panic("createKafkaErrorProducer: KAFKA_ERROR_TOPIC in empty")
	}

	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := kafka.SendMessage(ctx, kafka.CreateErrMsg(ctx, kafka.SendMessage(ctx, kafka.CreateValueMsg(ctx, csv.Process(ctx, s3.Fetch(ctx, sqs.Messages(ctx, QUEUE)))), valueTopic)), errTopic)

	for result := range resultCh {

		if result.GetError() != nil {
			var pip *pipeline.AppError
			if errors.As(result.GetError(), &pip) {
				fmt.Printf("Error: %v \n", pip.Error())
				continue
			} else {
				fmt.Printf("Critical Error: %v \n", result.GetError())
				panic(result)
			}
		} else {
			if result.GetOnDone() != nil {
				f := *result.GetOnDone()
				f()
			}
		}
	}
	fmt.Println(time.Since(start))
}
