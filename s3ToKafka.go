package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
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

	resultCh := sendToKafka(ctx, createKafkaErrMsg(ctx, sendToKafka(ctx, createKafkaValueMsg(ctx, processCsv(ctx, fetch(ctx, messages(ctx, QUEUE)))), valueTopic)), errTopic)

	for result := range resultCh {

		if result.getError() != nil {
			var pip *appError
			if errors.As(result.getError(), &pip) {
				fmt.Printf("Error: %v \n", pip.Error())
				continue
			} else {
				fmt.Printf("Critical Error: %v \n", result.getError())
				panic(result)
			}
		} else {
			if result.getOnDone() != nil {
				f := *result.getOnDone()
				f()
			}
		}
	}
	fmt.Println(time.Since(start))
}
