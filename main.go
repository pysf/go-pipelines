package main

import (
	"context"
	"fmt"
)

var QUEUE string = "s3-events"

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := read(ctx, fetchS3Files(ctx, getSQSEvents(ctx, &QUEUE)))

	for fEvent := range resultCh {
		if fEvent.err != nil {
			fmt.Printf("Error: %v \n", fEvent.err)
			continue
		}
		fmt.Printf("processed: %v \n", fEvent.processed)
		fmt.Printf("file name: %v \n", fEvent.sqsRecord.S3.Object.Key)

	}
}
