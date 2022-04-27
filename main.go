package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var QUEUE string = "s3-events"

func main() {

	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := sendErrorToKafka(ctx, sendRowToKafka(ctx, processCsv(ctx, fetch(ctx, messages(ctx, QUEUE)))))

	for result := range resultCh {

		var pip *pipelineError
		if errors.As(result.getError(), &pip) {
			fmt.Printf("Error: %v \n", pip.Error())
			continue
		} else {
			fmt.Printf("Critical Error: %v \n", result.getError())
			panic(result)
		}

	}
	fmt.Println(time.Since(start))
}
