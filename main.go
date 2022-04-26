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

	csvResultCh := sendToKafka(ctx, processCsv(ctx, fetch(ctx, messages(ctx, QUEUE))))

	for result := range csvResultCh {

		if result.err != nil {
			var pip *pipelineError
			if errors.As(result.err, &pip) {
				fmt.Printf("Error: %v \n", pip.Error())
				continue
			} else {
				panic(result.err)
			}
		}
		fmt.Printf("%v Message sent \n", result.sent)
	}
	fmt.Println(time.Since(start))
}
