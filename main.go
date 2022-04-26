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

	csvDataCh := sendToKafka(ctx, processCsv(ctx, fetch(ctx, messages(ctx, QUEUE))))

	for o := range csvDataCh {

		if o.err != nil {
			var pip *pipelineError
			if errors.As(o.err, &pip) {
				fmt.Printf("Swallow Error: %v \n", pip.Error())
				continue
			} else {
				panic(o.err)
			}
		}
		fmt.Println(o.data)
	}
	fmt.Println(time.Since(start) * time.Second)
}
