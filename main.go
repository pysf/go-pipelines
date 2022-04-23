package main

import (
	"context"
	"fmt"
)

var QUEUE string = "s3-events"

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	csvDataCh := sendToKafka(ctx, processCsv(ctx, fetch(ctx, messages(ctx, QUEUE))))

	for o := range csvDataCh {
		fmt.Println(o)
	}
}
