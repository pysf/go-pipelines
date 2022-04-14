package main

import (
	"context"
	"fmt"
	"time"

	"github.com/pysf/s3-to-kafka-go/pkg/s3"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	interval := time.Duration(2)

	fetchRequest := make(chan s3.FetchRequest)
	response, _ := s3.Fetch(ctx, interval/2, fetchRequest)

	re := s3.FetchRequest{
		Bucket: "pysf-kafka-to-s3",
		Key:    "order_delivery_items.csv",
	}

	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()

	go func() {
		for {
			fmt.Println("Sending Req...")
			fetchRequest <- re
		}
	}()

	for {
		select {
		case re, ok := <-response:
			if !ok {
				return
			}
			fmt.Println(re)
		}
	}

}
