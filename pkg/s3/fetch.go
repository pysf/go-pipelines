package s3

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type FetchRequest struct {
	Bucket string
	Key    string
}

type FetchResponse struct {
	err  error
	file *os.File
}

func Fetch(ctx context.Context, pulseInterval time.Duration, fetchRequests chan FetchRequest) (chan FetchResponse, chan time.Time) {

	response := make(chan FetchResponse)
	heartbeat := make(chan time.Time)
	//todo: read
	// return heartbeat, filePath

	go func() {
		defer close(response)
		defer close(heartbeat)

		for {

			select {
			case <-ctx.Done():
				fmt.Println("Closing...")
				return
			case <-fetchRequests:
				fmt.Println("Fetching...")
				fr := <-fetchRequests

				sess := session.Must(session.NewSession(&aws.Config{
					Region: aws.String("eu-central-1"),
				}))

				downloader := s3manager.NewDownloader(sess)
				f, err := os.Create(fmt.Sprintf("%v-%v.txt", time.Now(), fr.Key))
				if err != nil {
					fmt.Println("panic...!")
					panic(err)
				}

				n, err := downloader.Download(f, &s3.GetObjectInput{
					Bucket: aws.String(fr.Bucket),
					Key:    aws.String(fr.Key),
				})
				if err != nil {
					fmt.Printf("failed to download file, %v", err)
				}
				fmt.Printf("file downloaded, %d bytes\n", n)

				response <- FetchResponse{
					err:  err,
					file: f,
				}
				fmt.Println("Fetched...!")
				// wr to file

			}
		}
	}()

	return response, heartbeat
}
