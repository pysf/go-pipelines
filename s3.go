package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type fileEvent interface {
	file() *os.File
	getError() error
	done()
}

func fetch(ctx context.Context, s3NotificationCh chan s3Notification) chan fileEvent {

	resultCh := make(chan fileEvent)

	go func() {
		defer close(resultCh)

		sendResult := func(r fileEvent) {
			for {
				select {
				case <-ctx.Done():
					return
				case resultCh <- r:
					return
				}
			}
		}

		for {

			select {
			case <-ctx.Done():
				return
			case s3Notif, ok := <-s3NotificationCh:
				if !ok {
					return
				}

				if s3Notif.getError() != nil {
					resultCh <- &s3PipelineEvent{
						err: s3Notif.getError(),
					}
				}

				file, err := download(s3Notif.bucket(), s3Notif.key())

				sendResult(&s3PipelineEvent{
					f:   file,
					err: err,
					onDone: func() {
						s3Notif.done()
						fmt.Println("S3 on done")
					},
				})

			}
		}
	}()

	return resultCh
}

func download(bucket, key string) (*os.File, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1"),
	})
	if err != nil {
		return nil, fmt.Errorf("fetchS3Files: failed to connect to aws %w", err)
	}

	downloader := s3manager.NewDownloader(sess)
	f, err := ioutil.TempFile("", fmt.Sprintf("%v-*", key))
	if err != nil {
		return nil, fmt.Errorf("fetchS3Files: failed to create a new tmp file %w", err)
	}

	_, err = downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		os.Remove(f.Name())
		var s3Error awserr.Error

		if errors.As(err, &s3Error) {
			switch s3Error.Code() {
			case s3.ErrCodeNoSuchKey:
				return nil, wrapError(fmt.Errorf("fetchS3Files: %v file not found %w ", key, err))
			case s3.ErrCodeNoSuchBucket:
				return nil, wrapError(fmt.Errorf("fetchS3Files: %v bucket not found %w ", key, err))
			default:
				return nil, fmt.Errorf("fetchS3Files: failed to download the file %v %w ", key, err)
			}

		} else {
			return nil, fmt.Errorf("fetchS3Files: failed to download the file %v %w ", key, err)
		}

	}

	return f, nil
}

// type s3File struct {
// 	f      *os.File
// 	err    error
// 	onDone func()
// }

// func (e *s3File) file() *os.File {
// 	return e.f
// }

// func (e *s3File) getError() error {
// 	return e.err
// }

// func (e *s3File) done() {
// 	e.onDone()
// }
