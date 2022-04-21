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

func fetch(ctx context.Context, s3StageCh chan S3Stage) chan ParserStage {

	resultCh := make(chan ParserStage)

	go func() {
		defer close(resultCh)

		for {

			select {
			case <-ctx.Done():
				return
			case ev, ok := <-s3StageCh:
				if !ok {
					return
				}

				s3StageEvent := ev.(*S3StageEvent)

				if s3StageEvent.err != nil {
					resultCh <- &ParserStageEvent{
						err: s3StageEvent.err,
					}
				}

				f, err := fetchFile(s3StageEvent)

				resultCh <- &ParserStageEvent{
					file: f,
					err:  err,
					onDone: func() {
						s3StageEvent.onDone()
						fmt.Println("S3 on done")
					},
				}

			}
		}
	}()

	return resultCh
}

func fetchFile(s3StegeEvent *S3StageEvent) (*os.File, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1"),
	})
	if err != nil {
		return nil, fmt.Errorf("fetchS3Files: failed to connect to aws %w", err)
	}

	key := s3StegeEvent.key
	bucket := s3StegeEvent.bucket
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

type S3Stage interface {
	setBucket(string)
	getBucket() string
	getKey() string
	setKey(string)
}

type S3StageEvent struct {
	err    error
	key    string
	bucket string
	onDone func()
}

func (e *S3StageEvent) setBucket(b string) {
	e.bucket = b
}

func (e *S3StageEvent) getBucket() string {
	return e.bucket
}

func (e *S3StageEvent) setKey(k string) {
	e.key = k
}

func (e *S3StageEvent) getKey() string {
	return e.key
}

// func (e *S3StageEvent) getError() error {
// 	return e.err
// }

// func (e *S3StageEvent) setError(err error) {
// 	e.err = err
// }
