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

func fetchS3Files(ctx context.Context, pipeEventCh chan pipelineEvent) chan pipelineEvent {

	resultCh := make(chan pipelineEvent)

	go func() {
		defer close(resultCh)

		for {

			select {
			case <-ctx.Done():
				return
			case pipeEvent, ok := <-pipeEventCh:
				if !ok {
					return
				}
				fetchFile(&pipeEvent)
				resultCh <- pipeEvent

			}
		}
	}()

	return resultCh
}

func fetchFile(pipeEvent *pipelineEvent) {

	if pipeEvent.err != nil {
		return
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-central-1"),
	})
	if err != nil {
		pipeEvent.err = fmt.Errorf("fetchS3Files: failed to connect to aws %w", err)
		return
	}

	key := pipeEvent.sqsRecord.S3.Object.Key
	bucket := pipeEvent.sqsRecord.S3.Bucket.Name
	downloader := s3manager.NewDownloader(sess)
	f, err := ioutil.TempFile("", fmt.Sprintf("%v-*", key))
	if err != nil {
		pipeEvent.err = fmt.Errorf("fetchS3Files: failed to create a new tmp file %w", err)
		return
	}

	_, err = downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {

		var s3Error awserr.Error

		if errors.As(err, &s3Error) {
			switch s3Error.Code() {
			case s3.ErrCodeNoSuchKey:
				pipeEvent.err = wrapError(fmt.Errorf("fetchS3Files: %v file not found %w ", key, err))
			case s3.ErrCodeNoSuchBucket:
				pipeEvent.err = wrapError(fmt.Errorf("fetchS3Files: %v bucket not found %w ", key, err))
			default:
				pipeEvent.err = fmt.Errorf("fetchS3Files: failed to download the file %v %w ", key, err)
			}

		} else {
			pipeEvent.err = fmt.Errorf("fetchS3Files: failed to download the file %v %w ", key, err)
		}
		os.Remove(f.Name())
	}

	pipeEvent.file = f
}
