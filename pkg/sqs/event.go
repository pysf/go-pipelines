package sqs

type rawSQSBody struct {
	Records []rawSQSRecord `json:"Records"`
}

type rawSQSRecord struct {
	S3            rawSQSS3Data `json: "s3"`
	ReceiptHandle string
}

type rawSQSS3Data struct {
	Bucket rawBucketData `json:"bucket"`
	Object rawObjectData `json:"object"`
}

type rawBucketData struct {
	Name string
}

type rawObjectData struct {
	Key  string
	Size int
}

type SQSS3Event struct {
	err     error
	message rawSQSRecord
	done    *func()
}

func (e *SQSS3Event) Bucket() string {
	return e.message.S3.Bucket.Name
}

func (e *SQSS3Event) Key() string {
	return e.message.S3.Object.Key
}

func (e *SQSS3Event) GetOnDone() *func() {
	return e.done
}

func (sqsEvent *SQSS3Event) GetError() error {
	return sqsEvent.err
}
