package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestCreateMessage(t *testing.T) {

	jsonStr, err := json.Marshal(map[string]string{
		"name":   "payam",
		"family": "yousefi",
	})

	if err != nil {
		t.Error("failed to create json")
	}

	cases := []struct {
		topic      string
		errorTopic string
		input      FileRow
		expect     KafkaMessageInt
	}{
		{
			topic: "value",
			input: &csvRow{
				data: map[string]string{
					"name":   "payam",
					"family": "yousefi",
				},
				fileName: "test.csv",
				done:     nil,
			},
			expect: &kafkaMessage{
				err:  nil,
				done: nil,
				msg: &kafka.Message{
					Key:   []byte("test.csv"),
					Value: []byte(jsonStr),
					Topic: "value",
				},
			},
		},
		{
			errorTopic: "error",
			input: &csvRow{
				err:      fmt.Errorf("errorMSG"),
				fileName: "test.csv",
				done:     nil,
			},
			expect: &kafkaMessage{
				err:  nil,
				done: nil,
				msg: &kafka.Message{
					Topic: "error",
					Key:   []byte("test.csv"),
					Value: []byte(fmt.Errorf("errorMSG").Error()),
				},
			},
		},
	}

	for i, c := range cases {

		ks := &kafkaStage{
			errorTopic: c.errorTopic,
			valueTopic: c.topic,
		}
		ctx, cancel := context.WithCancel(context.Background())

		fileRowCh := make(chan FileRow)
		result := ks.CreateMessage(ctx, fileRowCh)

		go func() {
			fileRowCh <- c.input
		}()

		select {
		case r := <-result:
			if !reflect.DeepEqual(r.Message().Key, c.expect.Message().Key) {
				t.Errorf("case (%d), expect key %v got %v", i, r.Message().Key, c.expect.Message().Key)
			}
			if !reflect.DeepEqual(r.Message().Value, c.expect.Message().Value) {
				t.Errorf("case (%d), expect value %v got %v", i, r.Message().Value, c.expect.Message().Value)
			}

			if !reflect.DeepEqual(r.Message().Topic, c.expect.Message().Topic) {
				t.Errorf("case (%d), expect topic %v got %v", i, r.Message().Topic, c.expect.Message().Topic)
			}

		case <-time.After(1 * time.Second):
			t.Fatalf("%d ,timedout!", i)
		}
		cancel()

	}
}
