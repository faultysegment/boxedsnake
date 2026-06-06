package history

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/faultysegment/boxedsnake/internal/kafka"
)

type Consumer struct {
	client *kgo.Client
	db     *DB
}

func NewConsumer(brokers []string, groupID string, topic string, database *DB) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		client: client,
		db:     database,
	}, nil
}

func (c *Consumer) Start(ctx context.Context) {
	log.Println("Starting History Consumer to listen for task results...")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := c.client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}
			fetches.EachError(func(topic string, partition int32, err error) {
				log.Printf("History Consumer fetch error topic %s partition %d: %v", topic, partition, err)
			})

			fetches.EachRecord(func(record *kgo.Record) {
				var res kafka.ResultPayload
				if err := json.Unmarshal(record.Value, &res); err != nil {
					log.Printf("History Consumer failed to unmarshal result: %v", err)
					return
				}

				resDataStr := ""
				if res.ResultData != nil {
					b, _ := json.Marshal(res.ResultData)
					resDataStr = string(b)
				}

				taskResult := &TaskResult{
					ID:         uuid.New().String(),
					TaskID:     res.TaskID,
					Status:     res.Status,
					Stdout:     res.Stdout,
					Stderr:     res.Stderr,
					ResultData: resDataStr,
					ExecutedAt: time.Now(),
				}

				if err := c.db.InsertTaskResult(taskResult); err != nil {
					log.Printf("History Consumer failed to save task result for %s: %v", res.TaskID, err)
				} else {
					log.Printf("History Consumer saved task result for %s", res.TaskID)
				}
			})
		}
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
