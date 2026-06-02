package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type ResultPayload struct {
	TaskID     string      `json:"task_id"`
	Status     string      `json:"status"`
	Stdout     string      `json:"stdout"`
	Stderr     string      `json:"stderr"`
	ResultData interface{} `json:"result_data"` // Can be anything, will marshal back to JSON string
}

type Consumer struct {
	client      *kgo.Client
	topic       string
	subscribers map[string]chan ResultPayload
	mu          sync.Mutex
}

func NewConsumer(brokers []string, groupID string, topic string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		client:      client,
		topic:       topic,
		subscribers: make(map[string]chan ResultPayload),
	}, nil
}

func (c *Consumer) Subscribe(taskID string) <-chan ResultPayload {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan ResultPayload, 1)
	c.subscribers[taskID] = ch
	return ch
}

func (c *Consumer) Unsubscribe(taskID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.subscribers[taskID]; ok {
		close(ch)
		delete(c.subscribers, taskID)
	}
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Printf("Consumer poll errors: %v", errs)
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			var res ResultPayload
			if err := json.Unmarshal(record.Value, &res); err != nil {
				log.Printf("Failed to unmarshal result: %v", err)
				continue
			}

			c.mu.Lock()
			ch, exists := c.subscribers[res.TaskID]
			c.mu.Unlock()

			if exists {
				ch <- res
			}
		}
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
