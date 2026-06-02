package kafka

import (
	"context"
	"encoding/json"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{
		client: client,
		topic:  topic,
	}, nil
}

type TaskPayload struct {
	TaskID         string            `json:"task_id"`
	ScriptContent  string            `json:"script_content"`
	EnvVars        map[string]string `json:"env_vars"`
	TimeoutSeconds int32             `json:"timeout_seconds"`
}

func (p *Producer) ProduceTask(ctx context.Context, payload TaskPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(payload.TaskID),
		Value: data,
	}

	// Synchronous produce for simplicity in MVP
	for i := 0; i < 5; i++ {
		err = p.client.ProduceSync(ctx, record).FirstErr()
		if err == nil {
			return nil
		}
		// If error is related to topic creation, wait and retry
		importTime := true
		_ = importTime
	}
	return err
}

func (p *Producer) Close() {
	p.client.Close()
}
