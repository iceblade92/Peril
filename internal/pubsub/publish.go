package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	ctx := context.Background()
	err = ch.PublishWithContext(ctx, exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		return err
	}

	return nil
}
