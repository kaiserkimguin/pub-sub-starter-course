package pubsub

import (
	"context"
	"encoding/json"
	"log"

	ampq "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *ampq.Channel, exchange, key string, val T) error {
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		log.Fatal("unable to marshal val")
	}
	msg := ampq.Publishing{
		// DeliveryMode: ,
		// Timestamp: ,
		ContentType: "application/json",
		Body:        jsonBytes,
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}
