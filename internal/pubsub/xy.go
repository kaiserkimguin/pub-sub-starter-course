package pubsub

import (
	"context"
	"encoding/json"
	"log"

	ampq "github.com/rabbitmq/amqp091-go"
	amqp "github.com/rabbitmq/amqp091-go"
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

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	newChannel, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	durable := true
	autoDelete := false
	exclusive := false
	if queueType == Transient {
		durable = false
		autoDelete = true
		exclusive = true
	}
	newQueue, err := newChannel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = newChannel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	return newChannel, newQueue, nil
}
