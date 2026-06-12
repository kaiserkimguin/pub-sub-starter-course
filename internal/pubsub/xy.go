package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	fmt.Println("SubscribeJSON called")
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("DeclareAndBind succeeded, calling Consume...")
	m, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	fmt.Println("Consume succeeded, starting goroutine...")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("consumer started")

	go func() {
		for message := range m {
			var msg T
			err = json.Unmarshal(message.Body, &msg)
			if err != nil {
				log.Fatal(err)
			}
			handler(msg)
			err = message.Ack(false)
			if err != nil {
				log.Fatal(err)
			}
		}
		fmt.Println("consumer exiting goroutine")
	}()
	return nil
}
