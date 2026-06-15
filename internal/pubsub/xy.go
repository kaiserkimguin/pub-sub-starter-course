package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var gobBytes bytes.Buffer
	enc := gob.NewEncoder(&gobBytes)
	err := enc.Encode(val)
	if err != nil {
		log.Fatal(err)
	}

	msg := amqp.Publishing{
		// DeliveryMode: ,
		// Timestamp: ,
		ContentType: "application/gob",
		Body:        gobBytes.Bytes(),
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
	newQueue, err := newChannel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, ampq.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})
	if err != nil {
		log.Fatal(err)
	}

	err = newChannel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	return newChannel, newQueue, nil
}

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
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
			acktype := handler(msg)
			switch acktype {
			case Ack:
				err = message.Ack(false)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println("Message processed successfully")
			case NackRequeue:
				err = message.Nack(false, true)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println("Message not processed successfully, should be requeued")
			case NackDiscard:
				err = message.Nack(false, false)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println("Message not processed successfully, should be discarded")
			}
		}
		fmt.Println("consumer exiting goroutine")
	}()
	return nil
}
