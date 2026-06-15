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
		return err
	}
	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var gobBytes bytes.Buffer
	enc := gob.NewEncoder(&gobBytes)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		// DeliveryMode: ,
		// Timestamp: ,
		ContentType: "application/gob",
		Body:        gobBytes.Bytes(),
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
	if err != nil {
		return err
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
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	jsonDecoder := func(message []byte) (T, error) {
		var msg T
		err := json.Unmarshal(message, &msg)
		if err != nil {
			return msg, err
		}
		return msg, nil
	}
	err := subscribe(conn, exchange, queueName, key, simpleQueueType, handler, jsonDecoder)
	if err != nil {
		return err
	}

	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	gobDecoder := func(message []byte) (T, error) {
		messageReader := bytes.NewBuffer(message)
		decoder := gob.NewDecoder(messageReader)
		var msg T
		err := decoder.Decode(&msg)
		if err != nil {
			return msg, err
		}
		return msg, nil
	}
	err := subscribe(conn, exchange, queueName, key, simpleQueueType, handler, gobDecoder)
	if err != nil {
		return err
	}

	return nil
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	// establish channel on given connection to recieve messages
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}
	// queue messages to channel to consume them
	m, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for message := range m {
			msg, err := unmarshaller(message.Body)
			if err != nil {
				fmt.Printf("unexpected error: %v\n", err)
				continue
			}
			acktype := handler(msg)
			switch acktype {
			case Ack:
				err = message.Ack(false)
				if err != nil {
					fmt.Printf("unexpected error: %v\n", err)
					continue
				}
				fmt.Println("Message processed successfully")
			case NackRequeue:
				err = message.Nack(false, true)
				if err != nil {
					fmt.Printf("unexpected error: %v\n", err)
					continue
				}
				fmt.Println("Message not processed successfully, should be requeued")
			case NackDiscard:
				err = message.Nack(false, false)
				if err != nil {
					fmt.Printf("unexpected error: %v\n", err)
					continue
				}
				fmt.Println("Message not processed successfully, should be discarded")
			}
		}
		fmt.Println("consumer exiting goroutine")
	}()
	return nil
}
