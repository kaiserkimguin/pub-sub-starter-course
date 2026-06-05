package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionString := "amqp://guest:guest@localhost:5672/"

	newConntection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
	}

	newChannel, err := newConntection.Channel()

	defer newConntection.Close()

	fmt.Println("Connection successful")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	exchange := routing.ExchangePerilDirect
	key := routing.PauseKey
	data := routing.PlayingState{
		IsPaused: true,
	}
	err = pubsub.PublishJSON(newChannel, exchange, key, data)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Message sent.")

	<-signalChan
	fmt.Println("Programm is shutting down. Connection is closed.")
}
