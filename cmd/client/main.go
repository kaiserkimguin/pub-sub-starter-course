package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	gamelogic "github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	pubsub "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"

	newConntection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
	}

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}
	queueName := routing.PauseKey + "." + userName
	_, queue, err := pubsub.DeclareAndBind(newConntection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Programm is shutting down. Connection is closed.")
}
