package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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
	if err != nil {
		log.Fatal(err)
	}

	logChannel, logQueue, err := pubsub.DeclareAndBind(newConntection, routing.ExchangePerilTopic, "game_logs", "game_logs.*", pubsub.Durable)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Successfully set up queue:\n %v, on channel\n: %v\n", logChannel, logQueue)

	defer newConntection.Close()

	fmt.Println("Connection successful")
	gamelogic.PrintServerHelp()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println(">")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
			break
		}
		command := strings.TrimSpace(input)
		commandWords := strings.Split(command, " ")
		if commandWords[0] == "pause" {
			fmt.Println("Sending pause message.")
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
		} else if commandWords[0] == "resume" {
			fmt.Println("Sending resume message")
			exchange := routing.ExchangePerilDirect
			key := routing.PauseKey
			data := routing.PlayingState{
				IsPaused: false,
			}
			err = pubsub.PublishJSON(newChannel, exchange, key, data)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Message sent.")
		} else if commandWords[0] == "quit" {
			fmt.Println("Exiting")
			break
		} else {
			fmt.Println("Invalid command")
			continue
		}
	}
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
