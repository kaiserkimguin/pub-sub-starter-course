package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"
	"strings"

	gamelogic "github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	pubsub "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"

	newConnection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
	}

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	gameState := gamelogic.NewGameState(userName)
	queueName := routing.PauseKey + "." + userName
	err = pubsub.SubscribeJSON(newConnection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queueName)

	queueNameMoves := routing.ArmyMovesPrefix + "." + userName
	moveKey := routing.ArmyMovesPrefix + ".*"
	moveExchange := routing.ExchangePerilTopic
	moveChannel, err := newConnection.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer moveChannel.Close()
	fmt.Println("queue:", queueNameMoves)
	fmt.Println("binding key:", moveKey)
	err = pubsub.SubscribeJSON(newConnection, moveExchange, queueNameMoves, moveKey, pubsub.Transient, handlerMove(gameState, moveChannel))
	if err != nil {
		log.Fatal(err)
	}

	queueNameWar := "war"
	warKey := routing.WarRecognitionsPrefix + "." + userName
	warExchange := moveExchange
	err = pubsub.SubscribeJSON(newConnection, warExchange, queueNameWar, warKey, pubsub.Durable, handlerConsumeWarMsg(gameState))
	fmt.Println("Move queue declared and bound")

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
		unitTypes := []string{"infantry", "cavalry", "artillery"}
		locationTypes := []string{"americas", "europe", "africa", "asia", "antarctica", "australia"}
		publishCh, err := newConnection.Channel()
		if err != nil {
			log.Fatal(err)
		}
		defer publishCh.Close()

		if commandWords[0] == "spawn" && slices.Contains(unitTypes, commandWords[2]) && slices.Contains(locationTypes, commandWords[1]) {

			err = gameState.CommandSpawn(commandWords)
			if err != nil {
				fmt.Println(err)
				continue
			}
		} else if commandWords[0] == "move" && slices.Contains(locationTypes, commandWords[1]) {
			fmt.Println("Trying to move unit")
			am, err := gameState.CommandMove(commandWords)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(publishCh, moveExchange, moveKey, am)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Move published successfully.")
			fmt.Println("Move successfull.")
		} else if commandWords[0] == "status" {
			gameState.CommandStatus()
		} else if commandWords[0] == "help" {
			gamelogic.PrintClientHelp()
		} else if commandWords[0] == "spam" {
			fmt.Println("Spamming is not allowed yet!")
		} else if commandWords[0] == "quit" {
			gamelogic.PrintQuit()
			break
		} else {
			fmt.Println("Invalid command")
			continue
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Programm is shutting down. Connection is closed.")
}
