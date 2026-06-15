package main

import (
	"fmt"
	"log"
	"time"

	gamelogic "github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	pubsub "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	routing "github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(ps routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(am gamelogic.ArmyMove) pubsub.Acktype {
	return func(am gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(am)
		switch moveOutcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			key := routing.WarRecognitionsPrefix + "." + gs.GetUsername()
			data := gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, key, data)
			if err != nil {
				log.Fatal(err)
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerConsumeWarMsg(gs *gamelogic.GameState, ch *amqp.Channel) func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		wo, wi, lo := gs.HandleWar(rw)
		fmt.Println(wo)
		switch wo {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.Ack
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			err := publishGameLog(ch, gs, wo, wi, lo)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			err := publishGameLog(ch, gs, wo, wi, lo)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := publishGameLog(ch, gs, wo, wi, lo)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("unexpected war outcome. message should be discarded.")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp.Channel, gs *gamelogic.GameState, wo gamelogic.WarOutcome, wi, lo string) error {
	var msg string
	switch wo {
	case gamelogic.WarOutcomeOpponentWon:
		msg = fmt.Sprintf("%s won a war against %s", wi, lo)
	case gamelogic.WarOutcomeYouWon:
		msg = fmt.Sprintf("%s won a war against %s", wi, lo)
	case gamelogic.WarOutcomeDraw:
		msg = fmt.Sprintf("A war between %s and %s resulted in a draw", wi, lo)
	default:
		msg = " "
	}

	key := routing.GameLogSlug + "." + gs.GetUsername()
	data := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    gs.GetUsername(),
	}

	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, key, data)
	if err != nil {
		return err
	}
	return nil
}
