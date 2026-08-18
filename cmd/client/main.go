package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	url := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer connection.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error with Welcome: %v", err)
	}

	state := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.Transient, handlerPause(state))
	if err != nil {
		log.Fatalf("Error subscribeing json: %v", err)
	}

	loop := true
	for loop == true {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		switch inputs[0] {
		case "spawn":
			err = state.CommandSpawn(inputs)
			if err != nil {
				log.Printf("Error spawning: %s", err)
				continue
			}

		case "move":
			move, err := state.CommandMove(inputs)
			if err != nil {
				log.Printf("Error move: %v", err)
				continue
			}
			log.Print("Successful move!")
			log.Printf("%v", move)

		case "status":
			state.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			log.Print("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			log.Printf("Error unknown command: %s", inputs[0])
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
