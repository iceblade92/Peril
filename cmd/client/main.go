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

	_, _, err = pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.Transient) // remember to add channel and queue later
	if err != nil {
		log.Fatalf("Error creating channel and queue: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
