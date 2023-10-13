package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {
	// Set up configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Specify the Kafka broker address
	brokers := []string{"localhost:9091"}

	// Create a new consumer
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	// Specify the topic you want to consume messages from
	topic := "moreto"

	// Create a new partition consumer
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			panic(err)
		}
	}()

	// Create a signal channel to handle graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	// Start consuming messages
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message: %s\n", string(msg.Value))
		case err := <-partitionConsumer.Errors():
			fmt.Printf("Error: %v\n", err.Err)
		case <-signalChan:
			// Handle shutdown (e.g., close connections and clean up)
			fmt.Println("Shutting down...")
			return
		}
	}
}
