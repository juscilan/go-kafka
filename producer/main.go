package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	// Define the Kafka broker addresses and topic name
	brokerList := []string{"localhost:9092"} // Change this if your Kafka broker is running on a different address
	topic := "your-topic"                    // Change this to the desired Kafka topic

	// Configure the producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// Create a new sync producer
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalf("Error creating producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Error closing producer: %v", err)
		}
	}()

	// Create a message to send
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Hello, Kafka!"),
	}

	// Send the message to Kafka
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
