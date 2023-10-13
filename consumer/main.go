package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
)

func main() {
	// Define Kafka broker addresses and the topic to consume
	brokerList := []string{"localhost:9092"} // Change this if your Kafka broker is running on a different address
	topic := "your-topic"                    // Change this to the Kafka topic you want to consume from
	groupID := "my-consumer-group"           // Specify your consumer group

	// Create a new Kafka consumer
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalf("Error closing Kafka consumer: %v", err)
		}
	}()

	// Create a Kafka consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokerList, groupID, config)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer group: %v", err)
	}
	defer func() {
		if err := consumerGroup.Close(); err != nil {
			log.Fatalf("Error closing Kafka consumer group: %v", err)
		}
	}()

	// Create a channel to receive messages
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Consume messages from the topic
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-signals:
				return
			default:
				handler := MyMessageHandler{} // Replace with your custom message handler
				err := consumerGroup.Consume(handler, []string{topic})
				if err != nil {
					log.Printf("Error consuming from Kafka: %v", err)
				}
			}
		}
	}()

	wg.Wait()
}

// MyMessageHandler is a custom message handler that processes Kafka messages.
type MyMessageHandler struct{}

func (h MyMessageHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h MyMessageHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h MyMessageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Printf("Message received: Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
			message.Topic, message.Partition, message.Offset, message.Key, message.Value)
		session.MarkMessage(message, "")
	}

	return nil
}
