package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
)

func Consume(ctx context.Context) {
	config := sarama.NewConfig()
	config.ClientID = "go-kafka-messages"
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}
	config.Consumer.Offsets.AutoCommit.Enable = true

	// Create new messages
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	topics, _ := consumer.Topics()
	partitions, _ := consumer.Partitions("quang")
	log.Println(partitions)

	messages, errors := consume(topics, consumer)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case msg := <-messages:
				msgCount++
				fmt.Println("Received messages", string(msg.Key), string(msg.Value))
			case consumerError := <-errors:
				msgCount++
				fmt.Println("Received consumerError ", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
				doneCh <- struct{}{}
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

}

func consume(topics []string, consumer sarama.Consumer) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	messages := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)
	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		partitions, _ := consumer.Partitions(topic)
		// this only consumes partition no 1, you would probably want to consume all partitions
		consumerPartition, err := consumer.ConsumePartition(topic, partitions[0], sarama.OffsetOldest)
		if nil != err {
			fmt.Printf("Topic %v Partitions: %v", topic, partitions)
			panic(err)
		}
		fmt.Println(" Start consuming topic ", topic)
		go func(topic string, consumer sarama.PartitionConsumer) {
			for {
				select {
				case consumerError := <-consumer.Errors():
					errors <- consumerError
					fmt.Println("consumerError: ", consumerError.Err)

				case msg := <-consumer.Messages():
					messages <- msg
					fmt.Println("Got message on topic ", topic, string(msg.Value), msg.Partition, msg.Offset)
					consumer.HighWaterMarkOffset()
				}
			}
		}(topic, consumerPartition)
	}

	return messages, errors
}

func main() {
	Consume(context.Background())
}
