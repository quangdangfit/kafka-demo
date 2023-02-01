package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
)

var (
	enqueued int
)

func Produce(ctx context.Context) {
	brokers := []string{"localhost:9092"}

	producer, err := setupProducer(brokers)
	if err != nil {
		panic(err)
	} else {
		log.Println("Kafka AsyncProducer up and running!")
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	produceMessages(producer, signals)

	log.Printf("Kafka AsyncProducer finished with %d messages produced.", enqueued)
}

// setupProducer will create a AsyncProducer and returns it
func setupProducer(brokers []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	return sarama.NewAsyncProducer(brokers, config)
}

// produceMessages will send 'testing 123' to KafkaTopic each second, until receive a os signal to stop e.g. control + c
// by the user in terminal
func produceMessages(producer sarama.AsyncProducer, signals chan os.Signal) {
	for {
		time.Sleep(5 * time.Second)
		message := &sarama.ProducerMessage{Topic: "quang", Value: sarama.StringEncoder("testing 123"), Partition: int32(enqueued % 5)}
		select {
		case producer.Input() <- message:
			enqueued++
			log.Printf("New Message produced topic = %s, partition = %d\n", message.Topic, message.Partition)
		case <-signals:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			return
		}
	}
}

func main() {
	Produce(context.Background())
}
