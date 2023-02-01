package main

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	// Set broker configuration
	broker := sarama.NewBroker("localhost:9092")

	// Additional configurations. Check sarama doc for more info
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0

	// Open broker connection with configs defined above
	broker.Open(config)

	// check if the connection was OK
	connected, err := broker.Connected()
	if err != nil {
		log.Print(err.Error())
	}
	log.Print(connected)

	// Setup the Topic details in CreateTopicRequest struct
	topic := "quang"
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(5)
	topicDetail.ReplicationFactor = int16(1)
	topicDetail.ConfigEntries = make(map[string]*string)

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[topic] = topicDetail

	request := sarama.CreateTopicsRequest{
		Timeout:      time.Second * 15,
		TopicDetails: topicDetails,
	}

	// Send request to Broker
	response, err := broker.CreateTopics(&request)

	// handle errors if any
	if err != nil {
		log.Printf("%#v", &err)
	}
	t := response.TopicErrors
	for key, val := range t {
		log.Printf("Key is %s", key)
		log.Printf("Value is %#v", val.Err.Error())
		log.Printf("Value3 is %#v", val.ErrMsg)
	}
	log.Printf("the response is %#v", response)

	// close connection to broker
	broker.Close()
}
