package app

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewKafkaConsumer(topics []string, clientID string, groupID string) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         clientID,
		"group.id":          groupID,
		//"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(configMap)

	if err != nil {
		panic(err)
	}

	err = consumer.SubscribeTopics(topics, nil)

	if err != nil {
		panic(err)
	}

	for {
		message, err := consumer.ReadMessage(-1)

		if err == nil {
			fmt.Println(string(message.Value), message.TopicPartition)
		}
	}
}
