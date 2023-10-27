package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/emmanuelviniciusdev/imersao-fullcycle-apache-kafka/app"
)

// Handy commands:
// kafka-topics --create --bootstrap-server=localhost:9092 --topic=test --partitions=3
// kafka-console-consumer --bootstrap-server=localhost:9092 --topic=test

func main() {
	producer, err := app.NewKakfaProducer()

	if err != nil {
		panic(err)
	}

	deliveryChan := make(chan kafka.Event)

	err = app.Publish("Hello, Kafka c:", []byte("baz"), "test2", producer, deliveryChan)

	if err != nil {
		panic(err)
	}

	go deliveryReport(deliveryChan)

	producer.Flush(3000)
}

func deliveryReport(deliveryChan chan kafka.Event) {
	for response := range deliveryChan {
		switch event := response.(type) {
		case *kafka.Message:
			if event.TopicPartition.Error != nil {
				fmt.Println("Error sending event")
			} else {
				fmt.Println("Successfully sent event:", event.TopicPartition)
			}
		}
	}
}
