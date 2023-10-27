package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/emmanuelviniciusdev/imersao-fullcycle-apache-kafka/app"
	"sync"
	"time"
)

// Handy commands:
// kafka-topics --create --bootstrap-server=localhost:9092 --topic=test2 --partitions=5
// kafka-console-consumer --bootstrap-server=localhost:9092 --topic=test2

func main() {
	producer, err := app.NewKakfaProducer()

	if err != nil {
		panic(err)
	}

	deliveryChan := make(chan kafka.Event)

	go publishInfiniteMessages(producer, deliveryChan)

	go deliveryReport(deliveryChan)

	go app.NewKafkaConsumer([]string{"test2"}, "goapp-consumer", "goapp-consumer-group")
	go app.NewKafkaConsumer([]string{"test2"}, "goapp-consumer", "goapp-consumer-group")
	go app.NewKafkaConsumer([]string{"test2"}, "goapp-consumer", "goapp-consumer-group")
	go app.NewKafkaConsumer([]string{"test2"}, "goapp-consumer", "goapp-consumer-group")
	go app.NewKafkaConsumer([]string{"test2"}, "goapp-consumer", "goapp-consumer-group")

	wg := &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func publishInfiniteMessages(producer *kafka.Producer, deliveryChan chan kafka.Event) {
	for {
		err := app.Publish("Hello, Kafka c:", nil, "test2", producer, deliveryChan)

		if err != nil {
			panic(err)
		}

		time.Sleep(time.Second * 5)
	}
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
