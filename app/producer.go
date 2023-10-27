package app

import "github.com/confluentinc/confluent-kafka-go/kafka"

func Publish(value string, key []byte, topic string, producer *kafka.Producer, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(value),
		Key:            key,
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	}

	err := producer.Produce(message, deliveryChan)

	return err
}

func NewKakfaProducer() (*kafka.Producer, error) {
	// If something goes wrong during connection:
	// https://stackoverflow.com/questions/43103167/failed-to-resolve-kafka9092-name-or-service-not-known-docker-php-rdkafka
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := kafka.NewProducer(configMap)

	if err != nil {
		return nil, err
	}

	return producer, nil
}
