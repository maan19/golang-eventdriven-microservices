package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"client.id":         "something",
			"acks":              "all"})

		if err != nil {
			fmt.Printf("Failed to create producer: %s\n", err)
			os.Exit(1)
		}

		delivery_chan := make(chan kafka.Event, 10000)
		topic := "topic1"

		for {
			value := rand.Intn(100)
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(strconv.Itoa(value))},
				delivery_chan,
			)
			if err != nil {
				fmt.Printf("Failed to produce message: %s\n", err)
				os.Exit(1)
			}

			e := <-delivery_chan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}
		}
		//close(delivery_chan)

	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "foo",
			"auto.offset.reset": "smallest"})
		if err != nil {
			panic(err)
		}

		topics := []string{"topic1"}
		err = consumer.SubscribeTopics(topics, nil)
		if err != nil {
			panic(err)
		}

		run := true
		for run {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				// application-specific processing
				fmt.Printf("consumed from queue %s \n", string(e.Value))
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", ev)
				run = false
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}

		consumer.Close()
	}()

	wg.Wait()

}
