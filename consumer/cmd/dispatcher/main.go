package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"

	"kafka-fanout-test/consumer/internal/dispatcher"
)

const assignor = "range"

var (
	brokers = ""
	group   = ""
	topics  = ""
	oldest  = true
)

func init() {
	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&group, "group", "", "Kafka consumer group definition")
	flag.StringVar(&topics, "topics", "", "Kafka topics to be consumed, as a comma separated list")
	flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")

	flag.Parse()

	if len(brokers) == 0 {
		logrus.Fatal("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topics) == 0 {
		logrus.Fatal("no topics given to be consumed, please set the -topics flag")
	}

	if len(group) == 0 {
		logrus.Fatal("no Kafka consumer group defined, please set the -group flag")
	}
}

func main() {
	logrus.Info("starting consumer")

	config := sarama.NewConfig()

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	version, err := sarama.ParseKafkaVersion("2.1.1")
	if err != nil {
		logrus.Fatalf("Error parsing Kafka version: %v", err)
	}

	config.Version = version

	ctx, cancel := context.WithCancel(context.Background())
	consumerGroup, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)

	if err != nil {
		logrus.Fatalf("Error creating consumer group: %v", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	consumer := dispatcher.NewDispatchConsumer()

	go func() {
		defer wg.Done()

		for {
			err := consumerGroup.Consume(ctx, strings.Split(topics, ","), &consumer)

			if err != nil {
				logrus.Fatal("Unrecoverable error from consumer: %v", err)
			}

			if ctx.Err() != nil {
				return
			}

			consumer.Ready = make(chan bool)
		}
	}()

	<-consumer.Ready
	logrus.Info("Consumer started")

	// trap sigint
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// block and wait for us to cancel
	select {
	case <-ctx.Done():
		logrus.Info("terminating: context cancelled")
	case <-sigterm:
		logrus.Info("terminating: via signal")
	}

	cancel()
	wg.Wait()

	if err = consumerGroup.Close(); err != nil {
		logrus.Fatalf("Error closing client: %v", err)
	}
}
