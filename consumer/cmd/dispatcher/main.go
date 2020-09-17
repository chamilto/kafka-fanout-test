package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

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
	handler := dispatcher.NewDispatchConsumerHandler()

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	consumerGroup, err := dispatcher.NewConsumerGroup(ctx, wg, brokers, topics, group, oldest, handler)

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

	// Close calls our consumer's Cleanup method
	if err = consumerGroup.Close(); err != nil {
		logrus.Fatalf("Error closing client: %v", err)
	}
}
