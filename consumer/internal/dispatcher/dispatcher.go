package dispatcher

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var (
	bufferCap      = 10000
	tickerInterval = 1
)

type ConsumerSessionMessage struct {
	Session sarama.ConsumerGroupSession
	Message *sarama.ConsumerMessage
}

type msgBuf []*ConsumerSessionMessage

// Consumer represents a Sarama consumer group consumer
type DispatchConsumerHandler struct {
	sarama.ConsumerGroupHandler

	ready     chan bool
	mutex     sync.RWMutex
	msgBuffer msgBuf
	ticker    *time.Ticker // used flush the msg buffer on an interval
}

func NewDispatchConsumerHandler() DispatchConsumerHandler {
	return DispatchConsumerHandler{
		ready:     make(chan bool, 0),
		msgBuffer: make([]*ConsumerSessionMessage, 0, bufferCap),
		ticker:    time.NewTicker(time.Duration(tickerInterval) * time.Second),
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer DispatchConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer DispatchConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	logrus.Info("We're doing some cleanup")
	return nil
}

func (consumer *DispatchConsumerHandler) addMessageToBuffer(msg *ConsumerSessionMessage) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	consumer.msgBuffer = append(consumer.msgBuffer, msg)

	if len(consumer.msgBuffer) >= bufferCap {
		consumer.flush()
	}
}

func mapAndDispatchMessages(messages msgBuf) error {
	for _, message := range messages {
		message := message
		go func() {
			time.Sleep(3 * time.Second) // our "work" for now
			logrus.Infof("Message claimed: value = %s, timestamp = %v, topic = %s, offset = %v", string(message.Message.Value), message.Message.Timestamp, message.Message.Topic, message.Message.Offset)
			message.Session.MarkMessage(message.Message, "")
		}()
	}
	return nil
}

// flush performs the actual work of the consumer
func (consumer *DispatchConsumerHandler) flush() error {
	if len(consumer.msgBuffer) == 0 {
		return nil
	}
	err := mapAndDispatchMessages(consumer.msgBuffer)

	if err != nil {
		return err
	}

	consumer.msgBuffer = make([]*ConsumerSessionMessage, 0, bufferCap)

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer DispatchConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	claimMsgChan := claim.Messages()

	for {
		select {
		case message, ok := <-claimMsgChan:
			if ok {
				consumer.addMessageToBuffer(&ConsumerSessionMessage{
					Message: message,
					Session: session,
				})
			} else {
				return nil
			}
		case <-consumer.ticker.C:
			consumer.flush()
		}
	}

	return nil
}

func NewConsumerGroup(ctx context.Context, wg sync.WaitGroup, brokers, topics, group string, oldest bool, handler DispatchConsumerHandler) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	version, err := sarama.ParseKafkaVersion("2.1.1")
	if err != nil {
		logrus.Fatalf("Error parsing Kafka version: %v", err)
	}

	config.Version = version

	consumerGroup, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)

	if err != nil {
		logrus.Fatalf("Error creating consumer group: %v", err)
	}

	wg.Add(1)

	logrus.Info("starting consumer")

	go func() {
		defer wg.Done()

		for {
			err := consumerGroup.Consume(ctx, strings.Split(topics, ","), handler)

			if err != nil {
				logrus.Fatal("Unrecoverable error from consumer: %v", err)
			}

			if ctx.Err() != nil {
				return
			}

			handler.ready = make(chan bool)
		}
	}()

	// await for consumer setup
	<-handler.ready
	logrus.Info("Consumer started")

	return consumerGroup, nil
}
