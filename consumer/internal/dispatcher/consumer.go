package dispatcher

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"

	"kafka-fanout-test/consumer/internal/mapper"
)

// Consumer represents a Sarama consumer group consumer
type DispatchConsumer struct {
	Ready      chan bool
	dispatcher *Dispatcher
}

func NewDispatchConsumer() DispatchConsumer {
	consumer := DispatchConsumer{
		Ready: make(chan bool),
	}

	return consumer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *DispatchConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.Ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *DispatchConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *DispatchConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// Consume the message and do work
		logrus.Infof("message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		messageMapper, err := mapper.NewMessageMapper(mapper.MessageMapperConfig{})

		err = messageMapper.MapMessages(message)

		if err != nil {
			logrus.Fatalf("Error mapping message with offset %v for topic %s. ERROR: %v", message.Offset, message.Topic, err)
		}

		dispatcher := NewDispatcher()
		err = dispatcher.DispatchMessages(messageMapper.MappedMessages)

		if err != nil {
			logrus.Fatalf("Error processing message with offset %v for topic %s. ERROR: %v", message.Offset, message.Topic, err)
		}

		// commit offset
		session.MarkMessage(message, "")
	}

	return nil
}
