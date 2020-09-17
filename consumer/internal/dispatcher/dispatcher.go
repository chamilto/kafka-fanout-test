package dispatcher

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// Consumer represents a Sarama consumer group consumer
type DispatchConsumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *DispatchConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
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
		logrus.Infof("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		go func() {
			time.Sleep(time.Second * 3)
			logrus.Info("done sleeping")
		}()

		// commit offset
		session.MarkMessage(message, "")
	}

	return nil
}
