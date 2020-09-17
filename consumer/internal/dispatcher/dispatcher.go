package dispatcher

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// Consumer represents a Sarama consumer group consumer
type DispatchConsumer struct {
	Ready chan bool
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

type eventMapperConfig struct{}

type eventMapper struct {
	config  eventMapperConfig
	errors  chan error
	done    chan bool
	message *sarama.ConsumerMessage
	wg      sync.WaitGroup
}

func (e *eventMapper) mapMessages() ([][]byte, error) {
	return [][]byte{}, nil
}

func ReturnsErr() error {
	return nil
}

func (e *eventMapper) dispatch(message interface{}) error {
	defer e.wg.Done()

	time.Sleep(2)
	logrus.Infof("dispatching a message!: %v", message)
	err := ReturnsErr() // replace with actual producer call

	if err != nil {
		e.errors <- err
	}

	return nil
}

func (e *eventMapper) dispatchMessages() error {

	logrus.Infof("message claimed: value = %s, timestamp = %v, topic = %s", string(e.message.Value), e.message.Timestamp, e.message.Topic)
	// messages, _ := e.mapMessages()
	messages := []string{"a", "b", "c"}
	e.wg.Add(len(messages))

	for message := range messages {
		message := message
		go e.dispatch(message)
	}

	go func() {
		e.wg.Wait()
		close(e.done)
	}()

	// await either done or an error
	select {
	case <-e.done:
		break
	case err := <-e.errors:
		close(e.errors)
		return err
	}

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *DispatchConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// Consume the message and do work
		mapper := eventMapper{
			config:  eventMapperConfig{},
			errors:  make(chan error),
			done:    make(chan bool),
			message: message,
			wg:      sync.WaitGroup{},
		}
		err := mapper.dispatchMessages()

		if err != nil {
			logrus.Fatalf("Error processing message with offset %v for topic %s", message.Offset, message.Topic)
		}

		// commit offset
		session.MarkMessage(message, "")
	}

	return nil
}
