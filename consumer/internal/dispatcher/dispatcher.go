package dispatcher

import (
	"sync"
	"time"

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

func ReturnsErr() error {
	return nil
}

type Dispatcher struct {
	errors         chan error
	done           chan bool
	message        *sarama.ConsumerMessage
	wg             sync.WaitGroup
	mappedMessages []*mapper.MappedMessage
}

func NewDispatcher() Dispatcher {
	dispatcher := Dispatcher{
		errors: make(chan error),
		done:   make(chan bool),
		wg:     sync.WaitGroup{},
	}

	return dispatcher
}

func (d *Dispatcher) dispatch(message *mapper.MappedMessage) error {
	defer d.wg.Done()

	time.Sleep(time.Millisecond * 100) // fake some io
	logrus.Infof("dispatching message with id %s for party: %s", message.EventId, message.Party)
	err := ReturnsErr() // replace with actual kafka producer call

	if err != nil {
		d.errors <- err
	}

	return nil
}

func (d *Dispatcher) DispatchMessages(messages mapper.MappedMessages) error {
	d.errors = make(chan error)
	d.done = make(chan bool)
	d.wg = sync.WaitGroup{}
	d.wg.Add(len(messages))

	for _, message := range messages {
		message := message
		go d.dispatch(message)
	}

	go func() {
		d.wg.Wait()
		close(d.done)
	}()

	// await either done or an error
	select {
	case <-d.done:
		break
	case err := <-d.errors:
		// maybe pass context with cancel to dispatch() and cancel here?
		close(d.errors)
		return err
	}

	return nil
}
