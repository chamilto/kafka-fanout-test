package dispatcher

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"

	"kafka-fanout-test/consumer/internal/mapper"
)

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
