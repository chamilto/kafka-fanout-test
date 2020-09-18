package mapper

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

const (
	EVENT_TYPE_ONE = "on_event1"
	EVENT_TYPE_TWO = "on_event2"
)

// This would be pulled from a config file
var eventTypePartyMapping = map[string][]string{
	EVENT_TYPE_ONE: []string{"party1", "party2"},
	EVENT_TYPE_TWO: []string{"party3", "party4"},
}

type MessageMapperConfig struct{}
type message struct {
	EventType string                 `json:"eventType"`
	Content   map[string]interface{} `json:"content"`
	EventId   string                 `json:"eventId"`
}
type MappedMessage struct {
	message
	Party string
}

type MappedMessages []*MappedMessage

type messageMapper struct {
	config         MessageMapperConfig
	errors         chan error
	done           chan bool
	kafkaMessage   *sarama.ConsumerMessage
	wg             sync.WaitGroup
	MappedMessages MappedMessages
}

func NewMessageMapper(config MessageMapperConfig) (messageMapper, error) {
	mapper := messageMapper{
		config: MessageMapperConfig{},
		errors: make(chan error),
		done:   make(chan bool),
		wg:     sync.WaitGroup{},
	}

	return mapper, nil
}

func (m *messageMapper) MapMessages(kafkaMessage *sarama.ConsumerMessage) error {
	m.kafkaMessage = kafkaMessage
	msg := message{}

	err := json.Unmarshal(kafkaMessage.Value, &msg)
	if err != nil {
		return err
	}

	if parties, ok := eventTypePartyMapping[msg.EventType]; ok {
		mappedMessages := MappedMessages{}
		for _, party := range parties {
			mappedMessages = append(mappedMessages, &MappedMessage{
				message: message{
					EventType: msg.EventType,
					Content:   msg.Content,
					EventId:   msg.EventId,
				},
				Party: party,
			})

		}

		m.MappedMessages = mappedMessages
	} else {
		return fmt.Errorf("Unrecognized event type")
	}
	// use config to generate a message for each listener

	return nil
}

func ReturnsErr() error {
	return nil
}
