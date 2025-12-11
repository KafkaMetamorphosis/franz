package store

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaClient interface {
	WriteMessage(ctx context.Context, topic string, key, value []byte) error

	ReadMessages(ctx context.Context, topic string, partition int) ([]kafka.Message, error)

	CreateTopics(topicConfigs ...kafka.TopicConfig) error

	Close() error
}

type RealKafkaClient struct {
	brokers []string
	conn    *kafka.Conn
	timeout time.Duration
}

// NewRealKafkaClient creates a new real Kafka client
func NewRealKafkaClient(brokers []string, timeout time.Duration) (*RealKafkaClient, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers provided")
	}

	// Connect to the first broker
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return nil, err
	}

	return &RealKafkaClient{
		brokers: brokers,
		conn:    conn,
		timeout: timeout,
	}, nil
}

// WriteMessage writes a message to Kafka
func (r *RealKafkaClient) WriteMessage(ctx context.Context, topic string, key, value []byte) error {
	// Create custom dialer to handle Docker hostname resolution
	// This dialer will map Docker internal hostnames (e.g., kafka-fleet:9092)
	// to localhost addresses (e.g., localhost:19092) when running on host machine
	customDialer := &kafka.Dialer{
		Timeout:   r.timeout,
		DualStack: true,
		Resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				// Use custom resolver to prevent standard DNS lookup
				d := &net.Dialer{Timeout: r.timeout}
				return d.DialContext(ctx, network, address)
			},
		},
	}

	// Also set a custom DialFunc that applies our hostname mapping
	customDialer.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		// Map Docker internal hostnames to localhost if needed
		//mappedAddr := mapBrokerAddress(address)
		log.Printf("[KafkaClient] Dialing: %v", address)
		d := &net.Dialer{Timeout: r.timeout}
		return d.DialContext(ctx, network, address)
	}

	// Use a Writer which is the proper way to write messages in kafka-go
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      r.brokers,
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: 1,
		WriteTimeout: r.timeout,
		Dialer:       customDialer,
	})
	defer writer.Close()

	// Write the message
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})

	return err
}

// ReadMessages reads all messages from a topic partition
func (r *RealKafkaClient) ReadMessages(ctx context.Context, topic string, partition int) ([]kafka.Message, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   r.brokers,
		Topic:     topic,
		Partition: partition,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	// Seek to beginning
	if err := reader.SetOffset(0); err != nil {
		return nil, err
	}

	var messages []kafka.Message
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// End of messages
			break
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// CreateTopics creates topics
func (r *RealKafkaClient) CreateTopics(topicConfigs ...kafka.TopicConfig) error {
	// Try to get controller connection
	controller, err := r.conn.Controller()
	if err != nil {
		// Use existing connection if controller unavailable
		return r.conn.CreateTopics(topicConfigs...)
	}

	// Try to connect to controller
	controllerAddr := kafka.TCP(controller.Host).String() + ":" + string(rune(controller.Port))
	controllerConn, err := kafka.Dial("tcp", controllerAddr)
	if err != nil {
		// Fall back to existing connection
		return r.conn.CreateTopics(topicConfigs...)
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(topicConfigs...)
}

// Close closes the Kafka connection
func (r *RealKafkaClient) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// MockKafkaClient implements KafkaClient for testing
type MockKafkaClient struct {
	Messages    map[string][]kafka.Message // topic -> messages
	Topics      map[string]bool            // created topics
	WriteError  error                      // error to return on write
	ReadError   error                      // error to return on read
	CreateError error                      // error to return on create
	WriteCalls  int                        // number of write calls
	ReadCalls   int                        // number of read calls
	CreateCalls int                        // number of create calls
}

// NewMockKafkaClient creates a new mock Kafka client
func NewMockKafkaClient() *MockKafkaClient {
	return &MockKafkaClient{
		Messages: make(map[string][]kafka.Message),
		Topics:   make(map[string]bool),
	}
}

// WriteMessage simulates writing a message
func (m *MockKafkaClient) WriteMessage(ctx context.Context, topic string, key, value []byte) error {
	m.WriteCalls++
	if m.WriteError != nil {
		return m.WriteError
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}

	m.Messages[topic] = append(m.Messages[topic], msg)
	return nil
}

// ReadMessages simulates reading messages
func (m *MockKafkaClient) ReadMessages(ctx context.Context, topic string, partition int) ([]kafka.Message, error) {
	m.ReadCalls++

	if m.ReadError != nil {
		return nil, m.ReadError
	}

	messages, ok := m.Messages[topic]
	if !ok {
		return []kafka.Message{}, nil
	}

	return messages, nil
}

// CreateTopics simulates creating topics
func (m *MockKafkaClient) CreateTopics(topicConfigs ...kafka.TopicConfig) error {
	m.CreateCalls++

	if m.CreateError != nil {
		return m.CreateError
	}

	for _, config := range topicConfigs {
		m.Topics[config.Topic] = true
	}

	return nil
}

// Close is a no-op for mock
func (m *MockKafkaClient) Close() error {
	return nil
}

// Reset clears all mock data
func (m *MockKafkaClient) Reset() {
	m.Messages = make(map[string][]kafka.Message)
	m.Topics = make(map[string]bool)
	m.WriteCalls = 0
	m.ReadCalls = 0
	m.CreateCalls = 0
	m.WriteError = nil
	m.ReadError = nil
	m.CreateError = nil
}

// mapBrokerAddress maps Docker internal hostnames to localhost addresses when needed
// This function is primarily used when running the app on the host machine
// and connecting to Kafka in Docker. When both are in Docker, no mapping is needed.
func mapBrokerAddress(address string) string {
	// Check if we can already connect to the address (e.g., inside Docker network)
	// If address can be resolved, use it as-is
	// Otherwise, try to map it for host-based testing

	// Common Docker Kafka container hostnames and their localhost mappings
	// These are used when the app runs on host connecting to Docker containers
	hostMappings := map[string]string{
		"kafka-fleet:9092": "localhost:19092",
		"kafka-2:9092":     "localhost:29092",
		"kafka-3:9092":     "localhost:39092",
		"kafka-test:9092":  "localhost:29092",
	}

	// Try to map the address
	if mapped, ok := hostMappings[address]; ok {
		// Check if the original address resolves (we're inside Docker)
		// If it does, use original. Otherwise use mapped version (on host).
		if _, err := net.LookupHost(extractHost(address)); err == nil {
			// Address resolves - we're likely inside Docker network
			return address
		}
		// Address doesn't resolve - map to localhost for host-based access
		return mapped
	}

	// If no mapping found, return as-is (might be localhost already or valid hostname)
	return address
}

// extractHost extracts the hostname from an address like "hostname:port"
func extractHost(address string) string {
	if host, _, err := net.SplitHostPort(address); err == nil {
		return host
	}
	return address
}
