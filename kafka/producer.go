package kafka

import (
	"context"
	"encoding/binary"
	"github.com/linkedin/goavro/v2"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// abstracts kafka.Producer message type
type ProducerMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

type Producer interface {
	// caller should run the returned function in a goroutine, and consume
	// the returned error channel until it's closed at shutdown.
	Background() (func(), chan error)

	// user-facing event emit API
	Send(ProducerMessage) error
}

// internal type implementing kafka.Producer contract
type kafkaProducer struct {
	ctx  context.Context
	conf Config

	producer             sarama.AsyncProducer
	SchemaRegistryClient *CachedSchemaRegistryClient
	errors               chan error
	logger               *log.Logger
}

// the caller can cancel the producer's context to initiate shutdown.
func NewProducer(ctx context.Context, conf Config, logger *log.Logger) (*kafkaProducer, error) {
	producer, err := createProducer(conf, logger)
	if err != nil {
		return nil, err
	}

	// config should have a CSV list of brokers
	schemaRegistryServers := strings.Split(conf.SchemaRegistryServers, ",")
	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)
	return &kafkaProducer{
		conf:                 conf,
		ctx:                  ctx,
		producer:             producer,
		SchemaRegistryClient: schemaRegistryClient,
		errors:               make(chan error, errorQueueSize),
		logger:               logger,
	}, nil
}

// user-facing event emit API
func (kp *kafkaProducer) Send(msg ProducerMessage) error {
	if len(msg.Topic) == 0 {
		return errors.New("message Topic is required")
	}

	if len(msg.Key) == 0 && len(msg.Value) == 0 {
		return errors.New("at least one of message fields Key or Value is required")
	}

	kmsg := &sarama.ProducerMessage{
		Topic: msg.Topic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
	}

	// if shutdown is triggered, drop the message
	select {
	case <-kp.ctx.Done():
		return errors.Wrapf(kp.ctx.Err(), "message lost: shutdown triggered during send")

	case kp.producer.Input() <- kmsg:
		// fall through, msg has been queued for write
	}

	return nil
}

// user-facing event emit API
func (kp *kafkaProducer) SendAvroMsg(msg *sarama.ProducerMessage) error {

	// if shutdown is triggered, drop the message
	select {
	case <-kp.ctx.Done():
		return errors.Wrapf(kp.ctx.Err(), "message lost: shutdown triggered during send")

	case kp.producer.Input() <- msg:
		// fall through, msg has been queued for write
	}

	return nil
}

// caller should run the returned function in a goroutine, and consume
// the returned error channel until it's closed at shutdown.
func (kp *kafkaProducer) Background() (func(), chan error) {
	// proxy all Sarama errors to the caller until Close() drains and closes it
	go func() {
		for err := range kp.producer.Errors() {
			kp.errors <- err
		}

		kp.logger.Printf("Kafka producer: shutting down error reporter")
	}()

	return func() {
		defer func() {
			kp.errors <- kp.producer.Close()
			close(kp.errors)
		}()

		<-kp.ctx.Done()
		kp.logger.Printf("Kafka producer: shutdown triggered")
	}, kp.errors
}

func createProducer(conf Config, logger *log.Logger) (sarama.AsyncProducer, error) {
	if logger != nil {
		sarama.Logger = logger
	}

	cfg, err := configureProducer(conf)
	if err != nil {
		return nil, err
	}

	brokers := strings.Split(conf.Brokers, ",")
	producer, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create Kafka producer")
	}

	return producer, nil
}

// AvroEncoder encodes schemaId and Avro message.
type AvroEncoder struct {
	SchemaID int
	Content  []byte
}

// Notice: the Confluent schema registry has special requirements for the Avro serialization rules,
// not only need to serialize the specific content, but also attach the Schema ID and Magic Byte.
// Ref: https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
func (a *AvroEncoder) Encode() ([]byte, error) {
	var binaryMsg []byte
	// Confluent serialization format version number; currently always 0.
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by Schema Registry
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(a.SchemaID))
	binaryMsg = append(binaryMsg, binarySchemaId...)
	// Avro serialized data in Avro's binary encoding
	binaryMsg = append(binaryMsg, a.Content...)
	return binaryMsg, nil
}

// Length of schemaId and Content.
func (a *AvroEncoder) Length() int {
	return 5 + len(a.Content)
}

//GetSchemaId get schema id from schema-registry service
func (ap *kafkaProducer) GetSchemaId(topic string, avroCodec *goavro.Codec) (int, error) {
	schemaId, err := ap.SchemaRegistryClient.CreateSubject(topic+"-value", avroCodec)
	if err != nil {
		return 0, err
	}
	return schemaId, nil
}
