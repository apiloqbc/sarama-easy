package kafka

import (
	"context"
	"encoding/binary"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
)

// alias these to abstract the Sarama-specific message type from end users
type ConsumerMessage sarama.ConsumerMessage

type Consumer interface {
	// caller should run the returned function in a goroutine, and consume
	// the returned error channel until it's closed at shutdown.
	Background() (func(), chan error)
}

type kafkaConsumer struct {
	ctx  context.Context
	conf Config

	consumer             sarama.ConsumerGroup
	SchemaRegistryClient *CachedSchemaRegistryClient
	handler              Handler
	errors               chan error

	logger *log.Logger
}

type Message struct {
	SchemaId            int
	Topic               string
	Partition           int32
	Offset              int64
	Key                 string
	Value               string
	HighWaterMarkOffset int64
}

// caller should cancel the supplied context when a graceful consumer shutdown is desired
func NewConsumer(ctx context.Context, conf Config, handler Handler, logger *log.Logger) (Consumer, error) {
	// set the internal Sarama client logging
	if conf.Verbose {
		sarama.Logger = logger
	}

	saramaConf, err := configureConsumer(conf)
	if err != nil {
		return nil, err
	}

	// config should have a CSV list of brokers
	brokers := strings.Split(conf.Brokers, ",")

	// create consumer group w/underlying managed client.
	// docs recommend 1 client per producer or consumer for throughput
	consumer, err := sarama.NewConsumerGroup(brokers, conf.Group, saramaConf)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating consumer group message handler")
	}

	// config should have a CSV list of brokers
	schemaRegistryServers := strings.Split(conf.SchemaRegistryServers, ",")
	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)

	return &kafkaConsumer{
		ctx:                  ctx,
		conf:                 conf,
		consumer:             consumer,
		SchemaRegistryClient: schemaRegistryClient,
		handler:              handler,
		errors:               make(chan error, errorQueueSize),
		logger:               logger,
	}, nil
}

// caller should run the returned function in a goroutine, and consume
// the returned error channel until it's closed at shutdown.
func (kc *kafkaConsumer) Background() (func(), chan error) {
	// pass errors from Sarama to end user; closed by Sarama during shutdown
	go func() {
		for err := range kc.consumer.Errors() {
			kc.errors <- err
		}
	}()

	return func() {
		defer func() {
			if err := kc.consumer.Close(); err != nil {
				kc.errors <- err
			}
			// this releases the caller who should be consuming this channel
			close(kc.errors)
		}()

		// the main consume loop, parent of the ConsumerClaim() partition message loops.
		topics := strings.Split(kc.conf.Topics, ",")
		for kc.ctx.Err() == nil {
			kc.logger.Printf("Kafka consumer: begining parent Consume() loop for topic(s): %s", kc.conf.Topics)

			// if Consume() returns nil, a rebalance is in progress and it should be called again.
			// if Consume() returns an error, the consumer should break the loop, shutting down.
			if err := kc.consumer.Consume(kc.ctx, topics, kc); err != nil {
				kc.errors <- err
				return
			}
		}
	}, kc.errors
}

// Implements the sarama.ConsumerGroupHandler contract
func (kc *kafkaConsumer) Setup(session sarama.ConsumerGroupSession) error {
	kc.logger.Printf("Kafka client: consumer.Setup() called: session=%+v", session)
	return nil
}

// Implements the sarama.ConsumerGroupHandler contract
func (kc *kafkaConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	kc.logger.Printf("Kafka client: consumer.Cleanup() called: session=%+v", session)
	return nil
}

// Implements the sarama.ConsumerGroupHandler contract - Sarama runs this in a goroutine for you.
// Called once per partition assigned to this consumer group member.
func (kc *kafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	kc.logger.Printf("Kafka client: consuming partition %d of topic %s", claim.Partition(), claim.Topic())

	// consume each partitions' messages async and pass to supplied handler. Messages() closed for us on shutdown
	for msg := range claim.Messages() {
		saramaMsg := (*ConsumerMessage)(msg)
		message, _ := kc.ProcessAvroMsg(saramaMsg)

		message.HighWaterMarkOffset = claim.HighWaterMarkOffset()
		if err := kc.handler.Handle(message); err != nil {
			return err
		}

		// if the message handler didn't return an error, mark this message offset as consumed
		session.MarkMessage(msg, "")
	}

	return nil
}

func (ac *kafkaConsumer) ProcessAvroMsg(m *ConsumerMessage) (*Message, error) {
	if m.Value == nil {
		return &Message{
			SchemaId:  int(-1),
			Topic:     m.Topic,
			Partition: m.Partition,
			Offset:    m.Offset,
			Key:       string(m.Key),
			Value:     string("")}, nil
	}
	schemaId := binary.BigEndian.Uint32(m.Value[1:5])
	log.Printf("SchemaID: %d", schemaId)
	codec, err := ac.GetSchema(int(schemaId))
	if err != nil {
		log.Printf("Error: %s", err)
		return &Message{}, err
	}
	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(m.Value[5:])
	if err != nil {
		log.Printf("Error: %s", err)
		return &Message{}, err
	}

	// Convert native Go form to textual Avro data
	textual, err := codec.TextualFromNative(nil, native)

	if err != nil {
		log.Printf("Error: %s", err)
		return &Message{}, err
	}
	msg := Message{
		SchemaId:  int(schemaId),
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       string(m.Key),
		Value:     string(textual)}
	return &msg, nil
}

//GetSchemaId get schema id from schema-registry service
func (ac *kafkaConsumer) GetSchema(id int) (*goavro.Codec, error) {
	codec, err := ac.SchemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	return codec, nil
}
