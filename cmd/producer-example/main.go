package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apiloqbc/sarama-easy/kafka"
	"github.com/kelseyhightower/envconfig"
	"log"
	"os"
	"sync"
)

var (
	conf  kafka.Config
	topic string
	count int
)

func init() {
	// Initialize the Kafka configuration
	conf = kafka.NewKafkaConfig()

	// Process environment variables to populate the config struct
	err := envconfig.Process("", &conf)
	if err != nil {
		log.Fatal("Error reading environment variables: ", err.Error())
	}

	// Override configuration values with command-line flags if provided
	flag.StringVar(&conf.Brokers, "brokers", conf.Brokers, "CSV list of Kafka seed brokers")
	flag.StringVar(&conf.Topics, "topics", conf.Topics, "CSV list of Kafka topics to consume")
	flag.StringVar(&conf.Username, "username", conf.Username, "Kafka Username")
	flag.StringVar(&conf.Password, "password", conf.Password, "Kafka Password")
	flag.StringVar(&conf.ClientID, "clientid", conf.ClientID, "Kafka Client ID")
	flag.StringVar(&conf.Version, "version", conf.Version, "Kafka Version")
	flag.BoolVar(&conf.Verbose, "verbose", conf.Verbose, "Log detailed Kafka client internals?")
	flag.BoolVar(&conf.TLSEnabled, "tlsenabled", conf.TLSEnabled, "TLS enabled")
	flag.StringVar(&conf.TLSKey, "tlskey", conf.TLSKey, "TLS key file")
	flag.StringVar(&conf.TLSCert, "tlscert", conf.TLSCert, "TLS cert file")
	flag.StringVar(&conf.CACerts, "cacerts", conf.CACerts, "CA certificates")

	// Consumer specific flags
	flag.StringVar(&conf.Group, "group", conf.Group, "Consumer group ID")
	flag.StringVar(&conf.RebalanceStrategy, "rebalancestrategy", conf.RebalanceStrategy, "Consumer rebalance strategy")
	flag.DurationVar(&conf.RebalanceTimeout, "rebalancetimeout", conf.RebalanceTimeout, "Consumer rebalance timeout")
	flag.StringVar(&conf.InitOffsets, "initoffsets", conf.InitOffsets, "Consumer initial offsets")
	flag.DurationVar(&conf.CommitInterval, "commitinterval", conf.CommitInterval, "Consumer commit interval")

	// Producer specific flags
	flag.DurationVar(&conf.FlushInterval, "flushinterval", conf.FlushInterval, "Producer flush interval")

	// Schema Registry flags
	flag.StringVar(&conf.SchemaRegistryServers, "schemaregistryservers", conf.SchemaRegistryServers, "Schema Registry Servers")

	// Security and SASL flags
	flag.StringVar(&conf.SaslMechanism, "saslmechanism", conf.SaslMechanism, "SASL Mechanism")
	flag.BoolVar(&conf.SaslEnabled, "saslEnabled", conf.SaslEnabled, "SASL enabled")

	flag.IntVar(&count, "count", 10, "Number of example messages to produce")

	// Parse the command-line flags
	flag.Parse()

	// Log the final configuration for verification
	log.Printf("Config: %+v\n", conf)
}

func main() {
	flag.Parse()

	logger := log.New(os.Stdout, "[example Kafka producer] ", log.LstdFlags|log.LUTC|log.Lshortfile)

	ctx, cancelable := context.WithCancel(context.Background())

	producer, err := kafka.NewProducer(ctx, conf, logger)
	if err != nil {
		logger.Fatal(err)
	}

	// obtain the background kafka.Producer process, and the error channel to consume
	processor, errors := producer.Background()
	go processor()

	// if not consuming errors in main thread of exec,
	// use wait group (or http.ListenAndServe, etc.) to block
	wg := &sync.WaitGroup{}

	// consume errors channel until kafka.Producer closes it, when context is canceled
	wg.Add(1)
	go func() {
		defer wg.Done()

		// consume this until library closes it for us, indicating client has shut down
		for err := range errors {
			// nil exiting
			if err != nil {
				logger.Printf("Kafka message error: %s", err)
			}
		}
	}()

	for i := 0; i < count; i++ {
		msg := kafka.ProducerMessage{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("message_%d", i+1)),
			Value: []byte("example data"),
		}
		logger.Printf("Sending message %d", i+1)

		if err := producer.Send(msg); err != nil {
			logger.Printf(err.Error()) // only happens if context is cancelled while Send() is blocked on full queue
			break
		}
	}

	// send poison pill so consumer-example will know to shut down:
	pill := kafka.ProducerMessage{
		Topic: topic,
		Key:   []byte("poison pill"),
		Value: []byte{},
	}
	producer.Send(pill)

	// signal the producer's background client to gracefully shut down
	cancelable()

	// wait for the client to shut down after draining outgoing message and error queues
	wg.Wait()
	logger.Printf("Run complete, exiting")
}
