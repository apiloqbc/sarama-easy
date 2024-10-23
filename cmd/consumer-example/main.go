package main

import (
	"context"
	"flag"
	"github.com/kelseyhightower/envconfig"
	"log"
	"os"

	"github.com/apiloqbc/sarama-easy/kafka"
)

var conf kafka.Config

func init() {
	// Initialize the Kafka configuration
	conf = kafka.NewKafkaConfig()

	// Process environment variables to populate the config struct
	err := envconfig.Process("", &conf)
	if err != nil {
		log.Fatal("Error reading environment variables: ", err.Error())
	}

	flag.StringVar(&conf.Topics, "topics", "mytopic", "CSV list of Kafka topics to consume")

	// Override configuration values with command-line flags if provided
	flag.StringVar(&conf.Brokers, "brokers", conf.Brokers, "CSV list of Kafka seed brokers")
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

	// Parse the command-line flags
	flag.Parse()

	// Log the final configuration for verification
	log.Printf("Config: %+v\n", conf)
}

func main() {
	flag.Parse()

	logger := log.New(os.Stdout, "[example Kafka consumer] ", log.LstdFlags|log.LUTC|log.Lshortfile)

	// context can be canceled, initiating kafka.Consumer shutdown, from
	// kafka.Handlers scope or here in the main thread, it doesn't matter
	ctx, cancelable := context.WithCancel(context.Background())

	// implements kafka.Handler. All messages will be passed here by kafka.Consumer
	handlers := &exampleHandler{
		// arbitrary things you might want access to in kafka.Handler scope
		cancelable: cancelable,
		logger:     logger,
	}

	consumer, err := kafka.NewConsumer(ctx, conf, handlers, logger)
	if err != nil {
		logger.Fatal(err)
	}

	// obtain the background kafka.Consumer process that does the work,
	// and the error channel to consume (closed by Consumer on shutdown)
	processor, errors := consumer.Background()
	go processor()

	// could consume the errors channel in a goroutine, as producer-example does.
	for err := range errors {
		log.Printf("Kafka message error: %s", err)
		// note: if an error is deemed fatal you can call cancelable() from here too
	}

	log.Printf("Run complete, exiting")
}

// example kafka.Handler implementation
type exampleHandler struct {
	// included here so I have the option to gracefully shut down the consumer
	cancelable context.CancelFunc

	// so I can log the messages for our example run
	logger *log.Logger
}

// implements kafka.Handler - errors returned here will be treated as fatal
// and will trigger graceful shutdown of the kafka.Consumer
func (eh *exampleHandler) Handle(msg *kafka.Message) error {
	key := string(msg.Key)
	value := string(msg.Value)
	topic := msg.Topic

	eh.logger.Printf("Kafka topic %s message received: Key(%s) Value(%s)", topic, key, value)

	// when the example producer sends last message, trigger graceful shutdown
	if key == "poison pill" {
		eh.cancelable()
	}

	// no need to bubble up fatal errors in our example
	return nil
}
