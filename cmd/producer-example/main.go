package main

import (
	"context"
	"flag"
	"github.com/IBM/sarama"
	"github.com/apiloqbc/sarama-easy/kafka"
	"github.com/linkedin/goavro/v2"
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
	conf = kafka.NewKafkaConfig()

	// apply minimal config only for example run
	flag.StringVar(&conf.Brokers, "brokers", "localhost:9092", "CSV list of Kafka seed brokers to produce events to")
	flag.StringVar(&conf.SchemaRegistryServers, "schemaregistry", "http://localhost:8081", "CSV list of schema registry server")
	flag.StringVar(&topic, "topic", "example", "CSV list of Kafka topic to produce to")
	flag.BoolVar(&conf.Verbose, "verbose", false, "Log detailed Kafka client internals?")
	flag.IntVar(&count, "count", 10, "number of example messages to produce")
}

func main() {
	//Sample Data
	user := &User{
		FirstName: "John",
		LastName:  "Snow",
		Address: &Address{
			Address1: "1106 Pennsylvania Avenue",
			City:     "Wilmington",
			State:    "DE",
			Zip:      19806,
		},
	}

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
			logger.Printf("Kafka message error: %s", err)
		}
	}()

	avroCodec, err := goavro.NewCodec(`{
		"namespace": "my.namespace.com",
		"type":	"record",
		"name": "indentity",
		"fields": [
			{ "name": "FirstName", "type": "string"},
			{ "name": "LastName", "type": "string"},
			{ "name": "Errors", "type": ["null", {"type":"array", "items":"string"}], "default": null },
			{ "name": "Address", "type": ["null",{
				"namespace": "my.namespace.com",
				"type":	"record",
				"name": "address",
				"fields": [
					{ "name": "Address1", "type": "string" },
					{ "name": "Address2", "type": ["null", "string"], "default": null },
					{ "name": "City", "type": "string" },
					{ "name": "State", "type": "string" },
					{ "name": "Zip", "type": "int" }
				]
			}],"default":null}
		]
	}`)
	if err != nil {
		log.Fatal(err)
	}
	schemaId, err := producer.GetSchemaId(topic, avroCodec)
	if err != nil {
		log.Fatal(err)
	}

	// Convert native Go form to binary Avro data
	binaryValue, err := avroCodec.BinaryFromNative(nil, user.ToStringMap())

	if err != nil {
		logger.Fatal(err)
	}
	binaryMsg := &kafka.AvroEncoder{
		SchemaID: schemaId,
		Content:  binaryValue,
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("key"),
		Value: binaryMsg,
	}

	producer.SendAvroMsg(msg)

	// signal the producer's background client to gracefully shut down
	cancelable()

	// wait for the client to shut down after draining outgoing message and error queues
	wg.Wait()
	logger.Printf("Run complete, exiting")
}

// User holds information about a user.
type User struct {
	FirstName string
	LastName  string
	Errors    []string
	Address   *Address
}

// Address holds information about an address.
type Address struct {
	Address1 string
	Address2 string
	City     string
	State    string
	Zip      int
}

// ToStringMap returns a map representation of the User.
func (u *User) ToStringMap() map[string]interface{} {
	datumIn := map[string]interface{}{
		"FirstName": string(u.FirstName),
		"LastName":  string(u.LastName),
	}

	if len(u.Errors) > 0 {
		datumIn["Errors"] = goavro.Union("array", u.Errors)
	} else {
		datumIn["Errors"] = goavro.Union("null", nil)
	}

	if u.Address != nil {
		addDatum := map[string]interface{}{
			"Address1": string(u.Address.Address1),
			"City":     string(u.Address.City),
			"State":    string(u.Address.State),
			"Zip":      int(u.Address.Zip),
		}
		if u.Address.Address2 != "" {
			addDatum["Address2"] = goavro.Union("string", u.Address.Address2)
		} else {
			addDatum["Address2"] = goavro.Union("null", nil)
		}

		//important need namespace and record name
		datumIn["Address"] = goavro.Union("my.namespace.com.address", addDatum)

	} else {
		datumIn["Address"] = goavro.Union("null", nil)
	}
	return datumIn
}
