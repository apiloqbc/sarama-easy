package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"time"

	"github.com/IBM/sarama"
	from_env "github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
)

// simple Kafka config abstraction; can be populated from env vars
// via FromEnv() or fields can applied to CLI flags by the caller.
type Config struct {
	Brokers  string `envconfig:"KAFKA_BROKERS"`
	Version  string `envconfig:"KAFKA_VERSION"`
	Verbose  bool   `envconfig:"KAFKA_VERBOSE"`
	ClientID string `envconfig:"KAFKA_CLIENT_ID"`
	Topics   string `envconfig:"KAFKA_TOPICS"`

	TLSEnabled bool   `envconfig:"KAFKA_TLS_ENABLED"`
	TLSKey     string `envconfig:"KAFKA_TLS_KEY"`
	TLSCert    string `envconfig:"KAFKA_TLS_CERT"`
	CACerts    string `envconfig:"KAFKA_CA_CERTS"`

	// Consumer specific parameters
	Group             string        `envconfig:"KAFKA_GROUP"`
	RebalanceStrategy string        `envconfig:"KAFKA_REBALANCE_STRATEGY"`
	RebalanceTimeout  time.Duration `envconfig:"KAFKA_REBALANCE_TIMEOUT"`
	InitOffsets       string        `envconfig:"KAFKA_INIT_OFFSETS"`
	CommitInterval    time.Duration `envconfig:"KAFKA_COMMIT_INTERVAL"`

	// Producer specific parameters
	FlushInterval time.Duration `envconfig:"KAFKA_FLUSH_INTERVAL"`

	// Schema Registry server
	SchemaRegistryServers string `envconfig:"KAFKA_SCHEMA_REGISTRY_SERVERS"`

	IsolationLevel string `envconfig:"KAFKA_ISOLATION_LEVEL"`

	SASL SASL
}

type SASL struct {
	// Whether or not to use SASL authentication when connecting to the broker
	// (defaults to false).
	Enable bool
	// SASLMechanism is the name of the enabled SASL mechanism.
	// Possible values: OAUTHBEARER, PLAIN (defaults to PLAIN).
	Mechanism sarama.SASLMechanism
	// Version is the SASL Protocol Version to use
	// Kafka > 1.x should use V1, except on Azure EventHub which use V0
	Version int16
	// Whether or not to send the Kafka SASL handshake first if enabled
	// (defaults to true). You should only set this to false if you're using
	// a non-Kafka SASL proxy.
	Handshake bool
	// AuthIdentity is an (optional) authorization identity (authzid) to
	// use for SASL/PLAIN authentication (if different from User) when
	// an authenticated user is permitted to act as the presented
	// alternative user. See RFC4616 for details.
	AuthIdentity string
	// User is the authentication identity (authcid) to present for
	// SASL/PLAIN or SASL/SCRAM authentication
	User string
	// Password for SASL/PLAIN authentication
	Password string
	// authz id used for SASL/SCRAM authentication
	SCRAMAuthzID string
}

// returns a new kafka.Config with reasonable defaults for some values
func NewKafkaConfig() Config {
	return Config{
		Brokers:           "localhost:9092",
		Version:           "1.1.0",
		Group:             "default-group",
		ClientID:          "sarama-easy",
		RebalanceStrategy: "roundrobin",
		RebalanceTimeout:  1 * time.Minute,
		InitOffsets:       "latest",
		CommitInterval:    10 * time.Second,
		FlushInterval:     1 * time.Second,
	}
}

// hydrate kafka.Config using environment variables
func FromEnv() (Config, error) {
	var conf Config
	err := from_env.Process("", &conf)

	return conf, err
}

const errorQueueSize = 32

// apply env config properties to a Sarama consumer config
func configureConsumer(envConf Config) (*sarama.Config, error) {
	saramaConf := sarama.NewConfig()

	// Kafka broker version is mandatory for API compatability
	version, err := sarama.ParseKafkaVersion(envConf.Version)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing Kafka version: %v", envConf.Version)
	}
	saramaConf.Version = version

	saramaConf.ClientID = envConf.ClientID
	saramaConf.Consumer.Return.Errors = true
	saramaConf.Consumer.Offsets.CommitInterval = envConf.CommitInterval
	saramaConf.Consumer.Group.Rebalance.Timeout = envConf.RebalanceTimeout
	saramaConf.Consumer.Group.Rebalance.Retry.Max = 6
	saramaConf.Consumer.Group.Rebalance.Retry.Backoff = 2 * time.Second

	if err := configureTLS(envConf, saramaConf); err != nil {
		return nil, err
	}

	// configure group rebalance strategy
	switch envConf.RebalanceStrategy {
	case "roundrobin":
		saramaConf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		saramaConf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		return nil, errors.Errorf("unrecognized consumer group partition strategy: %s", envConf.RebalanceStrategy)
	}
	// configure group rebalance strategy
	switch envConf.IsolationLevel {
	case "ReadUncommitted":
		saramaConf.Consumer.IsolationLevel = sarama.ReadUncommitted
	case "ReadCommitted":
		saramaConf.Consumer.IsolationLevel = sarama.ReadCommitted
	default:
		saramaConf.Consumer.IsolationLevel = sarama.ReadUncommitted
	}

	// conf init offsets default: only honored if brokers on Kafka side have no pre-stored offsets for group
	switch envConf.InitOffsets {
	case "earliest":
		saramaConf.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "latest":
		saramaConf.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		return nil, errors.Errorf("failed to parse Kafka initial offset from service saramaConf: %s", envConf.InitOffsets)
	}

	return saramaConf, nil
}

// apply env config properties into a Sarama producer config
func configureProducer(envConf Config) (*sarama.Config, error) {
	saramaConf := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(envConf.Version)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing Kafka version: %v", envConf.Version)
	}

	if err := configureTLS(envConf, saramaConf); err != nil {
		return nil, err
	}

	// Produce side configs (TODO: tune and customize more settings if needed)
	saramaConf.Version = version
	saramaConf.ClientID = envConf.ClientID
	saramaConf.Producer.RequiredAcks = sarama.WaitForLocal     // Only wait for the leader to ack
	saramaConf.Producer.Compression = sarama.CompressionSnappy // Compress messages
	saramaConf.Producer.Flush.Frequency = envConf.FlushInterval
	saramaConf.Producer.Return.Successes = false
	saramaConf.Producer.Return.Errors = true

	return saramaConf, nil
}

// side effect TLS setup into Sarama config if env config specifies to do so
func configureTLS(envConf Config, saramaConf *sarama.Config) error {
	// configure TLS
	if envConf.TLSEnabled {
		cert, err := tls.LoadX509KeyPair(envConf.TLSCert, envConf.TLSKey)
		if err != nil {
			return errors.Wrapf(err, "failed to load TLS cert(%s) and key(%s)", envConf.TLSCert, envConf.TLSKey)
		}

		ca, err := ioutil.ReadFile(envConf.CACerts)
		if err != nil {
			return errors.Wrapf(err, "failed to load CA cert bundle at: %s", envConf.CACerts)
		}

		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)

		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
		}

		saramaConf.Net.TLS.Enable = true
		saramaConf.Net.TLS.Config = tlsCfg
	}

	return nil
}
