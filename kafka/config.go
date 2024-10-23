package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"time"

	"crypto/sha256"
	"crypto/sha512"
	"github.com/Shopify/sarama"
	from_env "github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"github.com/xdg/scram"
)

// simple Kafka config abstraction; can be populated from env vars
// via FromEnv() or fields can applied to CLI flags by the caller.
type Config struct {
	Username string `envconfig:"KAFKA_USERNAME"`
	Password string `envconfig:"KAFKA_PASSWORD"`

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

	SaslMechanism string `envconfig:"KAFKA_SASL_MECHANISM"`
	SaslEnabled   bool   `envconfig:"KAFKA_SASL_ENABLED"`
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
	saramaConf.Net.TLS.Enable = envConf.TLSEnabled
	configureSasl(envConf, saramaConf)

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

func configureSasl(envConf Config, saramaConf *sarama.Config) {
	if envConf.SaslEnabled {
		saramaConf.Net.SASL.Enable = envConf.SaslEnabled
		saramaConf.Net.SASL.Mechanism = sarama.SASLMechanism(envConf.SaslMechanism)
		saramaConf.Net.SASL.User = envConf.Username
		saramaConf.Net.SASL.Password = envConf.Password
		saramaConf.Net.SASL.Enable = envConf.SaslEnabled
		if envConf.SaslMechanism == sarama.SASLTypeSCRAMSHA256 {
			saramaConf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			}
		} else if envConf.SaslMechanism == sarama.SASLTypeSCRAMSHA512 {
			saramaConf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			}
		}
	}
}

var (
	// SHA256 SASLMechanism
	SHA256 scram.HashGeneratorFcn = sha256.New
	// SHA512 SASLMechanism
	SHA512 scram.HashGeneratorFcn = sha512.New
)

// XDGSCRAMClient for SASL-Protocol
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

// Begin of XDGSCRAMClient
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step of XDGSCRAMClient
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

// Done of XDGSCRAMClient
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// apply env config properties into a Sarama producer config
func configureProducer(envConf Config) (*sarama.Config, error) {
	saramaConf := sarama.NewConfig()
	saramaConf.Net.TLS.Enable = envConf.TLSEnabled
	configureSasl(envConf, saramaConf)

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
	if envConf.CACerts != "" {
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

		saramaConf.Net.TLS.Config = tlsCfg
	}

	return nil
}
