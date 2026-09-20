package relay

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// KafkaSecurity is shared by every stream using the same producer. Credentials
// are loaded from the environment; errors never include their values.
type KafkaSecurity struct {
	Protocol string
	Username string
	Password string
	CAFile   string
}

func KafkaSecurityFromEnv(getenv func(string) string) KafkaSecurity {
	return KafkaSecurity{Protocol: getenv("KAFKA_SECURITY_PROTOCOL"),
		Username: getenv("KAFKA_SASL_USERNAME"), Password: getenv("KAFKA_SASL_PASSWORD"),
		CAFile: getenv("KAFKA_TLS_CA_FILE")}
}

func (s KafkaSecurity) Validate() error {
	switch s.Protocol {
	case "", "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL":
	default:
		return errors.New("unsupported Kafka security protocol")
	}
	if strings.HasPrefix(s.Protocol, "SASL_") {
		if s.Username == "" || s.Password == "" {
			return errors.New("Kafka SCRAM-SHA-512 requires username and password")
		}
	} else if s.Username != "" || s.Password != "" {
		return errors.New("Kafka credentials require an explicit SASL protocol")
	}
	if s.CAFile != "" && !strings.HasSuffix(s.Protocol, "SSL") {
		return errors.New("Kafka CA configuration requires a TLS protocol")
	}
	return nil
}

func (s KafkaSecurity) tlsConfig() (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if s.CAFile != "" {
		pem, err := os.ReadFile(s.CAFile)
		if err != nil {
			return nil, errors.New("cannot read Kafka CA bundle")
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("Kafka CA bundle contains no valid certificates")
		}
		config.RootCAs = roots
	}
	// franz-go derives the verified server name from each broker's address.
	return config, nil
}

func (s KafkaSecurity) options() ([]kgo.Opt, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	var options []kgo.Opt
	if strings.HasSuffix(s.Protocol, "SSL") {
		config, err := s.tlsConfig()
		if err != nil {
			return nil, err
		}
		options = append(options, kgo.DialTLSConfig(config))
	}
	if strings.HasPrefix(s.Protocol, "SASL_") {
		options = append(options, kgo.SASL(scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism()))
	}
	return options, nil
}
