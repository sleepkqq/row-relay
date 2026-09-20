package relay

import (
	"crypto/tls"
	"encoding/pem"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestKafkaSecurityRejectsAmbiguousConfiguration(t *testing.T) {
	for _, s := range []KafkaSecurity{
		{Protocol: "unknown-secret"}, {Password: "private-password"},
		{Protocol: "SSL", Username: "private-user"},
		{Protocol: "SASL_SSL", Username: "private-user"},
		{Protocol: "SASL_PLAINTEXT", Password: "private-password"},
		{Protocol: "PLAINTEXT", CAFile: "private-path"},
	} {
		if _, err := s.options(); err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "unknown-secret") {
			t.Fatal("unsafe security configuration accepted or leaked", err)
		}
	}
	for _, protocol := range []string{"", "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"} {
		s := KafkaSecurity{Protocol: protocol}
		if strings.HasPrefix(protocol, "SASL_") {
			s.Username, s.Password = "user", "password"
		}
		if _, err := s.options(); err != nil {
			t.Fatal(protocol, err)
		}
	}
}

func TestKafkaTLSVerifiesTrustAndBrokerName(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	file := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	s := KafkaSecurity{Protocol: "SSL", CAFile: file}
	config, err := s.tlsConfig()
	if err != nil {
		t.Fatal(err)
	}
	dial := func(config *tls.Config) error {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", server.Listener.Addr().String(), config)
		if conn != nil {
			conn.Close()
		}
		return err
	}
	if err = dial(config); err != nil {
		t.Fatal("configured trust failed", err)
	}
	config.ServerName = "wrong.invalid"
	if err = dial(config); err == nil {
		t.Fatal("broker hostname was not verified")
	}
	if err = dial(&tls.Config{MinVersion: tls.VersionTLS12}); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
	if err = os.WriteFile(file, []byte("invalid CA private content"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.options(); err == nil || strings.Contains(err.Error(), "private content") {
		t.Fatal("invalid trust bundle accepted or leaked", err)
	}
}
