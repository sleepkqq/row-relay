//go:build integration && kafkaauth

package relay_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sleepkqq/row-relay/internal/lab"
	"github.com/sleepkqq/row-relay/internal/relay"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestSASLTLSAndTransactionAuthorization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e, err := lab.New(ctx, "outbox-pgque")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	admin := kadm.NewClient(e.Kafka)
	var aclBuilders []*kadm.ACLBuilder
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, builder := range aclBuilders {
			_, _ = admin.DeleteACLs(cleanup, builder)
		}
	}()
	credentials, err := admin.AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{User: e.Name, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: "public-fixture-password"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = credentials.Error(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _ = admin.AlterUserSCRAMs(cleanup, []kadm.DeleteSCRAM{{User: e.Name, Mechanism: kadm.ScramSha512}}, nil)
	}()
	if err = lab.WriteOutbox(ctx, e.DB, []int64{1}, []int64{1}, []string{"authenticated"}); err != nil {
		t.Fatal(err)
	}
	var initialTick int64
	if err = e.DB.QueryRow(ctx, "SELECT sub_last_tick FROM pgque.subscription").Scan(&initialTick); err != nil {
		t.Fatal(err)
	}
	if _, err = e.DB.Exec(ctx, "SELECT pgque.force_next_tick(queue_name) FROM pgque.queue; SELECT pgque.ticker(queue_name) FROM pgque.queue"); err != nil {
		t.Fatal(err)
	}
	ca, err := filepath.Abs("../../local/secrets/ca.pem")
	if err != nil {
		t.Fatal(err)
	}
	cfg := e.Config()
	cfg.OutboxStream = "events"
	cfg.Brokers = []string{"127.0.0.1:29095"}
	cfg.Security = relay.KafkaSecurity{Protocol: "SASL_SSL", Username: e.Name, Password: "public-fixture-password", CAFile: ca}
	rejected := func(label string, cfg relay.Config) {
		t.Helper()
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		r, err := relay.Open(attempt, cfg)
		stop()
		if r != nil {
			r.Close()
		}
		if err == nil {
			t.Fatal(label, "unexpectedly authorized")
		}
		var lastTick int64
		if err = e.DB.QueryRow(ctx, "SELECT sub_last_tick FROM pgque.subscription").Scan(&lastTick); err != nil || lastTick != initialTick {
			t.Fatal(label, "advanced source", err)
		}
	}
	rejected("missing topic ACL", cfg)
	var epoch string
	if err = e.DB.QueryRow(ctx, "SELECT epoch::text FROM rowrelay_outbox.stream WHERE name='events'").Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	grant := func(builder *kadm.ACLBuilder) {
		t.Helper()
		builder.Allow("User:"+e.Name).AllowHosts("*").Operations(kadm.OpWrite, kadm.OpDescribe).ResourcePatternType(kadm.ACLPatternLiteral)
		results, err := admin.CreateACLs(ctx, builder)
		if err != nil {
			t.Fatal(err)
		}
		for _, result := range results {
			if result.Err != nil {
				t.Fatal(result.Err)
			}
		}
		aclBuilders = append(aclBuilders, builder)
	}
	grant(kadm.NewACLs().Topics(e.Name))
	rejected("missing transactional ID ACL", cfg)
	grant(kadm.NewACLs().TransactionalIDs("rowrelay-" + epoch))
	// With both ACLs present, these failures isolate authentication/trust rather
	// than passing merely because every principal lacks authorization.
	wrong := cfg
	wrong.Security.Password = "wrong-fixture-password"
	rejected("bad SCRAM password", wrong)
	wrong = cfg
	wrong.Security.CAFile = ""
	rejected("untrusted broker", wrong)
	r, err := relay.Open(ctx, cfg)
	if err != nil {
		t.Fatal("authenticated owner", err)
	}
	defer r.Close()
	if n, _, err := r.Step(ctx); err != nil || n != 1 {
		t.Fatal("authorized transaction", n, err)
	}
	c, err := e.Consumer()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.PollRecords(ctx, 1).Records(); len(got) != 1 {
		t.Fatal("authenticated delivery missing")
	}
}
