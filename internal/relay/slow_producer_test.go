//go:build integration

package relay

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type slowProduceHook time.Duration

func (delay slowProduceHook) OnProduceRecordBuffered(*kgo.Record) { time.Sleep(time.Duration(delay)) }

// DelayProducerForTest installs a real idempotent Kafka client with a deliberately
// slow local produce hook. It is visible only to the external integration tests.
func DelayProducerForTest(r *Runner, delay time.Duration) error {
	r.kafka.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(r.config.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.WithHooks(slowProduceHook(delay)))
	if err != nil {
		return err
	}
	r.kafka = client
	return nil
}
