// Official fetch/complete API, not copied claim SQL or the SDK's work scheduler.
import { setTimeout as delay } from 'node:timers/promises';
import pg from 'pg';
import { PgBoss } from 'pg-boss';
import { Kafka, CompressionTypes, Partitioners, logLevel } from 'kafkajs';

const poll = Number(process.env.POLL_MS);
const batch = Number(process.env.BATCH_COUNT);
if (!Number.isFinite(poll) || poll <= 0 || poll > 60000 ||
    !Number.isInteger(batch) || batch < 1 || batch > 1000) throw new Error('invalid bounds');
const client = new pg.Client({ connectionString: process.env.DATABASE_URL, statement_timeout: 30000 });
client.on('error', () => process.exit(1));
const db = { executeSql: (sql, values) => client.query(sql, values) };
const boss = new PgBoss({ connectionString: process.env.DATABASE_URL, max: 2, schedule: false });
boss.on('error', () => process.exit(1));
let producer;
let stopping = false;
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => {
  stopping = true;
  setTimeout(() => process.exit(1), 5000).unref();
});
try {
  await client.connect();
  const lock = await client.query('SELECT pg_try_advisory_lock(1380931404::bigint) AS owned');
  if (!lock.rows[0].owned) throw new Error('source already owned');
  await client.query("SELECT set_config('idle_session_timeout',$1,false)", [`${35000 + poll}ms`]);
  const { rows: sources } = await client.query('SELECT epoch FROM bench_source');
  if (sources.length !== 1) throw new Error('invalid source identity');
  await boss.start();
  producer = new Kafka({
    clientId: 'rowrelay-pgboss-baseline', brokers: process.env.KAFKA_BROKERS.split(','),
    logLevel: logLevel.ERROR, requestTimeout: 30000, retry: { retries: 5 },
  }).producer({
    transactionalId: `rowrelay-${sources[0].epoch}`, idempotent: true,
    maxInFlightRequests: 1, transactionTimeout: 30000, allowAutoTopicCreation: false,
    createPartitioner: Partitioners.DefaultPartitioner,
  });
  await producer.connect();
  // transaction() initializes the broker epoch eagerly; fence before fetching.
  await (await producer.transaction()).commit();
  console.log('ready: official pg-boss fetch/complete; Kafka transactions; read_committed required');
  while (!stopping) {
    const jobs = await boss.fetch('events', { batchSize: batch, db });
    if (jobs.length === 0) {
      await delay(poll);
      continue;
    }
    const { rows } = await client.query(
      'SELECT id,delivery_id,kafka_key,kafka_value FROM bench_fact WHERE id=ANY($1::bigint[])',
      [jobs.map(job => job.data.id)],
    );
    const facts = new Map(rows.map(row => [row.id, row]));
    const messages = jobs.map(job => {
      const fact = facts.get(job.data.id);
      if (!fact || fact.delivery_id !== job.id) throw new Error('missing or mismatched immutable fact');
      return { key: fact.kafka_key, value: fact.kafka_value, headers: { id: fact.delivery_id } };
    });
    const transaction = await producer.transaction();
    // KafkaJS does not split a partition batch by bytes. Stay below broker's 1 MiB
    // record-batch limit, without changing the source claim/transaction boundary.
    let chunk = [], bytes = 0;
    for (const message of messages) {
      const size = message.key.length + message.value.length + 256;
      if (size > 900000) throw new Error('oversized fixture record');
      if (bytes + size > 900000) {
        await transaction.send({ topic: process.env.KAFKA_TOPIC, messages: chunk, acks: -1, compression: CompressionTypes.None });
        chunk = []; bytes = 0;
      }
      chunk.push(message); bytes += size;
    }
    await transaction.send({ topic: process.env.KAFKA_TOPIC, messages: chunk, acks: -1, compression: CompressionTypes.None });
    await transaction.commit();
    await client.query('BEGIN');
    try {
      const { rows: locks } = await client.query(`SELECT EXISTS (SELECT FROM pg_locks
        WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted
          AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
          AND mode='ExclusiveLock' AND classid=0 AND objid=1380931404 AND objsubid=1) AS owned`);
      if (!locks[0].owned) throw new Error('source ownership lost');
      const result = await boss.complete('events', jobs.map(job => job.id), null, { db });
      if (result.affected !== jobs.length) throw new Error('incomplete source ACK');
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  }
} finally {
  // A failed/ambiguous transaction terminates this worker. No epoch reinitialization
  // or abort/retry that could fence a successor after loss of PostgreSQL ownership.
  await producer?.disconnect();
  await boss.stop();
  await client.end();
}
