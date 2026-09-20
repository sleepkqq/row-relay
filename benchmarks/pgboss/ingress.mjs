// Common producer-side bridge: Go owns generation and the independent oracle.
import { createInterface } from 'node:readline';
import pg from 'pg';
import { PgBoss } from 'pg-boss';

const mode = process.env.BASELINE_MODE;
if (!['pgque', 'pgboss'].includes(mode)) throw new Error('invalid mode');
const client = new pg.Client({ connectionString: process.env.DATABASE_URL, statement_timeout: 30000 });
client.on('error', () => process.exit(1));
await client.connect();
const boss = mode === 'pgboss' ? new PgBoss({
  connectionString: process.env.DATABASE_URL, max: 2, schedule: false, supervise: false,
}) : null;
boss?.on('error', () => process.exit(1));
try {
  await client.query(`CREATE TABLE bench_fact (
    id bigint PRIMARY KEY REFERENCES items(id), delivery_id uuid UNIQUE NOT NULL,
    kafka_key bytea NOT NULL, kafka_value bytea NOT NULL);
    CREATE TABLE bench_source (epoch uuid NOT NULL DEFAULT gen_random_uuid());
    INSERT INTO bench_source DEFAULT VALUES`);
  if (boss) {
    await boss.start();
    await boss.createQueue('events', {
      policy: 'key_strict_fifo', expireInSeconds: 120,
      retentionSeconds: 1209600, deleteAfterSeconds: 30, retryLimit: 100,
    });
  }
  console.log(JSON.stringify({ ready: true }));
  for await (const line of createInterface({ input: process.stdin, crlfDelay: Infinity })) {
    const records = JSON.parse(line);
    if (!Array.isArray(records) || records.length < 1 || records.length > 1000) {
      throw new Error('invalid producer batch');
    }
    await client.query('BEGIN');
    try {
      // sent_ns remains a decimal string: JavaScript Number cannot represent it.
      await client.query(`WITH input AS (
        SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
          id bigint, sent_ns bigint, payload text, checksum text,
          delivery_id uuid, kafka_key text, kafka_value text)
      ), items_insert AS (
        INSERT INTO items(id,sent_ns,payload) SELECT id,sent_ns,payload FROM input RETURNING id
      ), expected_insert AS (
        INSERT INTO expected(id,sent_ns,checksum) SELECT id,sent_ns,checksum FROM input
      ) INSERT INTO bench_fact
        SELECT id,delivery_id,decode(kafka_key,'base64'),decode(kafka_value,'base64') FROM input`,
      [JSON.stringify(records)]);
      if (boss) {
        const ids = await boss.insert('events', records.map(row => ({
          id: row.delivery_id, data: { id: row.id }, singletonKey: row.id,
        })), { db: { executeSql: (sql, values) => client.query(sql, values) }, returnId: true });
        if (ids?.length !== records.length) throw new Error('incomplete enqueue');
      } else {
        await client.query(`SELECT rowrelay_outbox.enqueue('events',delivery_id,kafka_key,kafka_value)
          FROM bench_fact WHERE id=ANY($1::bigint[]) ORDER BY id`, [records.map(row => row.id)]);
      }
      await client.query('COMMIT');
      console.log(JSON.stringify({ committed: records.length }));
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  }
} finally {
  await boss?.stop();
  await client.end();
}
