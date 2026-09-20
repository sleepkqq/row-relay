-- Explicit installation. Applications call enqueue in their business transaction.
CREATE SCHEMA rowrelay_outbox;
CREATE TABLE rowrelay_outbox.version (version integer PRIMARY KEY CHECK (version = 1));
INSERT INTO rowrelay_outbox.version VALUES (1);
CREATE TABLE rowrelay_outbox.stream (
  name text PRIMARY KEY CHECK (name ~ '^[a-z][a-z0-9_-]{0,62}$'),
  epoch uuid NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  queue_mode text NOT NULL CHECK (queue_mode = 'pgque'),
  topic text NOT NULL CHECK (length(topic) BETWEEN 1 AND 249)
);

CREATE FUNCTION rowrelay_outbox.enqueue(
  stream_name text, delivery_id uuid, message_key bytea, wire_value bytea,
  message_headers jsonb DEFAULT '[]'::jsonb
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  selected rowrelay_outbox.stream%ROWTYPE;
  header jsonb;
  id_headers integer := 0;
  body text;
  event_id bigint;
BEGIN
  SELECT * INTO STRICT selected FROM rowrelay_outbox.stream WHERE name = stream_name;
  IF delivery_id IS NULL OR message_key IS NULL OR octet_length(message_key) NOT BETWEEN 1 AND 4096
     OR wire_value IS NULL OR octet_length(wire_value) NOT BETWEEN 1 AND 524288
     OR message_headers IS NULL OR jsonb_typeof(message_headers) <> 'array'
     OR octet_length(message_headers::text) > 16384 THEN
    RAISE EXCEPTION 'invalid outbox ID, key, value or headers';
  END IF;
  FOR header IN SELECT * FROM jsonb_array_elements(message_headers) LOOP
    IF jsonb_typeof(header) <> 'object' OR jsonb_typeof(header->'key') IS DISTINCT FROM 'string'
       OR octet_length(header->>'key') NOT BETWEEN 1 AND 256
       OR NOT (header ? 'value') OR jsonb_typeof(header->'value') NOT IN ('string', 'null') THEN
      RAISE EXCEPTION 'invalid outbox header';
    END IF;
    PERFORM decode(header->>'value', 'base64');
    IF header->>'key' = 'id' THEN
      id_headers := id_headers + 1;
      IF convert_from(decode(header->>'value', 'base64'), 'UTF8')::uuid IS DISTINCT FROM delivery_id THEN
        RAISE EXCEPTION 'outbox delivery header does not match ID';
      END IF;
    END IF;
  END LOOP;
  IF id_headers > 1 THEN RAISE EXCEPTION 'duplicate outbox delivery header'; END IF;
  IF id_headers = 0 THEN
    message_headers := message_headers || jsonb_build_array(jsonb_build_object(
      'key', 'id', 'value', encode(convert_to(delivery_id::text, 'UTF8'), 'base64')));
  END IF;
  IF octet_length(message_headers::text) > 16384 THEN RAISE EXCEPTION 'outbox headers exceed limit'; END IF;
  body := jsonb_build_object('id', delivery_id, 'key', encode(message_key, 'base64'),
    'value', encode(wire_value, 'base64'), 'headers', message_headers)::text;
  -- Same-key enqueues serialize until commit. Allocate the queue ID only AFTER
  -- taking this lock, so a later committed same-key enqueue cannot overtake it.
  -- Hash collisions only serialize unrelated keys; callers still retry deadlocks.
  -- Two-int lock keys are disjoint from the bigint publisher/installer locks.
  -- ponytail: one lock per distinct key/TX; bound bulk TXs to the server lock
  -- budget, or use application aggregate-row locking for larger transactions.
  PERFORM pg_advisory_xact_lock(hashtext(selected.epoch::text), hashtext(encode(message_key, 'hex')));
  SELECT pgque.insert_event('rowrelay_outbox.' || selected.epoch::text, 'wire', body) INTO event_id;
  RETURN event_id;
END;
$$;
REVOKE ALL ON SCHEMA rowrelay_outbox FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA rowrelay_outbox FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA rowrelay_outbox FROM PUBLIC;
