-- Install explicitly as the owner of the captured tables. No replication required.
CREATE SCHEMA rowrelay;
CREATE TABLE rowrelay.source (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  epoch uuid NOT NULL DEFAULT gen_random_uuid(),
  queue_mode text NOT NULL CHECK (queue_mode = 'pgque')
);

CREATE FUNCTION rowrelay.capture() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
  body text;
BEGIN
  body := jsonb_build_object(
    'schema', TG_TABLE_SCHEMA, 'table', TG_TABLE_NAME, 'op', TG_OP,
    'before', CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) END,
    'after', CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) END
  )::text;
  IF octet_length(body) > 524288 THEN
    RAISE EXCEPTION 'rowrelay capture exceeds 512 KiB limit';
  END IF;
  PERFORM pgque.insert_event('rowrelay', 'row', body);
  RETURN NULL;
END;
$$;

CREATE FUNCTION rowrelay.reject_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'rowrelay: TRUNCATE requires an explicit capture reset';
END;
$$;

-- Application migrations call this as the table owner, after their domain DDL.
-- Queue installation/epoch remain an explicit, separate operator action.
CREATE FUNCTION rowrelay.install_capture(source_schema text, source_tables text[]) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
  source_table text;
  relation regclass;
  valid boolean;
BEGIN
  IF source_schema IS NULL OR source_schema = '' OR coalesce(cardinality(source_tables), 0) = 0 THEN
    RAISE EXCEPTION 'rowrelay: capture requires a schema and tables';
  END IF;
  FOREACH source_table IN ARRAY source_tables LOOP
    IF source_table IS NULL OR source_table = '' THEN
      RAISE EXCEPTION 'rowrelay: empty captured table';
    END IF;
    relation := format('%I.%I', source_schema, source_table)::regclass;
    SELECT c.relkind = 'r' AND c.relpersistence = 'p'
      AND NOT EXISTS (SELECT FROM pg_inherits WHERE inhparent = c.oid)
      AND EXISTS (SELECT FROM pg_constraint WHERE conrelid = c.oid AND contype = 'p')
      INTO valid FROM pg_class c WHERE c.oid = relation;
    IF valid IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'rowrelay: captured table must be logged, ordinary, have a primary key and no inheritance children';
    END IF;
    EXECUTE format('DROP TRIGGER IF EXISTS rowrelay_capture ON %s', relation);
    EXECUTE format('CREATE TRIGGER rowrelay_capture AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION rowrelay.capture()', relation);
    EXECUTE format('DROP TRIGGER IF EXISTS rowrelay_no_truncate ON %s', relation);
    EXECUTE format('CREATE TRIGGER rowrelay_no_truncate BEFORE TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION rowrelay.reject_truncate()', relation);
  END LOOP;
END;
$$;
REVOKE ALL ON SCHEMA rowrelay FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA rowrelay FROM PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA rowrelay FROM PUBLIC;
