-- Phase 2. Additive research evidence and mutable delivery metadata only.
-- Requires 20261014_qeef_v1.sql. Never auto-applied by producer or worker.
BEGIN;
CREATE TABLE IF NOT EXISTS qeef_events (
 owner_chat_id bigint NOT NULL CHECK(owner_chat_id<>0),
 event_id text NOT NULL,
 kind text NOT NULL,
 source_id text NOT NULL,
 source_version text NOT NULL,
 effective_at timestamptz NOT NULL,
 available_at timestamptz NOT NULL,
 received_at timestamptz NOT NULL DEFAULT now(),
 parent_id text,
 content_hash text NOT NULL,
 payload jsonb NOT NULL,
 PRIMARY KEY(owner_chat_id,event_id),
 CHECK(jsonb_typeof(payload)='object'),
 CHECK(kind IN ('PLAN','PORTFOLIO','FILL','MOVEMENTS','MARKET','EVALUATION_INPUT','ACCOUNTING_INPUT','THESIS_EVENT','CASE','RECONCILIATION','EVALUATION','LIFECYCLE'))
);
CREATE INDEX IF NOT EXISTS qeef_events_source ON qeef_events(owner_chat_id,kind,effective_at,source_id);
CREATE TABLE IF NOT EXISTS qeef_jobs (
 owner_chat_id bigint NOT NULL,
 event_id text NOT NULL,
 status text NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','RETRY','DONE','DEAD_LETTER')),
 attempts int NOT NULL DEFAULT 0,
 duplicate_deliveries int NOT NULL DEFAULT 0,
 last_error_code text,
 next_attempt_at timestamptz NOT NULL DEFAULT now(),
 processed_at timestamptz,
 PRIMARY KEY(owner_chat_id,event_id),
 FOREIGN KEY(owner_chat_id,event_id) REFERENCES qeef_events(owner_chat_id,event_id)
);
DO $$ DECLARE tab text; BEGIN
 FOREACH tab IN ARRAY ARRAY['qeef_events','qeef_jobs'] LOOP
  EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',tab);
  EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY',tab);
  IF NOT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname=current_schema() AND tablename=tab AND policyname='qeef_owner_scope') THEN
   EXECUTE format('CREATE POLICY qeef_owner_scope ON %I USING(owner_chat_id=nullif(current_setting(''qeef.owner'',true),'''')::bigint) WITH CHECK(owner_chat_id=nullif(current_setting(''qeef.owner'',true),'''')::bigint)',tab);
  END IF;
 END LOOP;
 IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='qeef_events_immutable' AND tgrelid='qeef_events'::regclass) THEN
  CREATE TRIGGER qeef_events_immutable BEFORE UPDATE OR DELETE ON qeef_events FOR EACH ROW EXECUTE FUNCTION qeef_reject_mutation();
 END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='qeef_events_no_truncate' AND tgrelid='qeef_events'::regclass) THEN
  CREATE TRIGGER qeef_events_no_truncate BEFORE TRUNCATE ON qeef_events FOR EACH STATEMENT EXECUTE FUNCTION qeef_reject_mutation();
 END IF;
END $$;
COMMIT;
