-- QEEF v1: additive, opt-in research artifacts. No operational-table writes.
-- Apply only to a disposable database until the documented rollout is approved.
BEGIN;
CREATE TABLE IF NOT EXISTS qeef_artifacts (
    owner_chat_id bigint NOT NULL CHECK (owner_chat_id <> 0),
    artifact_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('THESIS', 'THESIS_EVENT', 'EVALUATION')),
    methodology_version text NOT NULL,
    code_version text NOT NULL,
    evidence_digest text NOT NULL,
    data_as_of timestamptz NOT NULL,
    evaluated_at timestamptz NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
    content_hash text NOT NULL,
    mode text NOT NULL DEFAULT 'RESEARCH_ONLY' CHECK (mode='RESEARCH_ONLY'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(owner_chat_id, kind, artifact_id)
);
CREATE INDEX IF NOT EXISTS qeef_artifacts_owner_cutoff ON qeef_artifacts(owner_chat_id, kind, data_as_of DESC);
CREATE OR REPLACE FUNCTION qeef_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'QEEF artifacts are append-only'; END;
$$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='qeef_artifacts_immutable' AND tgrelid='qeef_artifacts'::regclass) THEN
CREATE TRIGGER qeef_artifacts_immutable BEFORE UPDATE OR DELETE ON qeef_artifacts
FOR EACH ROW EXECUTE FUNCTION qeef_reject_mutation(); END IF;
IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='qeef_artifacts_no_truncate' AND tgrelid='qeef_artifacts'::regclass) THEN
CREATE TRIGGER qeef_artifacts_no_truncate BEFORE TRUNCATE ON qeef_artifacts
FOR EACH STATEMENT EXECUTE FUNCTION qeef_reject_mutation(); END IF;
END $$;
ALTER TABLE qeef_artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE qeef_artifacts FORCE ROW LEVEL SECURITY;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname=current_schema() AND tablename='qeef_artifacts' AND policyname='qeef_owner_scope') THEN
CREATE POLICY qeef_owner_scope ON qeef_artifacts
USING (owner_chat_id = nullif(current_setting('qeef.owner', true), '')::bigint)
WITH CHECK (owner_chat_id = nullif(current_setting('qeef.owner', true), '')::bigint);
END IF;
END $$;
COMMIT;
