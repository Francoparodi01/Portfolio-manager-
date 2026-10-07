-- T1 Telegram Contextual Shadow
--
-- Reuses the G2/G3 lineage model.  A contextual Telegram run needs a plan_id
-- for the existing snapshot foreign key, but the row must have no execution
-- authority.  Additive authority columns preserve every existing productive
-- caller through defaults and make the shadow envelope explicit.

ALTER TABLE execution_plans
    ADD COLUMN IF NOT EXISTS authority_mode TEXT NOT NULL DEFAULT 'PRODUCTIVE',
    ADD COLUMN IF NOT EXISTS affects_analysis BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS affects_execution BOOLEAN NOT NULL DEFAULT TRUE;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'execution_plans_authority_mode_check'
          AND conrelid = 'execution_plans'::regclass
    ) THEN
        ALTER TABLE execution_plans
            ADD CONSTRAINT execution_plans_authority_mode_check
            CHECK (authority_mode IN ('PRODUCTIVE', 'SHADOW_ONLY'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'execution_plans_contextual_shadow_safe_check'
          AND conrelid = 'execution_plans'::regclass
    ) THEN
        ALTER TABLE execution_plans
            ADD CONSTRAINT execution_plans_contextual_shadow_safe_check
            CHECK (
                authority_mode <> 'SHADOW_ONLY'
                OR (
                    gate = 'SHADOW_ONLY'
                    AND feasible = FALSE
                    AND affects_analysis = FALSE
                    AND affects_execution = FALSE
                    AND gross_sell_ars = 0
                    AND fee_sell_ars = 0
                    AND net_sell_ars = 0
                    AND gross_buy_ars = 0
                    AND fee_buy_ars = 0
                    AND cash_before = cash_after
                )
            );
    END IF;
END $$;

CREATE OR REPLACE FUNCTION quantia_reject_contextual_shadow_order_intent()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    parent_authority TEXT;
    parent_affects_execution BOOLEAN;
BEGIN
    SELECT authority_mode, affects_execution
      INTO parent_authority, parent_affects_execution
      FROM execution_plans
     WHERE id = NEW.execution_plan_id;

    IF parent_authority = 'SHADOW_ONLY'
       OR parent_affects_execution IS FALSE THEN
        RAISE EXCEPTION 'contextual shadow plans cannot create order intents';
    END IF;
    RETURN NEW;
END;
$$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'quantia_contextual_shadow_order_intent_guard'
          AND tgrelid = 'order_intents'::regclass
    ) THEN
        CREATE TRIGGER quantia_contextual_shadow_order_intent_guard
        BEFORE INSERT OR UPDATE ON order_intents
        FOR EACH ROW EXECUTE FUNCTION quantia_reject_contextual_shadow_order_intent();
    END IF;
END $$;
