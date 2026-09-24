"""Append-only artifacts and additive PostgreSQL persistence.

Filesystem artifacts contain replayable input data. Treat them as private account
information. Database tables contain immutable evidence and references, with no
foreign-key cascades or updates to operational tables.
"""

import csv
import hashlib
import io
import json
from pathlib import Path

from .models import canonical, digest

SCHEMA = """
CREATE TABLE IF NOT EXISTS decision_lab_objects (
    object_hash text PRIMARY KEY, owner_chat_id bigint NOT NULL,
    kind text NOT NULL, payload jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS decision_lab_runs (
    replay_run_id text PRIMARY KEY, owner_chat_id bigint NOT NULL,
    evaluated_as_of timestamptz NOT NULL, config jsonb NOT NULL,
    summary jsonb NOT NULL, object_refs jsonb NOT NULL,
    content_hash text NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS decision_lab_plan_captures (
    capture_hash text PRIMARY KEY, owner_chat_id bigint NOT NULL, plan_id text NOT NULL,
    captured_at timestamptz NOT NULL, payload jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS decision_lab_captures_owner_time ON decision_lab_plan_captures(owner_chat_id,captured_at);
CREATE INDEX IF NOT EXISTS decision_lab_runs_owner_cutoff
    ON decision_lab_runs(owner_chat_id,evaluated_as_of DESC);
CREATE OR REPLACE FUNCTION decision_lab_reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Decision Lab evidence is append-only'; END; $$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='decision_lab_objects_immutable' AND tgrelid='decision_lab_objects'::regclass) THEN
CREATE TRIGGER decision_lab_objects_immutable BEFORE UPDATE OR DELETE ON decision_lab_objects
FOR EACH ROW EXECUTE FUNCTION decision_lab_reject_mutation(); END IF;
IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='decision_lab_runs_immutable' AND tgrelid='decision_lab_runs'::regclass) THEN
CREATE TRIGGER decision_lab_runs_immutable BEFORE UPDATE OR DELETE ON decision_lab_runs
FOR EACH ROW EXECUTE FUNCTION decision_lab_reject_mutation(); END IF;
IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='decision_lab_captures_immutable' AND tgrelid='decision_lab_plan_captures'::regclass) THEN
CREATE TRIGGER decision_lab_captures_immutable BEFORE UPDATE OR DELETE ON decision_lab_plan_captures
FOR EACH ROW EXECUTE FUNCTION decision_lab_reject_mutation(); END IF;
END $$;
"""


def report_markdown(run):
    c = run["config"]
    lines = [
        "# Quantia Decision Lab",
        "",
        f"Run: `{run['replay_run_id']}`",
        f"Modo: `{c['mode']}` · Corte: {c['evaluated_as_of']}",
        "",
        "Son contrafactuales de cartera con capital común; no PnL ejecutado ni prueba causal.",
        "",
        f"Episodios: {len(run['episodes'])}. Solicitudes insuficientes: {len(run['failures'])}.",
        "",
        "| Versión | H | Población | n | n efectivo | PLAN | HOLD | DVA pp | IC95 pp | Estado |",
        "|---|---:|---|---:|---:|---:|---:|---:|---|---|",
    ]

    def pct(x):
        return "N/D" if x is None else f"{100*x:+.3f}"

    for r in run["metrics"]:
        if r["segment"] != "ALL":
            continue
        ci = r["ci"]
        lines.append(
            f"| {r['strategy_version']} | {r['horizon']} | {r['population']} | {r['n']} | {r['n_effective']} | {pct(r['plan_mean'])} | {pct(r['hold_mean'])} | {pct(r['mean'])} | {pct(ci['lower'])} / {pct(ci['upper'])} | {r['interpretation']} |"
        )
    lines.extend(
        [
            "",
            "Calidad baja no participa en inferencia primaria. Un IC que incluye cero no demuestra superioridad.",
            "Segmentaciones exploratorias: múltiples comparaciones; no se promueven reglas ni estrategias.",
            "Cash: ARS nominal sin interés. Retorno PRICE_ONLY no incluye dividendos, inflación ni rendimiento cash.",
            "Las ventanas superpuestas de cuentas no se encadenan en una curva de capital ni en un Sharpe.",
            "",
            "## Calidad y faltantes",
        ]
    )
    quality = {}
    for e in run["episodes"]:
        q = e["quality"]
        key = canonical(q)
        quality[key] = quality.get(key, 0) + 1
    for q, n in sorted(quality.items()):
        value = json.loads(q)
        lines.append(
            f"- {n} episodios: {value['level']}; faltantes: {', '.join(value['missing_fields']) or 'ninguno'}; supuestos: {', '.join(value['reconstruction_assumptions']) or 'ninguno'}."
        )
    if run["failures"]:
        lines.append("- Rechazos: " + canonical(run["failures"]))
    lines.extend(["", "## Ejemplos verificables", ""])
    examples = [r for r in run["comparisons"] if r["dva"] is not None][:5]
    for r in examples:
        lines.append(
            f"- {r['as_of']} · {r['episode_id'][:12]} · {r['horizon']}D: PLAN {pct(r['plan_return'])}%, HOLD {pct(r['hold_return'])}%, DVA {pct(r['dva'])} pp; calidad {r['quality']}."
        )
    return "\n".join(lines) + "\n"


def _csv(rows):
    stream = io.StringIO(newline="")
    fields = sorted({k for r in rows for k in r})
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                k: canonical(v) if isinstance(v, (list, dict)) else v
                for k, v in row.items()
            }
        )
    return stream.getvalue().encode("utf-8")


def write_artifacts(root, run, dataset):
    folder = Path(root) / run["replay_run_id"]
    files = {
        "run.json": canonical(run).encode(),
        "inputs.json": canonical(dataset).encode(),
        "report.md": report_markdown(run).encode(),
        "tables/outcomes.csv": _csv(run["outcomes"]),
        "tables/dva.csv": _csv(run["comparisons"]),
        "tables/metrics.csv": _csv(run["metrics"]),
        "analytics-v2-feed.json": canonical(
            {
                "schema": "decision-lab-analytics-feed-v1",
                "run_id": run["replay_run_id"],
                "estimand": "COUNTERFACTUAL_ACCOUNT_DVA_NOT_EXECUTED_PNL",
                "episodes": [
                    {
                        "episode_id": e["episode_id"],
                        "as_of": e["state"]["as_of"],
                        "quality": e["quality"],
                    }
                    for e in run["episodes"]
                ],
                "dva": run["comparisons"],
                "outcomes": run["outcomes"],
                "strategy_comparisons": run["strategy_comparisons"],
            }
        ).encode(),
    }
    manifest = {
        "schema": "decision-lab-manifest-v1",
        "replay_run_id": run["replay_run_id"],
        "evaluated_as_of": run["config"]["evaluated_as_of"],
        "config_hash": digest(run["config"]),
        "input_hashes": run["input_hashes"],
        "implementation": run["implementation"],
        "row_counts": {
            "episodes": len(run["episodes"]),
            "outcomes": len(run["outcomes"]),
            "dva": len(run["comparisons"]),
        },
        "files": {k: hashlib.sha256(v).hexdigest() for k, v in sorted(files.items())},
    }
    files["manifest.json"] = canonical(manifest).encode()
    folder.mkdir(parents=True, exist_ok=True)
    for relative, content in files.items():
        path = folder / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != content:
                raise ValueError("immutable artifact conflict: " + relative)
        else:
            with path.open("xb") as output:
                output.write(content)
    return folder


def validate_artifacts(folder, *, recompute=False):
    folder = Path(folder)
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    for name, expected in manifest["files"].items():
        path = (folder / name).resolve()
        if (
            not path.is_relative_to(folder.resolve())
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ):
            raise ValueError("artifact hash mismatch: " + name)
    run = json.loads((folder / "run.json").read_text(encoding="utf-8"))
    payload = {k: v for k, v in run.items() if k != "replay_run_id"}
    if (
        digest(payload) != run["replay_run_id"]
        or manifest["replay_run_id"] != run["replay_run_id"]
    ):
        raise ValueError("run hash mismatch")
    if recompute:
        from .models import Dataset, StrategySpec, Experiment, CostModel
        from .runner import replay
        from datetime import datetime

        c = run["config"]
        rebuilt, _ = replay(
            Dataset.model_validate_json(
                (folder / "inputs.json").read_text(encoding="utf-8")
            ),
            requests=c["requests"],
            owner=c["owner"],
            strategies=[StrategySpec.model_validate(s) for s in c["strategies"]],
            mode=c["mode"],
            evaluated_as_of=datetime.fromisoformat(c["evaluated_as_of"]),
            experiment=Experiment.model_validate(c["experiment"]),
            costs=CostModel.model_validate(c["costs"]),
            operation=c["operation"],
        )
        if canonical(rebuilt) != canonical(run):
            raise ValueError("recomputed run differs from frozen evidence")
    return {
        "valid": True,
        "replay_run_id": run["replay_run_id"],
        "recomputed": recompute,
    }


async def persist_run(conn, run):
    owner = run["config"]["owner"]
    objects = {}
    refs = {"episodes": [], "outcomes": [], "metrics": []}
    for episode in run["episodes"]:
        ep = json.loads(canonical(episode))
        source = ep["state"].pop("records")
        ep["state"]["evidence_refs"] = []
        for row in source:
            key = digest([owner, "evidence", row])
            objects[key] = (owner, "evidence", row)
            ep["state"]["evidence_refs"].append(key)
        key = digest(ep)
        objects[key] = (owner, "episode", ep)
        refs["episodes"].append(key)
    for field in ("outcomes", "metrics"):
        for row in run[field]:
            key = digest([owner, field, row])
            objects[key] = (owner, field, row)
            refs[field].append(key)
    summary = {
        "schema_version": run["schema_version"],
        "replay_run_id": run["replay_run_id"],
        "comparisons": run["comparisons"],
        "metrics": run["metrics"],
        "strategy_comparisons": run["strategy_comparisons"],
        "failures": run["failures"],
        "alternatives": [
            {
                k: o[k]
                for k in (
                    "episode_id",
                    "alternative",
                    "horizon",
                    "status",
                    "reason",
                    "gross_return",
                    "net_return",
                    "cost_drag",
                    "capital_base_ars",
                    "quality",
                )
            }
            for o in run["outcomes"]
        ],
        "quality": [
            {
                "episode_id": e["episode_id"],
                "as_of": e["state"]["as_of"],
                "quality": e["quality"],
            }
            for e in run["episodes"]
        ],
    }
    async with conn.transaction():
        await conn.executemany(
            "INSERT INTO decision_lab_objects(object_hash,owner_chat_id,kind,payload) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT DO NOTHING",
            [
                (key, value[0], value[1], canonical(value[2]))
                for key, value in sorted(objects.items())
            ],
        )
        existing = await conn.fetch(
            "SELECT object_hash,owner_chat_id,kind,payload FROM decision_lab_objects WHERE object_hash=ANY($1::text[])",
            list(objects),
        )
        for row in existing:
            value = objects[row["object_hash"]]
            if (
                row["owner_chat_id"],
                row["kind"],
                canonical(
                    json.loads(row["payload"])
                    if isinstance(row["payload"], str)
                    else row["payload"]
                ),
            ) != (value[0], value[1], canonical(value[2])):
                raise ValueError("immutable database object conflict")
        from datetime import datetime

        await conn.execute(
            """INSERT INTO decision_lab_runs(replay_run_id,owner_chat_id,evaluated_as_of,config,summary,object_refs,content_hash)
            VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7) ON CONFLICT DO NOTHING""",
            run["replay_run_id"],
            owner,
            datetime.fromisoformat(run["config"]["evaluated_as_of"]),
            canonical(run["config"]),
            canonical(summary),
            canonical(refs),
            digest(run),
        )
        row = await conn.fetchrow(
            "SELECT owner_chat_id,content_hash FROM decision_lab_runs WHERE replay_run_id=$1",
            run["replay_run_id"],
        )
        if row["owner_chat_id"] != owner or row["content_hash"] != digest(run):
            raise ValueError("immutable database run conflict")
