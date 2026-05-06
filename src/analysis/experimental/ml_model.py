"""
src/analysis/ml_model.py

Capa ML: LightGBM calibrado + EV + model registry.

Responsabilidades:
  - Entrenar clasificador de target_hit vs stop_hit
  - Calibrar probabilidades con isotonic regression
  - Calcular expected value por señal
  - Versionar y promover modelos en ml_model_registry
  - Cargar el modelo activo para inferencia en tiempo real

DISEÑO:
  - Modelo: LightGBM binario (target_hit = 1 / 0)
  - Calibración: CalibratedClassifierCV con method='isotonic'
  - EV = prob_target * target_pct - prob_stop * abs(stop_loss_pct)
  - Walk-forward: train hasta T-30d, validar en T-30d..T
  - Promotion gate: brier_score < baseline Y ev_mean > 0
  - Artefacto: joblib .pkl en /models/ (volumen Docker)
  - Sin redes neuronales: dataset demasiado pequeño todavía

INTEGRACIÓN:
  - decision_engine llama predict() antes de grabar la decisión
  - update_outcomes llama fill_labels_for_closed() + retrain si hay suficientes datos
  - verdict_engine usa prob_target_hit y ev para calibrar veredictos
"""

from __future__ import annotations

import json
import logging
import os
import pickle
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss  

from src.core.logger import get_logger
from src.analysis.feature_builder import FEATURE_COLUMNS, LABEL_COLUMN

logger = get_logger(__name__)

UTC = timezone.utc

# Path base para artefactos de modelos (volumen Docker)
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/models"))
MODELS_DIR.mkdir(parents=True, exist_ok=True)

MODEL_TYPE = "target_hit_lgbm"

# Gate de promoción
PROMOTION_MIN_BRIER_IMPROVEMENT = 0.005   # debe mejorar al menos 0.005 vs baseline
PROMOTION_MIN_EV_MEAN           = 0.0     # EV medio debe ser positivo
PROMOTION_MIN_SAMPLES           = 30      # mínimo de muestras para entrenar


class MLModel:
    """
    Wrapper del modelo LightGBM calibrado.

    Uso típico (inferencia):
        model = await MLModel.load_active(pool)
        if model:
            result = model.predict(features_dict)
            # result.prob_target_hit, result.expected_value, result.confidence_label
    """

    def __init__(self, pipeline, feature_names: list[str], version: str):
        self.pipeline      = pipeline   # sklearn Pipeline con LightGBM + calibración
        self.feature_names = feature_names
        self.version       = version

    def predict(self, features: dict) -> "PredictionResult":
        """
        Genera predicción para una señal.

        features: dict con las mismas keys que FEATURE_COLUMNS.
        Valores None se reemplazan por la mediana del entrenamiento
        (el imputer en el pipeline lo maneja).
        """
        row = pd.DataFrame([{
            col: features.get(col) for col in self.feature_names
        }])

        try:
            prob_target = float(self.pipeline.predict_proba(row)[0, 1])
        except Exception as e:
            logger.warning("predict: error en pipeline: %s", e)
            return PredictionResult(
                prob_target_hit=None,
                expected_value=None,
                confidence_label="SIN_MODELO",
                model_version=self.version,
            )

        # EV = prob_target * target_pct + prob_stop * stop_loss_pct
        # stop_loss_pct es negativo, así que EV puede ser negativo
        sl_pct  = features.get("stop_loss_pct") or -0.05   # fallback -5%
        tgt_pct = features.get("target_pct")    or  0.15   # fallback +15%
        prob_stop = 1.0 - prob_target
        ev = prob_target * abs(tgt_pct) - prob_stop * abs(sl_pct)

        label = _confidence_label(prob_target, ev)

        return PredictionResult(
            prob_target_hit=round(prob_target, 4),
            expected_value=round(ev, 4),
            confidence_label=label,
            model_version=self.version,
        )

    @staticmethod
    async def load_active(pool) -> Optional["MLModel"]:
        """
        Carga el modelo activo desde el registry.
        Retorna None si no hay modelo activo o si el artefacto no existe.
        """
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT version, artifact_path, feature_names
                FROM ml_model_registry
                WHERE model_type = $1
                  AND is_active = TRUE
                LIMIT 1
                """,
                MODEL_TYPE,
            )

        if not row:
            logger.info("MLModel: sin modelo activo en registry")
            return None

        path = Path(row["artifact_path"])
        if not path.exists():
            logger.warning("MLModel: artefacto no encontrado: %s", path)
            return None

        with open(path, "rb") as f:
            pipeline = pickle.load(f)

        feature_names = json.loads(row["feature_names"]) if row["feature_names"] else FEATURE_COLUMNS

        logger.info("MLModel cargado: version=%s", row["version"])
        return MLModel(pipeline, feature_names, row["version"])

    def save(self, version: str) -> Path:
        path = MODELS_DIR / f"{MODEL_TYPE}_{version}.pkl"
        with open(path, "wb") as f:
            pickle.dump(self.pipeline, f)
        return path


class PredictionResult:
    def __init__(
        self,
        prob_target_hit:  Optional[float],
        expected_value:   Optional[float],
        confidence_label: str,
        model_version:    str,
    ):
        self.prob_target_hit  = prob_target_hit
        self.expected_value   = expected_value
        self.confidence_label = confidence_label
        self.model_version    = model_version

    def to_dict(self) -> dict:
        return {
            "prob_target_hit":  self.prob_target_hit,
            "expected_value":   self.expected_value,
            "confidence_label": self.confidence_label,
            "model_version":    self.model_version,
        }


# ── Entrenamiento ─────────────────────────────────────────────────

class ModelTrainer:
    """
    Entrena, evalúa y promueve el modelo LightGBM calibrado.

    Flujo:
      1. build_training_set() → DataFrame
      2. walk_forward_eval() → métricas de validación
      3. train_final() → pipeline completo en todo el set
      4. promote_if_better() → actualiza registry si supera gate
    """

    def __init__(self, pool, val_days: int = 30):
        self.pool     = pool
        self.val_days = val_days  # días reservados para validación

    async def run(self, df: pd.DataFrame) -> Optional[dict]:
        """
        Ejecuta el pipeline completo de entrenamiento.
        Retorna métricas del nuevo modelo o None si no alcanzó el gate.
        """
        from sklearn.pipeline import Pipeline
        from sklearn.impute import SimpleImputer
        from sklearn.preprocessing import StandardScaler
        from sklearn.calibration import CalibratedClassifierCV
        import lightgbm as lgb

        if len(df) < PROMOTION_MIN_SAMPLES:
            logger.warning("ModelTrainer: %d muestras < mínimo %d", len(df), PROMOTION_MIN_SAMPLES)
            return None

        features = [c for c in FEATURE_COLUMNS if c in df.columns]
        X = df[features].copy()
        y = df[LABEL_COLUMN].copy()

        # ── Walk-forward split ────────────────────────────────────
        cutoff = df["captured_at"].max() - pd.Timedelta(days=self.val_days)
        is_train = df["captured_at"] <= cutoff
        is_val   = df["captured_at"] > cutoff

        if is_val.sum() < 5:
            logger.warning(
                "ModelTrainer: menos de 5 muestras en validación. "
                "Usando split 80/20 por filas."
            )
            split_idx = int(len(df) * 0.8)
            is_train = pd.Series([True] * split_idx + [False] * (len(df) - split_idx),
                                  index=df.index)
            is_val = ~is_train

        X_train, y_train = X[is_train], y[is_train]
        X_val,   y_val   = X[is_val],   y[is_val]

        logger.info(
            "ModelTrainer: train=%d val=%d | positivos train=%.1f%% val=%.1f%%",
            len(X_train), len(X_val),
            y_train.mean() * 100, y_val.mean() * 100,
        )

        # ── Pipeline base ─────────────────────────────────────────
        base_lgb = lgb.LGBMClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            num_leaves=15,
            min_child_samples=max(5, int(len(X_train) * 0.05)),
            subsample=0.8,
            colsample_bytree=0.8,
            class_weight="balanced",
            random_state=42,
            verbose=-1,
        )

        pipeline_uncal = Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler",  StandardScaler()),
            ("model",   base_lgb),
        ])

        # Calibración isotónica sobre validación
        calibrated = CalibratedClassifierCV(
            pipeline_uncal,
            method="isotonic",
            cv="prefit",
        )
        pipeline_uncal.fit(X_train, y_train)
        calibrated.fit(X_val, y_val)

        # ── Métricas en validación ────────────────────────────────
        val_metrics = self._evaluate(calibrated, X_val, y_val, df[is_val])
        logger.info("Validación → %s", val_metrics)

        # ── Baseline (modelo de frecuencia base) ──────────────────
        baseline_brier = _brier_baseline(y_val)
        beats_baseline = (
            val_metrics["brier_score"] < baseline_brier - PROMOTION_MIN_BRIER_IMPROVEMENT
            and val_metrics["ev_mean"] > PROMOTION_MIN_EV_MEAN
        )

        logger.info(
            "Baseline brier=%.4f | nuevo=%.4f | beats=%s",
            baseline_brier, val_metrics["brier_score"], beats_baseline,
        )

        # ── Re-entrenar en dataset completo si pasa el gate ───────
        if beats_baseline:
            final_pipeline_uncal = Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler",  StandardScaler()),
                ("model",   lgb.LGBMClassifier(
                    n_estimators=200,
                    max_depth=4,
                    learning_rate=0.05,
                    num_leaves=15,
                    min_child_samples=max(5, int(len(X) * 0.05)),
                    subsample=0.8,
                    colsample_bytree=0.8,
                    class_weight="balanced",
                    random_state=42,
                    verbose=-1,
                )),
            ])
            final_calibrated = CalibratedClassifierCV(
                final_pipeline_uncal, method="isotonic", cv=5
            )
            final_calibrated.fit(X, y)
        else:
            final_calibrated = calibrated

        # ── Guardar y registrar ───────────────────────────────────
        version = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
        model   = MLModel(final_calibrated, features, version)
        path    = model.save(version)

        await self._register_model(
            version=version,
            artifact_path=str(path),
            feature_names=features,
            train_samples=int(is_train.sum()),
            train_start=df[is_train]["captured_at"].min().date(),
            train_end=df[is_train]["captured_at"].max().date(),
            val_samples=int(is_val.sum()),
            val_start=df[is_val]["captured_at"].min().date(),
            val_end=df[is_val]["captured_at"].max().date(),
            metrics=val_metrics,
            baseline_brier=baseline_brier,
            beats_baseline=beats_baseline,
            promoted=beats_baseline,
        )

        if beats_baseline:
            await self._activate_model(version)
            logger.info("✅ Modelo %s promovido como activo", version)
        else:
            logger.info("❌ Modelo %s NO promovido (no supera gate)", version)

        return {
            "version":       version,
            "promoted":      beats_baseline,
            "brier_score":   val_metrics["brier_score"],
            "roc_auc":       val_metrics["roc_auc"],
            "ev_mean":       val_metrics["ev_mean"],
            "baseline_brier": baseline_brier,
            "train_samples": int(is_train.sum()),
            "val_samples":   int(is_val.sum()),
        }

    def _evaluate(self, pipeline, X_val, y_val, df_val: pd.DataFrame) -> dict:
        from sklearn.metrics import brier_score_loss, roc_auc_score  # pyright: ignore[reportMissingModuleSource]

        probs = pipeline.predict_proba(X_val)[:, 1]

        brier = brier_score_loss(y_val, probs)
        try:
            auc = roc_auc_score(y_val, probs)
        except ValueError:
            auc = 0.5

        # EV medio
        sl_pcts  = df_val["stop_loss_pct"].fillna(-0.05).values
        tgt_pcts = df_val["target_pct"].fillna(0.15).values
        evs = probs * abs(tgt_pcts) - (1 - probs) * abs(sl_pcts)
        ev_mean = float(evs.mean())
        ev_positive_rate = float((evs > 0).mean())

        # Precision en cuartil superior
        top_mask = probs >= np.percentile(probs, 75)
        if top_mask.sum() > 0:
            prec_top = float(y_val[top_mask].mean())
        else:
            prec_top = 0.0

        return {
            "brier_score":           round(brier, 6),
            "roc_auc":               round(auc, 6),
            "ev_mean":               round(ev_mean, 4),
            "ev_positive_rate":      round(ev_positive_rate, 4),
            "precision_at_top25pct": round(prec_top, 4),
        }

    async def _register_model(
        self,
        version: str,
        artifact_path: str,
        feature_names: list,
        train_samples: int,
        train_start, train_end,
        val_samples: int,
        val_start, val_end,
        metrics: dict,
        baseline_brier: float,
        beats_baseline: bool,
        promoted: bool,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ml_model_registry (
                    model_type, version, trained_at,
                    train_samples, train_start, train_end,
                    val_samples, val_start, val_end,
                    brier_score, roc_auc,
                    precision_at_top25pct, ev_mean, ev_positive_rate,
                    baseline_brier, beats_baseline,
                    is_active, is_promoted,
                    artifact_path, feature_names,
                    promotion_notes
                ) VALUES (
                    $1, $2, $3,
                    $4, $5, $6,
                    $7, $8, $9,
                    $10, $11, $12, $13, $14,
                    $15, $16,
                    FALSE, $17,
                    $18, $19,
                    $20
                )
                """,
                MODEL_TYPE, version, datetime.now(tz=UTC),
                train_samples, train_start, train_end,
                val_samples, val_start, val_end,
                metrics["brier_score"], metrics["roc_auc"],
                metrics["precision_at_top25pct"],
                metrics["ev_mean"], metrics["ev_positive_rate"],
                baseline_brier, beats_baseline,
                promoted,
                artifact_path,
                json.dumps(feature_names),
                (
                    f"Promovido: brier={metrics['brier_score']:.4f} < baseline={baseline_brier:.4f}"
                    if promoted
                    else f"No promovido: brier={metrics['brier_score']:.4f} >= baseline={baseline_brier:.4f} - {PROMOTION_MIN_BRIER_IMPROVEMENT:.3f}"
                ),
            )

    async def _activate_model(self, version: str) -> None:
        async with self.pool.acquire() as conn:
            # Desactivar cualquier modelo activo anterior
            await conn.execute(
                "UPDATE ml_model_registry SET is_active = FALSE WHERE model_type = $1",
                MODEL_TYPE,
            )
            # Activar el nuevo
            await conn.execute(
                """
                UPDATE ml_model_registry
                SET is_active = TRUE
                WHERE model_type = $1 AND version = $2
                """,
                MODEL_TYPE, version,
            )


# ── Helpers ───────────────────────────────────────────────────────

def _brier_baseline(y_val: pd.Series) -> float:
    """Brier score de un modelo de frecuencia base (pred = mean(y_train))."""
    from sklearn.metrics import brier_score_loss
    base_pred = float(y_val.mean())
    return brier_score_loss(y_val, [base_pred] * len(y_val))


def _confidence_label(prob: float, ev: float) -> str:
    """Etiqueta interpretable para el output del modelo."""
    if ev < 0:
        return "EV_NEGATIVO"
    if prob >= 0.65:
        return "ALTA"
    if prob >= 0.50:
        return "MEDIA"
    if prob >= 0.40:
        return "BAJA"
    return "MUY_BAJA"