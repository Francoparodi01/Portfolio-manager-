from datetime import datetime
from typing import Dict
from pathlib import Path
import logging
import sys
from pathlib import Path as _Path

# Ensure project root is on sys.path
_PROJECT_ROOT = _Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from src.analyzer.risk_metrics import RiskMetricsCalculator
    from src.analyzer.performance_metrics import PerformanceMetricsCalculator
    from src.analyzer.concentration_metrics import ConcentrationMetricsCalculator
    from src.analyzer.projections import ProjectionsCalculator
    from src.analyzer.health_score import HealthScoreCalculator
    from src.analyzer.advanced_forecaster import AdvancedForecaster
    from src.reporter.exporters.excel_exporter import ExcelExporter
except Exception:
    from analyzer.risk_metrics import RiskMetricsCalculator
    from analyzer.performance_metrics import PerformanceMetricsCalculator
    from analyzer.concentration_metrics import ConcentrationMetricsCalculator
    from analyzer.projections import ProjectionsCalculator
    from analyzer.health_score import HealthScoreCalculator
    from analyzer.advanced_forecaster import AdvancedForecaster
    from reporter.exporters.excel_exporter import ExcelExporter

logger = logging.getLogger(__name__)


class WeeklyReporter:

    def __init__(self, repository, output_dir: Path):
        self.repo = repository
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Calculadores
        self.risk_calc = RiskMetricsCalculator(repository)
        self.perf_calc = PerformanceMetricsCalculator(repository)
        self.conc_calc = ConcentrationMetricsCalculator(repository)
        self.proj_calc = ProjectionsCalculator(repository)
        self.health_calc = HealthScoreCalculator(repository)
        self.advanced_forecaster = AdvancedForecaster(repository)

        try:
            self.excel_exporter = ExcelExporter(output_dir)
        except Exception:
            self.excel_exporter = None

    # ==========================================================
    # MAIN
    # ==========================================================
    def generate_report(self) -> Dict:

        logger.info("=" * 60)
        logger.info("GENERANDO REPORTE SEMANAL V3.1")
        logger.info("=" * 60)

        # Calcular una sola vez
        forecast = self._section_advanced_forecast()
        risk = self._section_risk()
        health = self._section_health()
        performance = self._section_performance()

        report = {
            "metadata": {
                "fecha_generacion": datetime.utcnow().isoformat(),
                "periodo": "semanal",
                "version": "3.1"
            },
            "resumen_ejecutivo": self._section_executive_summary(
                forecast, risk, health
            ),
            "estado_patrimonio": self._section_wealth_status(),
            "metricas_riesgo": risk,
            "metricas_performance": performance,
            "concentracion": self._section_concentration(),
            "proyecciones": self._section_projections(),
            "advanced_forecast": forecast,
            "salud_portfolio": health,
            "alertas": self._section_alerts(forecast, risk),
            "recomendaciones": self._section_recommendations(forecast)
        }

        self._export_report(report)

        logger.info("[OK] Reporte semanal generado exitosamente")

        return report

    # ==========================================================
    # SECTIONS
    # ==========================================================
    def _section_advanced_forecast(self) -> Dict:
        try:
            return self.advanced_forecaster.run_forecast(
                window_days=180,
                horizon_days=20,
                simulations=10000
            )
        except Exception as e:
            logger.warning(f"Forecast falló: {e}")
            return {}

    def _section_executive_summary(self, forecast: Dict, risk: Dict, health: Dict) -> Dict:

        latest = self.repo.get_portfolio_history(days=1)
        if not latest:
            return {"puntos_clave": []}

        current_value = latest[0]["total_value"]

        week_ago = self.repo.get_portfolio_history(days=7)
        if len(week_ago) > 1:
            old_value = week_ago[-1]["total_value"]
            weekly_change = ((current_value - old_value) / old_value) * 100
        else:
            weekly_change = 0

        points = []

        # Estado general
        points.append({
            "titulo": f"Portfolio en estado {health.get('clasificacion', 'N/A')}",
            "detalle": f"Score de salud: {health.get('score_total', 0):.0f}/100",
            "tipo": "info"
        })

        # Performance semanal
        if weekly_change >= 0:
            tipo = "positivo"
            titulo = f"Ganancia semanal: +{weekly_change:.2f}%"
        else:
            tipo = "negativo"
            titulo = f"Pérdida semanal: {weekly_change:.2f}%"

        points.append({
            "titulo": titulo,
            "detalle": f"Valor actual: ${current_value:,.2f}",
            "tipo": tipo
        })

        # Forecast probabilístico
        prob_loss = forecast.get("probabilidad_perdida")

        if prob_loss is not None:
            if prob_loss > 0.6:
                tipo = "warning"
                titulo = "Alta probabilidad de pérdida futura"
            elif prob_loss > 0.4:
                tipo = "info"
                titulo = "Riesgo estadístico moderado"
            else:
                tipo = "positivo"
                titulo = "Probabilidad de pérdida baja"

            points.append({
                "titulo": titulo,
                "detalle": f"Probabilidad pérdida 20d: {prob_loss*100:.1f}%",
                "tipo": tipo
            })

        return {
            "puntos_clave": points,
            "valor_actual": current_value,
            "cambio_semanal_pct": weekly_change
        }

    def _section_wealth_status(self) -> Dict:
        latest = self.repo.get_portfolio_history(days=1)
        if not latest:
            return {"total_value": 0.0}

        snapshot = latest[0]
        return {
            "total_value": float(snapshot.get("total_value", 0.0)),
            "timestamp": snapshot.get("timestamp")
        }

    def _section_risk(self):
        try:
            return self.risk_calc.calculate_all(window_days=90)
        except Exception:
            return {}

    def _section_performance(self):
        try:
            return self.perf_calc.calculate(window_days=90)
        except Exception:
            return {}

    def _section_concentration(self):
        try:
            return self.conc_calc.calculate_top_holdings(top_n=5)
        except Exception:
            return {"top_holdings": []}

    def _section_projections(self):
        try:
            return self.proj_calc.generate_projections(
                weeks_ahead=[4, 12],
                window_days=90
            )
        except Exception:
            return {}

    def _section_health(self):
        try:
            return self.health_calc.calculate()
        except Exception:
            return {}

    def _section_alerts(self, forecast: Dict, risk: Dict):

        alerts = []

        max_dd = risk.get("max_drawdown")
        if max_dd is not None and max_dd < -15:
            alerts.append({
                "tipo": "riesgo",
                "mensaje": f"Drawdown alto: {max_dd:.2f}%"
            })

        prob_loss = forecast.get("probabilidad_perdida")
        if prob_loss is not None and prob_loss > 0.6:
            alerts.append({
                "tipo": "forecast",
                "mensaje": f"Alta probabilidad de pérdida (20d): {prob_loss*100:.1f}%"
            })

        return {"alertas": alerts}

    def _section_recommendations(self, forecast: Dict):

        recs = []

        prob_loss = forecast.get("probabilidad_perdida")
        if prob_loss is not None and prob_loss > 0.6:
            recs.append({
                "accion": "reduce_risk",
                "detalle": "Reducir exposición en activos volátiles"
            })

        expected_return = forecast.get("expected_return_pct")
        if expected_return is not None and expected_return < 0:
            recs.append({
                "accion": "review_strategy",
                "detalle": "Revisar estrategia por expectativa negativa"
            })

        return {"recomendaciones": recs}

    # ==========================================================
    # EXPORT
    # ==========================================================
    def _export_report(self, report: Dict):

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        if self.excel_exporter:
            excel_path = self.excel_exporter.export_weekly_report(report)
            logger.info(f"Reporte Excel: {excel_path}")

        import json
        json_path = self.output_dir / f"reporte_{timestamp}.json"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    from src.storage.postgres_repository import PostgresRepository

    output_dir = Path("/app/reports_output")

    repo = PostgresRepository()
    reporter = WeeklyReporter(repo, output_dir)

    report = reporter.generate_report()

    print("\nReporte generado correctamente")
    print("Forecast:", report.get("advanced_forecast", {}))