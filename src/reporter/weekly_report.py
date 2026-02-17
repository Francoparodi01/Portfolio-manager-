from datetime import datetime, timedelta
from typing import Dict
from pathlib import Path
import logging
import sys
from pathlib import Path as _Path

# Ensure project root is on sys.path so running this file directly works.
_PROJECT_ROOT = _Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    # Preferred absolute imports (works when running as module)
    from src.analyzer.risk_metrics import RiskMetricsCalculator
    from src.analyzer.performance_metrics import PerformanceMetricsCalculator
    from src.analyzer.concentration_metrics import ConcentrationMetricsCalculator
    from src.analyzer.projections import ProjectionsCalculator
    from src.analyzer.health_score import HealthScoreCalculator
    from src.reporter.exporters.excel_exporter import ExcelExporter
except Exception:
    try:
        # Fallback when running from src folder
        from analyzer.risk_metrics import RiskMetricsCalculator
        from analyzer.performance_metrics import PerformanceMetricsCalculator
        from analyzer.concentration_metrics import ConcentrationMetricsCalculator
        from analyzer.projections import ProjectionsCalculator
        from analyzer.health_score import HealthScoreCalculator
        from reporter.exporters.excel_exporter import ExcelExporter
    except Exception:
        # Last resort: relative imports (works when package is imported)
        from ..analyzer.risk_metrics import RiskMetricsCalculator
        from ..analyzer.performance_metrics import PerformanceMetricsCalculator
        from ..analyzer.concentration_metrics import ConcentrationMetricsCalculator
        from ..analyzer.projections import ProjectionsCalculator
        from ..analyzer.health_score import HealthScoreCalculator
        from .exporters.excel_exporter import ExcelExporter
logger = logging.getLogger(__name__)


class WeeklyReporter:
    """
    Genera reporte semanal automático
    
    Estructura:
    1. Resumen Ejecutivo (3 puntos clave)
    2. Estado del Patrimonio
    3. Métricas de Riesgo
    4. Proyecciones (4 y 12 semanas)
    5. Alertas
    6. Recomendaciones Priorizadas
    """
    
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
        
        # Exportadores
        try:
            self.excel_exporter = ExcelExporter(output_dir)
        except Exception:
            self.excel_exporter = None
        
    
    def generate_report(self) -> Dict:
        """
        Genera reporte completo
        
        Returns:
            Dict con todo el contenido del reporte
        """
        logger.info("="*60)
        logger.info("GENERANDO REPORTE SEMANAL DEL COPILOTO")
        logger.info("="*60)
        
        report = {
            'metadata': {
                'fecha_generacion': datetime.utcnow().isoformat(),
                'periodo': 'semanal',
                'version': '2.0'
            },
            'resumen_ejecutivo': self._section_executive_summary(),
            'estado_patrimonio': self._section_wealth_status(),
            'metricas_riesgo': self._section_risk(),
            'metricas_performance': self._section_performance(),
            'concentracion': self._section_concentration(),
            'proyecciones': self._section_projections(),
            'salud_portfolio': self._section_health(),
            'alertas': self._section_alerts(),
            'recomendaciones': self._section_recommendations()
        }
        
        # Exportar
        self._export_report(report)
        
        logger.info("[OK] Reporte semanal generado exitosamente")
        
        return report
    
    def _section_executive_summary(self) -> Dict:
        """
        Resumen ejecutivo: 3 puntos más importantes
        """
        # Latest snapshot
        latest = self.repo.get_portfolio_history(days=1)
        
        if not latest:
            return {'puntos_clave': []}
        
        current_value = latest[0]['total_value']
        
        # Performance semanal
        week_ago = self.repo.get_portfolio_history(days=7)
        
        if len(week_ago) > 1:
            old_value = week_ago[-1]['total_value']
            weekly_change = ((current_value - old_value) / old_value) * 100
        else:
            weekly_change = 0
        
        # Salud
        health = self.health_calc.calculate()
        
        # Top 3 puntos
        points = []
        
        # 1. Estado general
        points.append({
            'titulo': f'Portfolio en estado {health["clasificacion"]}',
            'detalle': f'Score de salud: {health["score_total"]:.0f}/100',
            'tipo': 'info'
        })
        
        # 2. Performance
        if weekly_change > 0:
            points.append({
                'titulo': f'Ganancia semanal: +{weekly_change:.2f}%',
                'detalle': f'Valor actual: ${current_value:,.2f}',
                'tipo': 'positivo'
            })
        else:
            points.append({
                'titulo': f'Pérdida semanal: {weekly_change:.2f}%',
                'detalle': f'Valor actual: ${current_value:,.2f}',
                'tipo': 'negativo'
            })
        
        # 3. Riesgo
        risk = self.risk_calc.calculate_all(window_days=30)
        
        if risk['max_drawdown'] < -15:
            points.append({
                'titulo': f'Drawdown significativo: {risk["max_drawdown"]:.2f}%',
                'detalle': 'Considerar reducir riesgo',
                'tipo': 'warning'
            })
        else:
            points.append({
                'titulo': 'Riesgo controlado',
                'detalle': f'Volatilidad: {risk["volatility"]:.2f}% anual',
                'tipo': 'positivo'
            })
        
        return {
            'puntos_clave': points,
            'valor_actual': current_value,
            'cambio_semanal_pct': weekly_change
        }
    
    def _section_projections(self) -> Dict:
        """
        Proyecciones multi-escenario
        """
        return self.proj_calc.generate_projections(
            weeks_ahead=[4, 12],
            window_days=90
        )
    
    def _section_health(self) -> Dict:
        """
        Score de salud del portfolio
        """
        return self.health_calc.calculate()
    
    def _section_wealth_status(self) -> Dict:
        """Estado del patrimonio: último snapshot y composición simple"""
        latest = self.repo.get_portfolio_history(days=1)
        if not latest:
            return {'total_value': 0.0, 'positions': []}

        snapshot = latest[0]
        positions = []
        try:
            positions = self.repo.get_current_positions() or []
        except Exception:
            positions = []

        return {
            'total_value': float(snapshot.get('total_value', 0.0)),
            'timestamp': snapshot.get('timestamp'),
            'positions': positions
        }

    def _section_risk(self) -> Dict:
        """Métricas de riesgo resumidas"""
        try:
            return self.risk_calc.calculate_all(window_days=90)
        except Exception:
            return {}

    def _section_performance(self) -> Dict:
        """Métricas de performance resumidas"""
        try:
            return self.perf_calc.calculate(window_days=90)
        except Exception:
            return {}

    def _section_concentration(self) -> Dict:
        """Top holdings / concentración"""
        try:
            return self.conc_calc.calculate_top_holdings(top_n=5)
        except Exception:
            return {'top_holdings': []}

    def _section_alerts(self) -> Dict:
        """Genera alertas simples a partir de riesgo y salud"""
        alerts = []
        risk = self._section_risk()
        health = self._section_health()

        max_dd = risk.get('max_drawdown') if isinstance(risk, dict) else None
        score = health.get('score_total') if isinstance(health, dict) else None

        if max_dd is not None and max_dd < -15:
            alerts.append({'tipo': 'riesgo', 'mensaje': f'Drawdown alto: {max_dd:.2f}%'})

        if score is not None and score < 50:
            alerts.append({'tipo': 'salud', 'mensaje': f'Score de salud bajo: {score:.0f}/100'})

        return {'alertas': alerts}

    def _section_recommendations(self) -> Dict:
        """Recomendaciones básicas basadas en alertas y performance"""
        recs = []
        perf = self._section_performance()
        alerts = self._section_alerts().get('alertas', [])

        if perf and perf.get('annual_return', 0) < 0:
            recs.append({'accion': 'review_alloc', 'detalle': 'Revisar asignación por bajo rendimiento anual'})

        for a in alerts:
            if a.get('tipo') == 'riesgo':
                recs.append({'accion': 'reduce_risk', 'detalle': 'Considerar reducir exposición en activos volátiles'})

        return {'recomendaciones': recs}

    def _export_report(self, report: Dict):
        """
        Exporta reporte en múltiples formatos
        """
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        # Excel (conservar exporter actual)
        excel_path = self.excel_exporter.export_weekly_report(report)
        logger.info(f"Reporte Excel: {excel_path}")
        
        # JSON (para IA/ML futuro)
        import json
        json_path = self.output_dir / f'reporte_{timestamp}.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # TXT (para email)
        txt_path = self.output_dir / f'reporte_{timestamp}.txt'
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(self._format_text_report(report))
    
    def _format_text_report(self, report: Dict) -> str:
        """
        Formatea reporte como texto plano
        """
        lines = []
        lines.append("="*60)
        lines.append("REPORTE SEMANAL - COPILOTO INTELIGENTE")
        lines.append("="*60)
        lines.append("")
        lines.append(f"Fecha: {report['metadata']['fecha_generacion']}")
        lines.append("")
        
        # Resumen Ejecutivo
        lines.append("RESUMEN EJECUTIVO")
        lines.append("-"*60)
        for punto in report['resumen_ejecutivo']['puntos_clave']:
            lines.append(f"• {punto['titulo']}")
            lines.append(f"  {punto['detalle']}")
            lines.append("")
        
        # Proyecciones
        lines.append("PROYECCIONES")
        lines.append("-"*60)
        
        proj = report['proyecciones']
        for periodo, escenarios in proj.get('escenarios', {}).items():
            lines.append(f"\n{periodo.upper()}:")
            for nombre, data in escenarios.items():
                lines.append(f"  {nombre:12}: ${data['valor']:,.2f} ({data['cambio_pct']:+.2f}%)")
        
        lines.append("")
        lines.append("="*60)
        
        return "\n".join(lines)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    class FakeRepo:
        def get_portfolio_history(self, days=90):
            from datetime import datetime, timedelta
            hist = []
            now = datetime.utcnow()
            for i in range(days):
                ts = (now - timedelta(days=i)).isoformat()
                value = 100000 * (1 + 0.0005 * i)
                hist.append({'timestamp': ts, 'total_value': value})
            return hist

        def get_current_positions(self):
            return [
                {'ticker': 'AAA', 'value': 50000},
                {'ticker': 'BBB', 'value': 30000},
                {'ticker': 'CCC', 'value': 20000}
            ]

    out_dir = Path.cwd() / 'reports_output'
    out_dir.mkdir(parents=True, exist_ok=True)

    repo = FakeRepo()
    reporter = WeeklyReporter(repo, out_dir)
    report = reporter.generate_report()

    print(f"Reporte generado. Archivos en: {out_dir}")
    try:
        for p in report['resumen_ejecutivo']['puntos_clave']:
            print('-', p.get('titulo'))
    except Exception:
        pass