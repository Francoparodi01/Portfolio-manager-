import logging

from src.collector.data.repository import PortfolioRepository
from src.analyzer.risk_metrics import RiskMetricsCalculator
from src.analyzer.projections import ProjectionsCalculator
from src.analyzer.health_score import HealthScoreCalculator
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = load_dotenv("DATABASE_URL", "sqlite:///portfolio.db")


def main():
    repo = PortfolioRepository(DATABASE_URL)

    # =========================
    # RISK ENGINE
    # =========================
    risk_calc = RiskMetricsCalculator(repo)
    risk_metrics = risk_calc.calculate_all(window_days=180)

    # =========================
    # HEALTH SCORE
    # =========================
    health_calc = HealthScoreCalculator(repo)
    health = health_calc.calculate()

    # =========================
    # PROJECTIONS
    # =========================
    proj_calc = ProjectionsCalculator(repo)
    projections = proj_calc.generate_projections(
        weeks_ahead=[4, 12],
        window_days=90
    )

    print("\n========== RESUMEN ANALÍTICO ==========")
    print(f"Volatilidad anualizada: {risk_metrics['volatility']:.2f}%")
    print(f"Máximo Drawdown: {risk_metrics['max_drawdown']:.2f}%")
    print(f"Sharpe Ratio: {risk_metrics['sharpe_ratio']:.2f}")
    print(f"VaR 95%: {risk_metrics['var_95']:.2f}%")
    print(f"Health Score: {health['score_total']:.2f} ({health['clasificacion']})")

    if projections:
        for horizon, scenarios in projections["scenarios"].items():
            base = scenarios["base"]
            stress = scenarios["estres"]

            print(f"\n--- Proyección {horizon} ---")
            print(f"Base: {base['cambio_pct']:.2f}%")
            print(f"Estrés: {stress['cambio_pct']:.2f}%")

    print("=======================================\n")


if __name__ == "__main__":
    main()
