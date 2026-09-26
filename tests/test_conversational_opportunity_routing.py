from src.agentic.harness.context import ContextSelector
from src.agentic.harness.task import TaskParser


def test_natural_buy_recommendation_routes_to_opportunities():
    for message in (
        "¿Qué opciones me recomiendas comprar?",
        "¿Qué me recomendas comprar?",
        "¿Qué puedo comprar?",
        "¿Qué compro?",
        "Dame opciones para comprar",
    ):
        task = TaskParser().parse(message)
        assert task.intent == "opportunities", message
        assert task.entities == [], message


def test_opportunities_requires_portfolio_and_radar_evidence():
    task = TaskParser().parse("¿Qué me recomendas comprar?")
    plan = ContextSelector().select(
        task,
        {
            "get_portfolio_snapshot",
            "get_decision_evidence",
            "scan_opportunities",
            "get_decision_value_added",
        },
    )
    assert plan.required_tools == ["get_portfolio_snapshot", "scan_opportunities"]
    assert ["get_portfolio_snapshot", "scan_opportunities"] in plan.parallel_groups


def test_named_ticker_buy_question_stays_position_analysis():
    task = TaskParser().parse("¿Conviene comprar NVDA?")
    assert task.intent == "position_analysis"
    assert task.entities == ["NVDA"]
