import warnings

import pytest

from src.analysis.decision_engine import FinalAction
from src.analysis.enums import DecisionType
from src.analysis.execution_planner import Action


def test_final_action_maps_to_decision_type():
    assert FinalAction.BUY.to_decision_type() == DecisionType.BUY
    assert FinalAction.SELL.to_decision_type() == DecisionType.SELL


def test_action_maps_to_decision_type():
    assert Action.SELL_PARTIAL.to_decision_type() == DecisionType.SELL_PARTIAL


def test_final_action_emits_deprecation_warning():
    with pytest.warns(DeprecationWarning, match="FinalAction"):
        assert FinalAction("BUY") == FinalAction.BUY


def test_action_emits_deprecation_warning():
    with pytest.warns(DeprecationWarning, match="Action"):
        assert Action("SELL_PARTIAL") == Action.SELL_PARTIAL


def test_decision_type_is_exhaustive():
    final_action_targets = {item.to_decision_type() for item in FinalAction}
    action_targets = {item.to_decision_type() for item in Action}
    assert final_action_targets | action_targets <= set(DecisionType)
