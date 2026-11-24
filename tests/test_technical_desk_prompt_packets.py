from copy import deepcopy
from decimal import Decimal

from src.technical_desk.prompt_packets import project_packet
from src.technical_desk.prompts import assert_private_free
from tests.test_technical_desk_prompts import NOW


def test_projection_drops_private_fields_raw_volume_and_retrospective_diagnostics():
    cutoff=NOW.isoformat()
    packet=dict(operating_mode="SHADOW_ONLY",instrument_id="BYMA:CEDEAR:AMD:ARS",cutoff=cutoff,market_data_cutoff=cutoff,
        history_start="2026-01-01",owner_chat_id="PRIVATE_OWNER_SENTINEL",positions=[{"amount":"PRIVATE_AMOUNT_SENTINEL"}],
        volume_basis="RELATIVE_UNVERIFIED",atr_by_timeframe={tf:dict(value="10") for tf in ("D","W")},
        daily=[dict(session_date="2026-01-01",bar_start="2026-01-01T13:30:00+00:00",open="100",high="101",low="99",close="100",
                    volume="PRIVATE_ABSOLUTE_VOLUME",relative_volume_feature=dict(relative_volume="1.1",status="AVAILABLE"))],
        weekly=[dict(monday="2026-01-01",last_session="2026-01-01",open="100",high="101",low="99",close="100",volume=None)])
    feature=dict(mode="SHADOW_ONLY",tick_contract=dict(tick_size="1",capture_hash="PRIVATE_SOURCE_TOKEN"),features={},elliott={},
                 volume_review=dict(private_test="RETROSPECTIVE_CANARY"))
    secondary,evaluations={},{}
    for tf in ("D","W"):
        pivot=dict(id="a"*64,index=1,kind="LOW",price="90",timeframe=tf,pivot_time="2026-01-02T13:30:00+00:00",
                   confirmed_at="2026-01-04T20:00:00+00:00",confirmed_index=3,status="CONFIRMED")
        feature["features"][tf]=dict(confirmed_pivots=[pivot],snapshots=[dict(time=cutoff,known_at=cutoff,indicators={})],missing_features={},swings=[])
        feature["elliott"][tf]=dict(ordinary_impulse=dict(count=0,candidates=[],status_counts={}))
        secondary[tf]=[deepcopy(pivot)]
        evaluations[tf]=dict(candidate_count=0,candidates=[],status_counts={})
    view,context=project_packet(packet,feature,secondary,evaluations)
    assert_private_free(view,forbidden_values=("PRIVATE_OWNER_SENTINEL","PRIVATE_AMOUNT_SENTINEL","PRIVATE_ABSOLUTE_VOLUME","PRIVATE_SOURCE_TOKEN","RETROSPECTIVE_CANARY"))
    assert view["ohlcv"]["D"][0]["volume"] is None and view["ohlcv"]["D"][0]["relative_volume"]=="1.1"
    assert view["ohlcv"]["W"][0]["volume"] is None
    assert len(context.anchors)==len(set(a.anchor_id for a in context.anchors))==4
    assert {a.anchor_id[:2] for a in context.anchors}=={"P2","P5"}


def test_cli_guard_blocks_network_and_every_non_output_database(tmp_path):
    import pytest
    from scripts.build_technical_desk_stage1b import offline_guard
    guard=offline_guard(tmp_path/"output")
    for event in ("socket.connect","socket.getaddrinfo","subprocess.Popen"):
        with pytest.raises(RuntimeError,match="FORBIDDEN"):
            guard(event,())
    for path in (tmp_path/"production.sqlite3",":memory:","postgresql://production"):
        with pytest.raises(RuntimeError,match="ONLY_LOCAL"):
            guard("sqlite3.connect",(str(path),))
    guard("sqlite3.connect",(str(tmp_path/"output/ledger.sqlite3"),))
