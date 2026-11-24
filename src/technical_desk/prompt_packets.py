"""Allowlisted market projection and scale-qualified anchors for role-output-v1."""
from copy import deepcopy
from datetime import datetime
from decimal import Decimal

from .evidence_capture import content_hash
from .pilot_protocol_v3 import PacketSpec
from .role_contracts import AnchorContext, PacketContext, role_output_schema
from .prompts import assert_private_free


def _select(row, fields):
    return {field:deepcopy(row[field]) for field in fields if field in row}


def _rename_references(value, mapping):
    if isinstance(value, dict):
        return {key:_rename_references(item,mapping) for key,item in value.items()}
    if isinstance(value, (list,tuple)):
        return [_rename_references(item,mapping) for item in value]
    return mapping.get(value,value) if isinstance(value,str) else value


def project_packet(packet, feature_packet, secondary_pivots, secondary_evaluations):
    if packet["operating_mode"] != "SHADOW_ONLY" or feature_packet["mode"] != "SHADOW_ONLY":
        raise ValueError("SHADOW_ONLY_INPUT_REQUIRED")
    view = dict(instrument_id=packet["instrument_id"],
                cuts=dict(market_data_cutoff=packet["market_data_cutoff"], cutoff=packet["cutoff"],
                          history_start=packet["history_start"]), ohlcv={}, features={}, pivots={})
    view["ohlcv"]["D"] = [dict(_select(row,("session_date","bar_start","open","high","low","close")),
        volume=None,relative_volume=row["relative_volume_feature"]["relative_volume"],
        relative_volume_status=row["relative_volume_feature"]["status"],volume_basis="RELATIVE_UNVERIFIED")
        for row in packet["daily"]]
    view["ohlcv"]["W"] = [dict(_select(row,("monday","last_session","open","high","low","close")),
        volume=None,volume_status="ABSOLUTE_WEEKLY_VOLUME_PROHIBITED") for row in packet["weekly"]]
    view["features"]["price_tick"] = str(feature_packet["tick_contract"]["tick_size"])
    view["features"]["volume_ratio_window_sessions"] = 20
    view["features"]["atr_by_timeframe"] = {tf:packet["atr_by_timeframe"][tf]["value"] for tf in ("D","W")}
    view["features"]["elliott_scales"] = {}
    anchors = []
    for tf in ("D","W"):
        base = feature_packet["features"][tf]
        mappings = {}
        pivots = []
        for prefix,source in (("P2",base["confirmed_pivots"]),("P5",secondary_pivots[tf])):
            mapping = {p["id"]:f"{prefix}:{tf}:{p['id']}" for p in source}
            mappings[prefix] = mapping
            for pivot in source:
                p = _select(pivot,("id","index","kind","price","timeframe","pivot_time","confirmed_at","confirmed_index","status"))
                p["id"] = mapping[p["id"]]
                p["scale"] = "2+2" if prefix == "P2" else "5+5"
                pivots.append(p)
                anchors.append(AnchorContext(p["id"],tf,datetime.fromisoformat(p["pivot_time"]),datetime.fromisoformat(p["confirmed_at"])))
        view["pivots"][tf] = pivots
        latest = _select(base["snapshots"][-1],("time","known_at","indicators","structure","range","latest_swing",
                                                "breakout_candidate","breakout_confirmed","open_extrema"))
        view["features"][tf] = _rename_references(dict(latest=latest,missing=base["missing_features"],
                                                     swings=base["swings"]),mappings["P2"])
        view["features"]["elliott_scales"][tf] = {}
        for prefix,result in (("P2",feature_packet["elliott"][tf]["ordinary_impulse"]),
                              ("P5",secondary_evaluations[tf])):
            # Only validated endpoint references and rule states; no random controls or retrospective diagnostics.
            view["features"]["elliott_scales"][tf][prefix] = _rename_references(result,mappings[prefix])
    assert_private_free(view)
    digest = content_hash(view)
    view.update(packet_id=f"market-{digest[:24]}",packet_digest=digest,output_schema=role_output_schema())
    spec = PacketSpec(view["packet_id"],digest,packet["instrument_id"],packet["instrument_id"].split(":")[-1],
        Decimal(view["features"]["price_tick"]),tuple(a.anchor_id for a in anchors),("D","W"),"READY",
        atr_by_timeframe=tuple((tf,Decimal(packet["atr_by_timeframe"][tf]["value"])) for tf in ("D","W")))
    return view, PacketContext(spec,datetime.fromisoformat(packet["cutoff"]),tuple(anchors))
