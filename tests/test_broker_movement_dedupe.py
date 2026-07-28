from src.collector.broker_movements import (
    broker_fills_from_movements,
    broker_movements_from_cocos_payloads,
)


def _movement_row(
    *,
    id_ticket: str,
    has_ticket_pdf: bool,
    description,
    quantity: float,
    price: float,
    amount: float,
) -> dict:
    return {
        "amount": amount,
        "description": description,
        "execution_date": "2026-06-09",
        "has_ticket_pdf": has_ticket_pdf,
        "id_account": 255066,
        "id_currency": "ARS",
        "id_instrument": 2129,
        "id_ticket": id_ticket,
        "instrument_code": "TSM",
        "instrument_type": "CEDEARS",
        "label": "Compra",
        "price": price,
        "quantity": quantity,
        "settlement_date": "2026-06-09",
        "short_name": "Taiwan se",
    }


def test_broker_fills_skip_synthetic_placeholder_when_real_ticket_is_present():
    payload = {
        "tickerMovements": [
            _movement_row(
                id_ticket="",
                has_ticket_pdf=False,
                description=None,
                quantity=3,
                price=71925,
                amount=215775,
            ),
            _movement_row(
                id_ticket="132369915",
                has_ticket_pdf=True,
                description="Compra",
                quantity=4,
                price=71950,
                amount=287800,
            ),
        ]
    }

    movements = broker_movements_from_cocos_payloads([payload])
    fills = broker_fills_from_movements(movements)

    assert [fill.external_fill_id for fill in fills] == ["132369915"]
    assert fills[0].quantity == 4
    assert fills[0].gross_amount_ars == 287800


def test_broker_fills_keep_synthetic_placeholder_until_real_ticket_arrives():
    payload = {
        "tickerMovements": [
            _movement_row(
                id_ticket="",
                has_ticket_pdf=False,
                description=None,
                quantity=3,
                price=71925,
                amount=215775,
            )
        ]
    }

    movements = broker_movements_from_cocos_payloads([payload])
    fills = broker_fills_from_movements(movements)

    assert len(fills) == 1
    assert fills[0].external_fill_id.startswith("synthetic:2026-06-09:TSM:BUY:")
