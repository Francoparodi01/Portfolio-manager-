"""Synthetic PDF text fixtures; no personal data, broker or database access."""
from pathlib import Path
from decimal import Decimal
import sys
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from extract_cocos_tickets import parse_ticket, local_decimal

TEXT = '''BOLETO
Comitente Operacion Liquidacion Numero
9 2026-10-09 2026-10-13 123
Operacion: Compra
2,00 10,00 20,00 1 000 44 2026-10-09 12:18:29
Importe Bruto 20,00
Debito Derechos Mercado Byma 0,10
Comisiones ARS 0,90
IVA 21% 0,21
Importe Neto en ARS 21,21
'''

def test_document_arithmetic_preserves_explicit_costs_and_clock():
    result = parse_ticket(TEXT, '9')
    assert result['status'] == 'VERIFIED_DOCUMENT_ARITHMETIC'
    assert result['document_charges_sum'] == Decimal('1.21')
    assert result['execution_legs'][0]['document_order_number'] == '44'
    assert result['execution_legs'][0]['clock_timezone'] is None
    assert result['economic_pnl_net'] is None

def test_other_owner_never_enters_document_ledger():
    with pytest.raises(ValueError, match='account mismatch'):
        parse_ticket(TEXT, '8')
    with pytest.raises(ValueError, match='account mismatch'):
        parse_ticket(TEXT+'8 2026-10-09 2026-10-13 124\n', '9')

def test_absent_cost_lines_are_not_zero():
    result = parse_ticket('\n'.join(line for line in TEXT.splitlines() if not line.startswith(('Debito','Comisiones','IVA'))), '9')
    assert result['status'] == 'PARTIAL'
    assert 'document_charges_sum' not in result

def test_mixed_currencies_cannot_pass_arithmetic():
    result = parse_ticket(TEXT.replace('Importe Bruto 20,00','Importe Bruto en USD 20,00'), '9')
    assert result['status'] == 'PARTIAL'
    assert result['checks']['conflicting_currency']

def test_unknown_layout_and_duplicate_totals_stay_partial():
    assert parse_ticket('Not a known account document', '9')['status'] == 'PARTIAL'
    assert parse_ticket(TEXT+'Importe Bruto 20,00\n', '9')['status'] == 'PARTIAL'

def test_sell_keeps_negative_quantity_and_positive_displayed_charges():
    text=TEXT.replace('Compra','Venta').replace('2,00 10,00','-2,00 10,00').replace('21,21','18,79')
    result=parse_ticket(text,'9')
    assert result['status']=='VERIFIED_DOCUMENT_ARITHMETIC'
    assert result['execution_legs'][0]['quantity']==Decimal('-2')
    assert result['checks']['gross_charges_net_residual']==0

def test_three_decimal_totals_are_not_rounded_in_place():
    result=parse_ticket(TEXT.replace('21,21','21,208'),'9')
    assert result['net']==Decimal('21.208')
    assert result['checks']['gross_charges_net_residual']==Decimal('.002')

def test_localized_numbers_require_declared_format():
    assert local_decimal('1.234,567')==Decimal('1234.567')
    with pytest.raises(ValueError): local_decimal('1234.56')

def test_payment_receipt_dates_keep_original_and_normalized_text():
    result=parse_ticket(TEXT.replace('2026-10-09 2026-10-13','09/10/2026 13/10/2026'),'9')
    assert result['execution_date']=='2026-10-09'
    assert result['execution_date_text']=='09/10/2026'
    assert result['status']=='VERIFIED_DOCUMENT_ARITHMETIC'

def test_explicit_credit_label_reduces_displayed_costs():
    text=TEXT.replace('Operacion: Compra','Operacion: Compra Trading').replace('IVA 21%', 'Credito Derechos Mercado Byma 0,10\nIVA 21%').replace('21,21','21,11')
    result=parse_ticket(text,'9')
    assert result['status']=='VERIFIED_DOCUMENT_ARITHMETIC'
    assert result['document_charges_sum']==Decimal('1.11')
    assert next(c['amount'] for c in result['charges'] if c['label'].startswith('Credito'))==Decimal('.10')

def test_price_quotation_basis_is_not_silently_scaled():
    text=TEXT.replace('2,00 10,00 20,00', '2,00 1.000,00 20,00')
    result=parse_ticket(text,'9')
    assert result['status']=='PARTIAL'
    assert result['checks']['leg_price_residuals']==[Decimal('1980')]

def test_receipt_without_document_number_keeps_account_and_null_identifier():
    result=parse_ticket(TEXT.replace('2026-10-13 123','2026-10-13 ---'),'9')
    assert result['owner_evidence']=='EXPLICIT_PDF_ACCOUNT_MATCH'
    assert result['document_number'] is None
    assert result['document_number_text']=='---'

def test_registration_leg_keeps_missing_order_and_unverified_price_basis():
    text=TEXT.replace('Operacion: Compra','Operacion: Venta Registracion USD').replace('1 000 44','1 - -')
    result=parse_ticket(text,'9')
    assert result['side']=='SELL'
    assert result['execution_legs'][0]['document_order_number'] is None
    assert result['execution_legs'][0]['document_order_number_text']=='-'


@pytest.mark.parametrize('title,kind', [
    ('LIQUIDACION DE RESCATE DE FCI', 'FCI_REDEMPTION'),
    ('LIQUIDACION DE SUSCRIPCION DE FCI', 'FCI_SUBSCRIPTION'),
])
def test_fci_account_does_not_imply_verified_cash_or_dates(title, kind):
    result = parse_ticket(title+'\nBuenos Aires, 26/06/2024\nUso de firma: Indistinto Número de cuenta: 9', '9')
    assert result['owner_evidence'] == 'EXPLICIT_PDF_ACCOUNT_MATCH'
    assert result['document_type'] == kind
    assert result['status'] == 'PARTIAL'
    assert result['net'] is None and result['execution_date'] is None
    assert result['charges'] == [] and 'document_charges_sum' not in result


def test_fci_cross_account_and_unrecognized_title_are_not_accepted():
    title = 'LIQUIDACION DE RESCATE DE FCI\n'
    with pytest.raises(ValueError, match='account mismatch'):
        parse_ticket(title+'Número de cuenta: 8', '9')
    with pytest.raises(ValueError, match='account mismatch'):
        parse_ticket(title+'Número de cuenta: 9\nNúmero de cuenta: 8', '9')
    assert parse_ticket('Unknown title\nNúmero de cuenta: 9', '9')['owner_evidence'] == 'ACCOUNT_NOT_PARSED'
