"""Fixtures are synthetic: no live SEC access is required for these checks."""
from datetime import date

from src.analysis.public_disclosures import parse_sec_form4, recent_form4_filings


SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<ownershipDocument xmlns="http://www.sec.gov/edgar/ownership">
  <issuer><issuerCik>1</issuerCik><issuerTradingSymbol>AMD</issuerTradingSymbol></issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Example Executive</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>CEO</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-10-12</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionPricePerShare><value>12.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-10-13</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>10</value></transactionShares>
        <transactionPricePerShare><value>13.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-10-13</value></transactionDate>
      <transactionCoding><transactionCode>M</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>200</value></transactionShares>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
  <derivativeTable><derivativeTransaction>
    <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
  </derivativeTransaction></derivativeTable>
</ownershipDocument>"""


def test_form4_parser_buy_sell_not_option_exercise():
    result = parse_sec_form4(
        SAMPLE, ticker="AMD", cik="0000000001",
        accession="0000000001-26-000001", filed_on="2026-10-14",
        source_url="https://www.sec.gov/example.xml",
    )
    assert len(result) == 2
    assert [r["direction"] for r in result] == ["BUY", "SELL"]
    assert [r["event_key"].rsplit(":", 1)[-1] for r in result] == ["0", "1"]
    assert result[0]["person_name"] == "Example Executive"
    assert result[0]["person_role"] == "CEO"
    assert result[0]["value_usd"] == "1250.00"
    assert result[1]["value_usd"] == "130.00"
    assert result[0]["occurred_on"] != result[0]["filed_on"]


def test_submission_filter_uses_filing_date_not_transaction_date():
    payload = {"filings": {"recent": {
        "form": ["4", "4/A", "4", "8-K"],
        "accessionNumber": [
            "0000000001-26-000001", "0000000001-26-000002",
            "0000000001-26-000003", "0000000001-26-000004",
        ],
        "filingDate": ["2026-10-15", "2026-10-16", "2026-01-01", "2026-10-17"],
        "primaryDocument": ["xslF345X05/ownership.xml", "ownership2.xml", "old.xml", "form8k.htm"],
    }}}
    filings = recent_form4_filings(payload, cik="0000000001", since=date(2026, 10, 1))
    assert len(filings) == 2
    assert filings[0]["source_url"].endswith("/000000000126000001/ownership.xml")
    assert filings[1]["form"] == "4/A"


def test_other_issuer_transaction_does_not_leak_into_monitored_ticker():
    xml = SAMPLE.replace("<issuerCik>1</issuerCik>", "<issuerCik>2</issuerCik>")
    result = parse_sec_form4(xml, ticker="AMD", cik="0000000001",
                             accession="a", filed_on="2026-10-14", source_url="https://sec.gov")
    assert result == []


def test_reject_non_ownership_xml_and_invalid_shares():
    try:
        parse_sec_form4("<data/>", ticker="AMD", cik="1", accession="a",
                        filed_on="2026-10-14", source_url="https://sec.gov")
    except ValueError:
        pass
    else:
        raise AssertionError("wrong document should not be accepted")
    modified = SAMPLE.replace("<value>100</value>", "<value>NaN</value>")
    events = parse_sec_form4(modified, ticker="AMD", cik="1", accession="a",
                             filed_on="2026-10-14", source_url="https://sec.gov")
    assert len(events) == 1
    assert events[0]["direction"] == "SELL"
