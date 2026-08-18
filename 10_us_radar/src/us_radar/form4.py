"""Form 4 ownership XML parsing and lightweight SEC detail fetching."""

from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import hashlib
from urllib.parse import urljoin
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from .schema import NormalizedEvent


@dataclass(frozen=True)
class Form4Transaction:
    tx_id: str
    event_id: str
    issuer_ticker: str | None
    owner_name: str | None
    owner_relationship: str | None
    transaction_date: str | None
    transaction_code: str | None
    acquired_disposed: str | None
    shares: float | None
    price: float | None
    value: float | None
    is_open_market: bool


class _XmlLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attrs_dict = {key.lower(): value for key, value in attrs}
        href = attrs_dict.get("href") or ""
        if href.lower().endswith(".xml") and "xsl" not in href.lower():
            self.links.append(href)


def fetch_form4_xml_from_index(index_url: str, user_agent: str, timeout_seconds: int = 20) -> str | None:
    request = Request(index_url, headers={"User-Agent": user_agent, "Accept-Encoding": "identity"})
    with urlopen(request, timeout=timeout_seconds) as response:
        html = response.read().decode("utf-8", errors="replace")
    parser = _XmlLinkParser()
    parser.feed(html)
    if not parser.links:
        return None
    xml_url = urljoin(index_url, parser.links[0])
    request = Request(xml_url, headers={"User-Agent": user_agent, "Accept-Encoding": "identity"})
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_form4_xml(event: NormalizedEvent, xml_text: str) -> list[Form4Transaction]:
    root = ET.fromstring(xml_text)
    issuer_ticker = _text(root, "issuer/issuerTradingSymbol") or event.ticker
    owner_name = _text(root, "reportingOwner/reportingOwnerId/rptOwnerName")
    owner_relationship = _relationship(root)
    txs: list[Form4Transaction] = []

    for node in root.findall(".//nonDerivativeTransaction"):
        date = _text(node, "transactionDate/value")
        code = _text(node, "transactionCoding/transactionCode")
        acquired_disposed = _text(node, "transactionAmounts/transactionAcquiredDisposedCode/value")
        shares = _float(_text(node, "transactionAmounts/transactionShares/value"))
        price = _float(_text(node, "transactionAmounts/transactionPricePerShare/value"))
        value = shares * price if shares is not None and price is not None else None
        is_open_market = code in {"P", "S"}
        material = "|".join(str(part or "") for part in [event.event_id, owner_name, date, code, acquired_disposed, shares, price])
        txs.append(
            Form4Transaction(
                tx_id=hashlib.sha256(material.encode("utf-8")).hexdigest(),
                event_id=event.event_id,
                issuer_ticker=issuer_ticker,
                owner_name=owner_name,
                owner_relationship=owner_relationship,
                transaction_date=date,
                transaction_code=code,
                acquired_disposed=acquired_disposed,
                shares=shares,
                price=price,
                value=value,
                is_open_market=is_open_market,
            )
        )
    return txs


def _text(root: ET.Element, path: str) -> str | None:
    node = root.find(path)
    if node is None or node.text is None:
        return None
    return " ".join(node.text.split())


def _float(value: str | None) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _relationship(root: ET.Element) -> str | None:
    rel = root.find("reportingOwner/reportingOwnerRelationship")
    if rel is None:
        return None
    parts = []
    for child in rel:
        if child.text and child.text.strip() and child.text.strip() != "0":
            parts.append(child.tag)
    return ",".join(parts) or None
