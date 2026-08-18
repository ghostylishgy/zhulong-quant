from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from spark_etf.db.db_init import DEFAULT_DB_PATH, init_db

COST_BASIS_TYPES = {"tiantian_position_cost", "diluted_cost", "unknown"}
FUND_CODE_RE = re.compile(r"(?P<code>\d{6}(?:\.OF)?)")
NUMBER_RE = re.compile(r"[-+]?\d+(?:,\d{3})*(?:\.\d+)?")


@dataclass(slots=True)
class WeeklyPositionInput:
    fund_code: str
    fund_name: str
    shares: float
    unit_cost: float
    note: str = ""


class WeeklySnapshotService:
    def __init__(self, db_path: Path | str | None = None) -> None:
        self.db_path = init_db(db_path or DEFAULT_DB_PATH)

    def import_text(
        self,
        snapshot_date: date,
        text: str,
        cost_basis_type: str,
        source: str = "manual_text",
        source_path: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        if cost_basis_type not in COST_BASIS_TYPES:
            raise ValueError(f"cost_basis_type must be one of {sorted(COST_BASIS_TYPES)}")

        positions = self.parse_text(text)
        if not positions:
            raise ValueError("No valid fund position rows were parsed")

        batch_id = str(uuid4())
        input_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        warnings = self._share_change_warnings(snapshot_date, positions)

        conn = duckdb.connect(str(self.db_path))
        try:
            conn.execute("BEGIN")
            conn.execute(
                """
                INSERT INTO spark_import_batches (
                    batch_id, source, source_path, input_text_hash, cost_basis_type, note
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [batch_id, source, source_path, input_hash, cost_basis_type, note],
            )
            for item in positions:
                source_hash = self._source_hash(snapshot_date, item, cost_basis_type)
                conn.execute(
                    """
                    DELETE FROM spark_position_weekly_snapshot
                    WHERE snapshot_date = ? AND fund_code = ?
                    """,
                    [snapshot_date, item.fund_code],
                )
                conn.execute(
                    """
                    INSERT INTO spark_position_weekly_snapshot (
                        snapshot_id, snapshot_date, fund_code, fund_name, shares, unit_cost,
                        cost_basis_type, source, source_batch_id, source_hash, note
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(uuid4()),
                        snapshot_date,
                        item.fund_code,
                        item.fund_name,
                        item.shares,
                        item.unit_cost,
                        cost_basis_type,
                        source,
                        batch_id,
                        source_hash,
                        item.note,
                    ],
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        return {
            "batch_id": batch_id,
            "snapshot_date": snapshot_date.isoformat(),
            "count": len(positions),
            "warnings": warnings,
            "positions": [asdict(item) for item in positions],
        }

    def latest_snapshot_date(self) -> date | None:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = conn.execute("SELECT max(snapshot_date) FROM spark_position_weekly_snapshot").fetchone()
        finally:
            conn.close()
        return row[0] if row and row[0] is not None else None

    def load_snapshot(self, snapshot_date: date | None = None) -> list[WeeklyPositionInput]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            if snapshot_date is None:
                row = conn.execute("SELECT max(snapshot_date) FROM spark_position_weekly_snapshot").fetchone()
                if row is None or row[0] is None:
                    return []
                snapshot_date = row[0]
            rows = conn.execute(
                """
                SELECT fund_code, fund_name, shares, unit_cost, coalesce(note, '')
                FROM spark_position_weekly_snapshot
                WHERE snapshot_date = ?
                ORDER BY fund_code
                """,
                [snapshot_date],
            ).fetchall()
        finally:
            conn.close()
        return [WeeklyPositionInput(str(r[0]), str(r[1]), float(r[2]), float(r[3]), str(r[4])) for r in rows]

    @staticmethod
    def parse_text(text: str) -> list[WeeklyPositionInput]:
        positions: list[WeeklyPositionInput] = []
        for line_no, raw_line in enumerate(text.splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                positions.append(WeeklySnapshotService.parse_line(line))
            except ValueError as exc:
                raise ValueError(f"Line {line_no}: {exc}: {raw_line}") from exc
        return positions

    @staticmethod
    def parse_line(line: str) -> WeeklyPositionInput:
        normalized = line.replace("，", " ").replace(",", " ").replace("：", ":")
        match = FUND_CODE_RE.search(normalized)
        if not match:
            raise ValueError("missing fund code")

        code = match.group("code")
        if "." not in code:
            code = f"{code}.OF"

        shares = WeeklySnapshotService._extract_marked_number(normalized, ("份额", "shares"))
        unit_cost = WeeklySnapshotService._extract_marked_number(normalized, ("持仓成本", "成本", "unit_cost", "cost"))

        if shares is None or unit_cost is None:
            numbers = [float(item.replace(",", "")) for item in NUMBER_RE.findall(normalized)]
            numbers = [num for num in numbers if abs(num - float(code[:6])) > 1e-9]
            if len(numbers) < 2:
                raise ValueError("missing shares or unit cost")
            shares = numbers[-2] if shares is None else shares
            unit_cost = numbers[-1] if unit_cost is None else unit_cost

        marker_positions = [
            pos
            for marker in ("份额", "shares", "持仓成本", "成本", "unit_cost", "cost")
            for pos in [normalized.lower().find(marker.lower(), match.end())]
            if pos >= 0
        ]
        name_end = min(marker_positions) if marker_positions else len(normalized)
        name = normalized[match.end() : name_end].strip()
        name = re.sub(r"\s+", " ", name) or code

        if shares < 0:
            raise ValueError("shares must be non-negative")

        return WeeklyPositionInput(fund_code=code, fund_name=name, shares=shares, unit_cost=unit_cost)

    @staticmethod
    def _extract_marked_number(text: str, markers: tuple[str, ...]) -> float | None:
        for marker in markers:
            pattern = re.compile(rf"{re.escape(marker)}\s*[:=]?\s*({NUMBER_RE.pattern})", re.IGNORECASE)
            match = pattern.search(text)
            if match:
                return float(match.group(1).replace(",", ""))
        return None

    @staticmethod
    def _source_hash(snapshot_date: date, item: WeeklyPositionInput, cost_basis_type: str) -> str:
        src = f"{snapshot_date.isoformat()}|{item.fund_code}|{item.shares:.8f}|{item.unit_cost:.8f}|{cost_basis_type}"
        return hashlib.sha256(src.encode("utf-8")).hexdigest()

    def _share_change_warnings(self, snapshot_date: date, positions: list[WeeklyPositionInput]) -> list[dict[str, Any]]:
        conn = duckdb.connect(str(self.db_path), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT fund_code, shares, snapshot_date
                FROM spark_position_weekly_snapshot
                WHERE snapshot_date < ?
                  AND snapshot_date = (
                      SELECT max(snapshot_date)
                      FROM spark_position_weekly_snapshot
                      WHERE snapshot_date < ?
                  )
                """,
                [snapshot_date, snapshot_date],
            ).fetchall()
        finally:
            conn.close()

        prev = {str(code): (float(shares), prev_date) for code, shares, prev_date in rows}
        warnings: list[dict[str, Any]] = []
        for item in positions:
            old = prev.get(item.fund_code)
            if old is None:
                continue
            prev_shares, prev_date = old
            if prev_shares <= 0:
                continue
            ratio = (item.shares - prev_shares) / prev_shares
            if abs(ratio) >= 0.10:
                warnings.append(
                    {
                        "fund_code": item.fund_code,
                        "fund_name": item.fund_name,
                        "previous_snapshot_date": str(prev_date),
                        "previous_shares": prev_shares,
                        "current_shares": item.shares,
                        "change_ratio": round(ratio, 6),
                        "risk_flag": "SHARE_CHANGE_SENTINEL",
                    }
                )
        return warnings


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser(description="Import Spark ETF weekly position snapshot")
    parser.add_argument("--date", required=True, type=_parse_date, help="Snapshot date, YYYY-MM-DD")
    parser.add_argument("--file", type=str, default=None, help="Text file containing weekly positions")
    parser.add_argument("--db", type=str, default=None, help="DuckDB path")
    parser.add_argument(
        "--cost-basis-type",
        required=True,
        choices=sorted(COST_BASIS_TYPES),
        help="Use tiantian_position_cost for Tiantian Fund displayed position cost price",
    )
    parser.add_argument("--note", type=str, default=None)
    args = parser.parse_args()

    if args.file:
        path = Path(args.file)
        text = path.read_text(encoding="utf-8")
        source_path = str(path)
    else:
        import sys

        text = sys.stdin.read()
        source_path = None

    service = WeeklySnapshotService(db_path=args.db)
    result = service.import_text(
        snapshot_date=args.date,
        text=text,
        cost_basis_type=args.cost_basis_type,
        source="manual_text",
        source_path=source_path,
        note=args.note,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
