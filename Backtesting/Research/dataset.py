import json
import os
from bisect import bisect_left
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class FundingObservation:
    symbol: str
    synthetix_block_number: int
    binance_block_number: int
    synthetix_funding_rate: float
    binance_funding_rate: float
    synthetix_price: float
    binance_price: float
    spread: float

    def to_dict(self) -> dict:
        return asdict(self)


def _default_data_root() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "MasterBacktester",
        "historicalDataJSON",
    )


def _load_json(path: str) -> list:
    with open(path, "r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, list):
        raise ValueError(f"Expected a list in {path}")
    return payload


def _normalize_binance_rows(rows: list, symbol: str) -> list:
    normalized = []
    for row in rows:
        block_number = int(row["block_number"])
        funding_rate = float(row["funding_rate"])
        price = float(row.get("markPrice", 0.0))
        normalized.append(
            {
                "symbol": symbol,
                "block_number": block_number,
                "funding_rate": funding_rate,
                "price": price,
            }
        )
    return sorted(normalized, key=lambda item: item["block_number"])


def _normalize_synthetix_rows(rows: list, symbol: str) -> list:
    normalized = []
    for row in rows:
        block_number = int(row["block_number"])
        funding_rate = float(row["funding_rate"])
        price = float(row.get("price", 0.0))
        normalized.append(
            {
                "symbol": symbol,
                "block_number": block_number,
                "funding_rate": funding_rate,
                "price": price,
            }
        )
    return sorted(normalized, key=lambda item: item["block_number"])


def _find_nearest_binance_row(binance_rows: list, target_block: int):
    blocks = [row["block_number"] for row in binance_rows]
    insert_index = bisect_left(blocks, target_block)

    candidates = []
    if insert_index < len(binance_rows):
        candidates.append(binance_rows[insert_index])
    if insert_index > 0:
        candidates.append(binance_rows[insert_index - 1])
    if not candidates:
        return None

    return min(candidates, key=lambda row: abs(row["block_number"] - target_block))


def align_histories(synthetix_rows: list, binance_rows: list, max_block_gap: int = 15000) -> list:
    if not synthetix_rows or not binance_rows:
        return []

    aligned = []
    for snx_row in synthetix_rows:
        nearest_binance = _find_nearest_binance_row(binance_rows, snx_row["block_number"])
        if nearest_binance is None:
            continue

        block_gap = abs(snx_row["block_number"] - nearest_binance["block_number"])
        if block_gap > max_block_gap:
            continue

        spread = snx_row["funding_rate"] - nearest_binance["funding_rate"]
        aligned.append(
            FundingObservation(
                symbol=snx_row["symbol"],
                synthetix_block_number=snx_row["block_number"],
                binance_block_number=nearest_binance["block_number"],
                synthetix_funding_rate=snx_row["funding_rate"],
                binance_funding_rate=nearest_binance["funding_rate"],
                synthetix_price=snx_row["price"],
                binance_price=nearest_binance["price"],
                spread=spread,
            )
        )

    return aligned


def load_aligned_symbol_dataset(symbol: str, data_root: str = None, max_block_gap: int = 15000) -> list:
    data_root = data_root or _default_data_root()
    binance_path = os.path.join(data_root, "Binance", f"{symbol}Historical.json")
    synthetix_path = os.path.join(data_root, "Synthetix", f"{symbol}Historical.json")

    binance_rows = _normalize_binance_rows(_load_json(binance_path), symbol)
    synthetix_rows = _normalize_synthetix_rows(_load_json(synthetix_path), symbol)
    return align_histories(synthetix_rows, binance_rows, max_block_gap=max_block_gap)
