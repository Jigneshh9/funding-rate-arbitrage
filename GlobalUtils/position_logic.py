def parse_bool_flag(value) -> bool:
    """Parse string/bool style flags stored in the trade log."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() == 'true'


def get_signed_position_size(position: dict) -> float:
    """
    Convert a stored position row into a signed size.
    Long exposure is positive, short exposure is negative.
    """
    size = float(position.get('size_in_asset', 0.0))
    side = str(position.get('side', '')).strip().upper()
    return -size if side == 'SHORT' else size


def calculate_relative_delta(positions: list) -> float:
    """
    Relative delta = absolute net exposure divided by total absolute exposure.
    Perfectly hedged pair -> 0.0, fully unhedged -> 1.0.
    """
    signed_sizes = [get_signed_position_size(position) for position in positions]
    total_absolute_notional_value = sum(abs(value) for value in signed_sizes)
    if total_absolute_notional_value == 0:
        return 0.0
    net_delta = sum(signed_sizes)
    return abs(net_delta) / total_absolute_notional_value
