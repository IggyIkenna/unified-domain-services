"""
DateFilterService - Uniform date filtering for instruments.

Provides protocol-specific default dates and filters instruments by
available_from_datetime / available_to_datetime for point-in-time correctness.

Belongs in unified-domain-services: domain validation logic.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _to_utc(dt: datetime) -> datetime:
    """Convert datetime to UTC-aware."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_iso_datetime(s: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime string to UTC-aware datetime."""
    if not s or not isinstance(s, str) or not s.strip():
        return None
    try:
        s_clean = s.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s_clean)
        return _to_utc(dt)
    except (ValueError, TypeError):
        return None


class DateFilterService:
    """
    Service for filtering instruments by date availability.

    Uses protocol-specific default launch dates when instruments lack
    available_from_datetime. Ensures point-in-time correctness for
    historical backfills.
    """

    def __init__(self) -> None:
        """Initialize DateFilterService with protocol defaults."""
        # Protocol default dates (available_from) - from venue_config / protocol launch
        self._protocol_defaults: Dict[str, Dict[str, str]] = {
            "uniswap_v2": {"available_from": "2020-05-18T00:00:00Z"},
            "uniswap_v3": {"available_from": "2021-05-05T00:00:00Z"},
            "uniswap_v4": {"available_from": "2024-11-01T00:00:00Z"},
            "curve": {"available_from": "2020-01-20T00:00:00Z"},
            "aave_v3": {"available_from": "2023-01-27T00:00:00Z"},
            "morpho": {"available_from": "2024-01-08T00:00:00Z"},
            "euler_plasma": {"available_from": "2024-03-01T00:00:00Z"},
            "fluid_plasma": {"available_from": "2024-06-01T00:00:00Z"},
            "aave_plasma": {"available_from": "2024-03-01T00:00:00Z"},
            "lido": {"available_from": "2020-12-18T00:00:00Z"},
            "etherfi": {"available_from": "2024-01-01T00:00:00Z"},
            "ethena": {"available_from": "2024-02-16T00:00:00Z"},
        }
        logger.info("DateFilterService initialized")

    def get_protocol_default_date(self, protocol: str, key: str = "available_from") -> Optional[str]:
        """
        Get protocol default date for a given key.

        Args:
            protocol: Protocol name (e.g., uniswap_v3, ethena)
            key: Date key (e.g., available_from, available_to)

        Returns:
            ISO datetime string or None if not configured
        """
        proto_lower = (protocol or "").lower()
        defaults = self._protocol_defaults.get(proto_lower, {})
        return defaults.get(key)

    def set_protocol_default_date(self, protocol: str, key: str, value: str) -> None:
        """
        Set protocol default date (for testing or runtime override).

        Args:
            protocol: Protocol name
            key: Date key (e.g., available_from)
            value: ISO datetime string (e.g., 2024-01-01T00:00:00Z)
        """
        proto_lower = (protocol or "").lower()
        if proto_lower not in self._protocol_defaults:
            self._protocol_defaults[proto_lower] = {}
        self._protocol_defaults[proto_lower][key] = value

    def filter_instruments_by_date(
        self,
        instruments: Dict[str, Any],
        target_date: datetime,
        protocol: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Filter instruments to those available on target_date.

        An instrument is included if:
        - target_date >= available_from (or protocol default when None)
        - target_date <= available_to (when set)
        - No date restriction (both None and no protocol default) -> include

        Args:
            instruments: Dict of instrument_key -> instrument data
            target_date: Date to filter for (naive treated as UTC)
            protocol: Optional protocol for default available_from when missing

        Returns:
            Filtered dict of instruments
        """
        target_utc = _to_utc(target_date)
        result: Dict[str, Any] = {}

        for inst_key, inst_data in instruments.items():
            if not isinstance(inst_data, dict):
                continue

            available_from_str = inst_data.get("available_from_datetime")
            available_to_str = inst_data.get("available_to_datetime")

            if available_from_str:
                available_from = _parse_iso_datetime(available_from_str)
            elif protocol:
                default_str = self.get_protocol_default_date(protocol, "available_from")
                available_from = _parse_iso_datetime(default_str) if default_str else None
            else:
                available_from = None

            available_to = _parse_iso_datetime(available_to_str) if available_to_str else None

            if available_from is not None and target_utc < available_from:
                continue

            if available_to is not None and target_utc > available_to:
                continue

            result[inst_key] = inst_data

        return result
