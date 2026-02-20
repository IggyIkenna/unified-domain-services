"""
Date Validation Utilities for Expected Start Dates

This module provides utilities for checking if a date has sufficient data
for processing, based on the expected_start_dates.yaml configuration.

Philosophy:
- Services should SKIP dates with insufficient data rather than FAIL
- This enables clean backfill operations without artificial failures
- Provides clear data availability timeline for downstream services

Usage:
    from unified_domain_services import (
        DateValidator,
        should_skip_date,
    )

    # Quick check
    if should_skip_date("2023-05-23", "CEFI", "BINANCE-FUTURES", "24h"):
        logger.info("Skipping date - insufficient lookback")
        continue

    # Full validator
    validator = DateValidator()
    result = validator.check_date("2023-05-23", "CEFI", "BINANCE-FUTURES", "24h")
    if not result.is_valid:
        logger.info(f"Skipping: {result.reason}")
"""

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import yaml

from unified_cloud_services.utils.lookback import (
    FEATURE_GROUP_LOOKBACK,
    MAX_LOOKBACK_DAYS_BY_TIMEFRAME,
)
from unified_cloud_services.utils.timeframe import TIMEFRAME_TO_SECONDS

logger = logging.getLogger(__name__)


@dataclass
class DateValidationResult:
    """Result of date validation check."""

    is_valid: bool
    reason: str
    earliest_valid_date: Optional[str] = None
    requested_date: Optional[str] = None
    days_until_valid: Optional[int] = None


class DateValidator:
    """
    Validator for checking if dates have sufficient data for processing.

    Uses expected_start_dates.yaml to determine earliest valid dates
    for each category/venue/timeframe combination.
    """

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the validator.

        Args:
            config_path: Path to expected_start_dates.yaml (auto-detected if None)
        """
        self._config: dict = {}
        self._config_path = config_path
        self._loaded = False

    def _load_config(self):
        """Lazy load the config file."""
        if self._loaded:
            return

        # Common locations to search for expected_start_dates.yaml
        possible_paths = [
            Path(__file__).parent.parent.parent.parent.parent
            / "unified-trading-deployment-v2/configs/expected_start_dates.yaml",
            Path("/app/configs/expected_start_dates.yaml"),
            Path.cwd() / "configs/expected_start_dates.yaml",
        ]

        if self._config_path is None:
            for path in possible_paths:
                if path.exists():
                    self._config_path = path
                    break

        if self._config_path and self._config_path.exists():
            with open(self._config_path) as f:
                self._config = yaml.safe_load(f) or {}
            logger.debug(f"Loaded date validation config from {self._config_path}")
        else:
            logger.error(
                "Could not find expected_start_dates.yaml - searched paths: %s",
                [str(p) for p in possible_paths],
            )
            self._config = {}

        self._loaded = True

    def get_earliest_raw_data_date(
        self,
        service: str,
        category: str,
        venue: Optional[str] = None,
    ) -> Optional[str]:
        """
        Get earliest raw data date for a service/category/venue.

        Args:
            service: Service name (e.g., "market-tick-data-handler")
            category: Category (CEFI, TRADFI, DEFI)
            venue: Optional venue name

        Returns:
            Date string (YYYY-MM-DD) or None if not found
        """
        self._load_config()

        service_config = self._config.get(service, {})
        category_config = service_config.get(category, {})

        if venue and "venues" in category_config:
            return category_config["venues"].get(venue)

        return category_config.get("category_start")

    def get_earliest_valid_feature_date(
        self,
        category: str,
        venue: Optional[str] = None,
        timeframe: str = "24h",
    ) -> Optional[str]:
        """
        Get earliest date when features can be computed.

        Args:
            category: Category (CEFI, TRADFI, DEFI)
            venue: Optional venue name (uses per-venue dates if available)
            timeframe: Timeframe string (15s, 1m, 1h, 24h, etc.)

        Returns:
            Date string (YYYY-MM-DD) or None if not found
        """
        self._load_config()

        # Try per-venue dates first
        features_config = self._config.get("features-delta-one-service", {})
        category_config = features_config.get(category, {})

        timeframe_key = f"venues_{timeframe}"
        if venue and timeframe_key in category_config:
            venue_date = category_config[timeframe_key].get(venue)
            if venue_date:
                return venue_date

        # Fall back to category-level computed dates
        earliest_features = self._config.get("earliest_valid_features", {})
        timeframe_dates = earliest_features.get(timeframe, {})

        return timeframe_dates.get(category)

    def get_earliest_valid_ml_date(
        self,
        category: str,
        timeframe: str = "24h",
    ) -> Optional[str]:
        """
        Get earliest date when ML models can be trained.

        Args:
            category: Category (CEFI, TRADFI, DEFI)
            timeframe: Timeframe string (15s, 1m, 1h, 24h, etc.)

        Returns:
            Date string (YYYY-MM-DD) or None if not found
        """
        self._load_config()

        ml_config = self._config.get("ml-training-service", {})
        earliest_ml = ml_config.get("earliest_valid_ml", {})
        timeframe_dates = earliest_ml.get(timeframe, {})

        return timeframe_dates.get(category)

    def calculate_lookback_days(
        self,
        timeframe: str,
        feature_groups: Optional[list] = None,
    ) -> int:
        """
        Calculate required lookback days for given timeframe and feature groups.

        Args:
            timeframe: Timeframe string (15s, 1m, 1h, 24h, etc.)
            feature_groups: List of feature groups (uses max of all if None)

        Returns:
            Number of days needed for lookback
        """
        if feature_groups:
            max_lookback = max(FEATURE_GROUP_LOOKBACK.get(g, 0) for g in feature_groups)
        else:
            max_lookback = max(FEATURE_GROUP_LOOKBACK.values())

        seconds_per_period = TIMEFRAME_TO_SECONDS.get(timeframe, 86400)
        total_seconds = max_lookback * seconds_per_period

        # Round up to days
        return math.ceil(total_seconds / 86400)

    def check_date(
        self,
        date: str,
        category: str,
        venue: Optional[str] = None,
        timeframe: str = "24h",
        service: str = "features-delta-one-service",
    ) -> DateValidationResult:
        """
        Check if a date is valid for processing.

        Args:
            date: Date to check (YYYY-MM-DD)
            category: Category (CEFI, TRADFI, DEFI)
            venue: Optional venue name
            timeframe: Timeframe string
            service: Service name

        Returns:
            DateValidationResult with validation details
        """
        earliest_date = self.get_earliest_valid_feature_date(
            category=category,
            venue=venue,
            timeframe=timeframe,
        )

        if earliest_date is None:
            # Fall back to raw data date + lookback
            raw_date = self.get_earliest_raw_data_date(
                service="market-tick-data-handler",
                category=category,
                venue=venue,
            )

            if raw_date:
                lookback_days = MAX_LOOKBACK_DAYS_BY_TIMEFRAME.get(timeframe, 200)
                raw_dt = datetime.strptime(raw_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                earliest_dt = raw_dt + timedelta(days=lookback_days)
                earliest_date = earliest_dt.strftime("%Y-%m-%d")
            else:
                return DateValidationResult(
                    is_valid=True,
                    reason="No start date configured - allowing processing",
                    requested_date=date,
                )

        # Compare dates
        requested_dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        earliest_dt = datetime.strptime(earliest_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if requested_dt >= earliest_dt:
            return DateValidationResult(
                is_valid=True,
                reason="Date is valid for processing",
                earliest_valid_date=earliest_date,
                requested_date=date,
            )

        days_until_valid = (earliest_dt - requested_dt).days

        return DateValidationResult(
            is_valid=False,
            reason=f"Date {date} is before earliest valid date {earliest_date} "
            f"(need {days_until_valid} more days of data for {timeframe} lookback)",
            earliest_valid_date=earliest_date,
            requested_date=date,
            days_until_valid=days_until_valid,
        )


# Module-level instance for convenience
_validator: Optional[DateValidator] = None


def get_validator() -> DateValidator:
    """Get or create module-level validator instance."""
    global _validator
    if _validator is None:
        _validator = DateValidator()
    return _validator


def should_skip_date(
    date: str,
    category: str,
    venue: Optional[str] = None,
    timeframe: str = "24h",
) -> bool:
    """
    Quick check if a date should be skipped due to insufficient lookback.

    Args:
        date: Date to check (YYYY-MM-DD)
        category: Category (CEFI, TRADFI, DEFI)
        venue: Optional venue name
        timeframe: Timeframe string

    Returns:
        True if date should be skipped, False if valid
    """
    result = get_validator().check_date(
        date=date,
        category=category,
        venue=venue,
        timeframe=timeframe,
    )
    return not result.is_valid


def get_earliest_valid_date(
    category: str,
    venue: Optional[str] = None,
    timeframe: str = "24h",
) -> Optional[str]:
    """
    Get earliest valid date for feature processing.

    Args:
        category: Category (CEFI, TRADFI, DEFI)
        venue: Optional venue name
        timeframe: Timeframe string

    Returns:
        Date string (YYYY-MM-DD) or None
    """
    return get_validator().get_earliest_valid_feature_date(
        category=category,
        venue=venue,
        timeframe=timeframe,
    )
