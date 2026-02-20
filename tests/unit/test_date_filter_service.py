"""Unit tests for DateFilterService (UDS)."""

from datetime import datetime, timezone

import pytest

from unified_domain_services import DateFilterService


class TestDateFilterService:
    """Test DateFilterService functionality."""

    @pytest.fixture
    def date_filter_service(self):
        """Create DateFilterService fixture."""
        return DateFilterService()

    @pytest.fixture
    def sample_instruments(self):
        """Create sample instruments for testing."""
        return {
            "INST1": {
                "instrument_key": "INST1",
                "available_from_datetime": "2024-01-01T00:00:00Z",
                "available_to_datetime": None,
            },
            "INST2": {
                "instrument_key": "INST2",
                "available_from_datetime": "2023-01-01T00:00:00Z",
                "available_to_datetime": "2023-12-31T23:59:59Z",
            },
            "INST3": {
                "instrument_key": "INST3",
                "available_from_datetime": None,
                "available_to_datetime": None,
            },
        }

    def test_init(self, date_filter_service):
        """Test DateFilterService initialization."""
        assert date_filter_service._protocol_defaults is not None
        assert "uniswap_v3" in date_filter_service._protocol_defaults
        assert "ethena" in date_filter_service._protocol_defaults

    def test_filter_instruments_by_date_before_launch(self, date_filter_service, sample_instruments):
        """Test filtering instruments before launch date."""
        target_date = datetime(2023, 6, 1, tzinfo=timezone.utc)
        filtered = date_filter_service.filter_instruments_by_date(
            instruments=sample_instruments,
            target_date=target_date,
        )
        assert "INST1" not in filtered
        assert "INST2" in filtered
        assert "INST3" in filtered

    def test_get_protocol_default_date(self, date_filter_service):
        """Test getting protocol default dates."""
        date = date_filter_service.get_protocol_default_date("uniswap_v3", "available_from")
        assert date == "2021-05-05T00:00:00Z"
        date = date_filter_service.get_protocol_default_date("ethena", "available_from")
        assert date == "2024-02-16T00:00:00Z"
        assert date_filter_service.get_protocol_default_date("unknown", "available_from") is None
