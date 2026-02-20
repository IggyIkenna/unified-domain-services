"""Unified Domain Services - Trading domain logic (clients, validation, cloud service wrappers)."""

from unified_cloud_services.domain import (
    FEATURE_GROUP_LOOKBACK,
    MAX_LOOKBACK_DAYS_BY_TIMEFRAME,
    TIMEFRAME_SECONDS,
    TimestampAlignmentResult,
    TimestampDateValidator,
    validate_timestamp_date_alignment,
)
from unified_domain_services.clients import (
    ExecutionDomainClient,
    InstrumentsDomainClient,
    MarketCandleDataDomainClient,
    MarketDataDomainClient,
    MarketTickDataDomainClient,
    create_execution_client,
    create_features_client,
    create_instruments_client,
    create_market_candle_data_client,
    create_market_data_client,
    create_market_tick_data_client,
)
from unified_domain_services.date_validation import (
    DateValidationResult,
    DateValidator,
    get_earliest_valid_date,
    get_validator,
    should_skip_date,
)
from unified_domain_services.factories import (
    create_backtesting_cloud_service,
    create_features_cloud_service,
    create_market_data_cloud_service,
    create_strategy_cloud_service,
)
from unified_domain_services.standardized_service import StandardizedDomainCloudService
from unified_domain_services.validation import DomainValidationConfig, DomainValidationService

__all__ = [
    "StandardizedDomainCloudService",
    "DomainValidationService",
    "DomainValidationConfig",
    "TimestampDateValidator",
    "TimestampAlignmentResult",
    "validate_timestamp_date_alignment",
    "DateValidator",
    "DateValidationResult",
    "should_skip_date",
    "get_earliest_valid_date",
    "get_validator",
    "FEATURE_GROUP_LOOKBACK",
    "MAX_LOOKBACK_DAYS_BY_TIMEFRAME",
    "TIMEFRAME_SECONDS",
    "create_market_data_cloud_service",
    "create_features_cloud_service",
    "create_strategy_cloud_service",
    "create_backtesting_cloud_service",
    "InstrumentsDomainClient",
    "MarketCandleDataDomainClient",
    "MarketTickDataDomainClient",
    "ExecutionDomainClient",
    "MarketDataDomainClient",
    "create_instruments_client",
    "create_market_candle_data_client",
    "create_market_tick_data_client",
    "create_execution_client",
    "create_market_data_client",
    "create_features_client",
    "CloudDataProviderBase",
    "FeaturesDataProvider",
    "InstrumentsDataProvider",
    "MarketDataProvider",
]

# Cloud data providers (moved from UCS to avoid circular deps) - re-exported via __all__
from unified_domain_services.cloud_data_provider import (
    CloudDataProviderBase,
    FeaturesDataProvider,
    InstrumentsDataProvider,
    MarketDataProvider,
)  # noqa: F401
