# unified-domain-services - Dependencies

## Library Dependencies

| Library | Why | Import |
|---------|-----|--------|
| unified-cloud-services | StorageClient, validation, lookback constants | `from unified_cloud_services.core.validation import ...` |

## Install Order (CI/Cloud Build)

1. unified-cloud-services (from Artifact Registry)
2. unified-domain-services (self)

## Local Development

```bash
uv pip install -e ../unified-cloud-services
uv pip install -e ".[dev]"
```

## Cloud Build

Resolves unified-cloud-services from Artifact Registry via pip.conf (`unified-libraries`).

## Public API (for downstream services)

- `StandardizedDomainCloudService`, `create_market_data_cloud_service`, `create_features_cloud_service`, etc.
- `validate_timestamp_date_alignment`, `DateValidator`, `should_skip_date`, `get_earliest_valid_date`
- `CloudDataProviderBase`, `InstrumentsDataProvider`, `MarketDataProvider`, `FeaturesDataProvider`
