"""
Cloud Data Provider Base

Provides a centralized base class for cloud data access across services.
Each service can extend this class for domain-specific functionality.

Domains supported:
- instruments: Instrument definitions and availability
- market_data: Tick data, candles, HFT features
- features: Computed features (delta-one, volatility, onchain)
- ml: ML models and predictions
"""

import logging
import os
from abc import ABC
from datetime import datetime
from typing import Optional

import pandas as pd

from unified_cloud_services.core.cloud_config import CloudTarget
from unified_cloud_services.core.config import get_config
from unified_cloud_services.core.market_category import get_bucket_for_category
from unified_domain_services.standardized_service import (
    StandardizedDomainCloudService,
)

logger = logging.getLogger(__name__)


class CloudDataProviderBase(ABC):
    """
    Base class for cloud data providers.

    Provides common functionality for:
    - GCS read/write operations
    - BigQuery queries
    - Category-specific bucket selection
    - Test mode detection

    Services should extend this class and add domain-specific methods.
    """

    def __init__(
        self,
        domain: str,
        cloud_target: Optional[CloudTarget] = None,
        project_id: Optional[str] = None,
        gcs_bucket: Optional[str] = None,
        bigquery_dataset: Optional[str] = None,
        bigquery_location: Optional[str] = None,
    ):
        """
        Initialize cloud data provider.

        Args:
            domain: Domain name (e.g., 'instruments', 'market_data', 'features', 'ml')
            cloud_target: Optional CloudTarget configuration (auto-detects if not provided)
            project_id: GCP project ID (overrides cloud_target)
            gcs_bucket: GCS bucket name (overrides cloud_target)
            bigquery_dataset: BigQuery dataset name (overrides cloud_target)
            bigquery_location: BigQuery location (overrides cloud_target)
        """
        self.domain = domain

        # Build cloud target from parameters or defaults
        if cloud_target is None:
            cloud_target = CloudTarget(
                project_id=project_id or get_config("GCP_PROJECT_ID", "central-element-323112"),
                gcs_bucket=gcs_bucket or get_config("GCS_BUCKET", f"{domain}-store"),
                bigquery_dataset=bigquery_dataset or get_config("BIGQUERY_DATASET", domain),
                bigquery_location=bigquery_location or get_config("BIGQUERY_LOCATION", "asia-northeast1"),
            )

        # Create domain cloud service
        self.cloud_service = StandardizedDomainCloudService(
            domain=domain,
            cloud_target=cloud_target,
        )
        self.cloud_target = cloud_target

        logger.info(
            f"✅ CloudDataProviderBase initialized: "
            f"domain={domain}, project={cloud_target.project_id}, "
            f"bucket={cloud_target.gcs_bucket}, dataset={cloud_target.bigquery_dataset}"
        )

    @property
    def is_test_mode(self) -> bool:
        """Check if running in test mode."""
        environment = get_config("ENVIRONMENT", "development").lower()
        return (
            environment in ["test", "testing"]
            or "pytest" in os.environ.get("_", "")
            or get_config("PYTEST_CURRENT_TEST", "") != ""
        )

    def download_from_gcs(
        self,
        gcs_path: str,
        format: str = "parquet",
        log_errors: bool = True,
    ) -> pd.DataFrame:
        """
        Download data from GCS.

        Args:
            gcs_path: Path within the bucket
            format: File format ('parquet', 'csv', 'json')
            log_errors: Whether to log errors

        Returns:
            DataFrame with downloaded data
        """
        try:
            logger.info(f"📥 Loading from GCS: {self.cloud_target.gcs_bucket}/{gcs_path}")
            df = self.cloud_service.download_from_gcs(
                gcs_path=gcs_path,
                format=format,
                log_errors=log_errors,
            )

            if df.empty:
                logger.warning(f"⚠️ No data found at {gcs_path}")
            else:
                logger.info(f"✅ Loaded {len(df)} rows from GCS")

            return df

        except Exception as e:
            error_msg = str(e)
            # Handle 404/Not Found gracefully
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info(f"ℹ️ No data found (404): {gcs_path}")
                return pd.DataFrame()

            if log_errors:
                logger.error(f"❌ Failed to load from GCS: {e}")
            return pd.DataFrame()

    def download_from_category_bucket(
        self,
        gcs_path: str,
        category: str,
        format: str = "parquet",
    ) -> pd.DataFrame:
        """
        Download data from a category-specific bucket.

        Args:
            gcs_path: Path within the bucket
            category: Market category ('CEFI', 'TRADFI', 'DEFI')
            format: File format

        Returns:
            DataFrame with downloaded data
        """
        try:
            # Get bucket for category
            category_bucket = get_bucket_for_category(category, test_mode=self.is_test_mode)

            # Create cloud service for category bucket
            category_target = CloudTarget(
                project_id=self.cloud_target.project_id,
                gcs_bucket=category_bucket,
                bigquery_dataset=self.cloud_target.bigquery_dataset,
                bigquery_location=self.cloud_target.bigquery_location,
            )
            category_service = StandardizedDomainCloudService(
                domain=self.domain,
                cloud_target=category_target,
            )

            logger.info(f"📥 Loading {category} data from: {category_bucket}/{gcs_path}")
            df = category_service.download_from_gcs(
                gcs_path=gcs_path,
                format=format,
                log_errors=False,
            )

            if df.empty:
                logger.warning(f"⚠️ No {category} data found at {category_bucket}/{gcs_path}")
            else:
                logger.info(f"✅ Loaded {len(df)} {category} rows from GCS")

            return df

        except Exception as e:
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg or "No such object" in error_msg:
                logger.info(f"ℹ️ No {category} data found (404): {gcs_path}")
                return pd.DataFrame()

            logger.error(f"❌ Failed to load {category} data from GCS: {e}")
            return pd.DataFrame()

    def query_bigquery(
        self,
        query: str,
        parameters: Optional[dict] = None,
    ) -> pd.DataFrame:
        """
        Execute a BigQuery query.

        Args:
            query: SQL query string
            parameters: Query parameters

        Returns:
            DataFrame with query results
        """
        try:
            logger.info("📥 Executing BigQuery query")
            result = self.cloud_service.query_bigquery(
                query=query,
                parameters=parameters,
            )
            logger.info(f"✅ Query returned {len(result)} rows")
            return result

        except Exception as e:
            logger.error(f"❌ BigQuery query failed: {e}")
            return pd.DataFrame()

    def upload_to_gcs(
        self,
        df: pd.DataFrame,
        gcs_path: str,
        format: str = "parquet",
    ) -> bool:
        """
        Upload data to GCS.

        Args:
            df: DataFrame to upload
            gcs_path: Path within the bucket
            format: File format

        Returns:
            True if successful
        """
        try:
            logger.info(f"📤 Uploading {len(df)} rows to GCS: {gcs_path}")
            self.cloud_service.upload_to_gcs(
                df=df,
                gcs_path=gcs_path,
                format=format,
            )
            logger.info(f"✅ Upload complete: {gcs_path}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to upload to GCS: {e}")
            return False

    def check_gcs_exists(self, gcs_path: str) -> bool:
        """
        Check if a GCS path exists.

        Args:
            gcs_path: Path within the bucket

        Returns:
            True if exists
        """
        try:
            df = self.download_from_gcs(gcs_path, log_errors=False)
            return not df.empty
        except Exception:
            return False


class InstrumentsDataProvider(CloudDataProviderBase):
    """Data provider for instruments domain."""

    def __init__(self, cloud_target: Optional[CloudTarget] = None):
        super().__init__(
            domain="instruments",
            cloud_target=cloud_target,
            gcs_bucket=get_config("INSTRUMENTS_GCS_BUCKET_CEFI", "instruments-store-cefi-central-element-323112"),
            bigquery_dataset=get_config("INSTRUMENTS_BIGQUERY_DATASET", "instruments"),
        )

    def get_instruments_for_date(
        self,
        date: datetime,
        category: Optional[str] = None,
        venue: Optional[str] = None,
        instrument_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Get instruments for a specific date.

        Args:
            date: Target date
            category: Optional market category ('CEFI', 'TRADFI', 'DEFI')
            venue: Optional venue filter
            instrument_type: Optional instrument type filter

        Returns:
            DataFrame with instruments
        """
        date_str = date.strftime("%Y-%m-%d")
        gcs_path = f"instrument_availability/by_date/day={date_str}/instruments.parquet"

        if category:
            df = self.download_from_category_bucket(gcs_path, category)
        else:
            df = self.download_from_gcs(gcs_path)

        # Apply filters
        if venue and not df.empty:
            df = df[df["venue"] == venue]
        if instrument_type and not df.empty:
            df = df[df["instrument_type"] == instrument_type]

        return df

    def check_instruments_exist(
        self,
        date: datetime,
        categories: Optional[list] = None,
    ) -> bool:
        """
        Check if instruments exist for a date.

        Args:
            date: Target date
            categories: Categories to check (default: all)

        Returns:
            True if instruments exist
        """
        if categories is None:
            categories = ["CEFI", "TRADFI", "DEFI"]

        for category in categories:
            df = self.get_instruments_for_date(date, category=category)
            if not df.empty:
                return True

        return False


class MarketDataProvider(CloudDataProviderBase):
    """Data provider for market_data domain."""

    def __init__(self, cloud_target: Optional[CloudTarget] = None):
        super().__init__(
            domain="market_data",
            cloud_target=cloud_target,
            gcs_bucket=get_config("MARKET_DATA_GCS_BUCKET", "market-data-tick"),
            bigquery_dataset=get_config("MARKET_DATA_BIGQUERY_DATASET", "market_data_hft"),
        )

    def get_candles(
        self,
        instrument_id: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Get candles from BigQuery.

        Args:
            instrument_id: Canonical instrument key
            timeframe: Candle timeframe ('1m', '5m', '1h', etc.)
            start_date: Start datetime
            end_date: End datetime
            limit: Maximum rows to return

        Returns:
            DataFrame with OHLCV data
        """
        from datetime import timezone

        # Ensure timezone-aware datetimes
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        # Build table name
        table_suffix = timeframe.replace("h", "h").replace("m", "m").replace("s", "s")
        table_name = f"candles_{table_suffix}_trades"

        dataset = self.cloud_target.bigquery_dataset
        project_id = self.cloud_target.project_id

        query = f"""
        SELECT *
        FROM `{project_id}.{dataset}.{table_name}`
        WHERE instrument_id = @instrument_id
          AND timestamp >= @start_time
          AND timestamp < @end_time
        ORDER BY timestamp ASC
        """

        params = {
            "instrument_id": instrument_id,
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat(),
        }

        if limit and isinstance(limit, int) and limit > 0:
            query += " LIMIT @limit"
            params["limit"] = limit

        return self.query_bigquery(query, params)


class FeaturesDataProvider(CloudDataProviderBase):
    """Data provider for features domain."""

    def __init__(self, cloud_target: Optional[CloudTarget] = None):
        super().__init__(
            domain="features",
            cloud_target=cloud_target,
            gcs_bucket=get_config("FEATURES_GCS_BUCKET", "features-store"),
            bigquery_dataset=get_config("FEATURES_BIGQUERY_DATASET", "features"),
        )

    def get_features_for_date(
        self,
        date: datetime,
        feature_type: str = "delta_one",
        instrument_key: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Get computed features for a date.

        Args:
            date: Target date
            feature_type: Feature type ('delta_one', 'volatility', 'onchain')
            instrument_key: Optional instrument filter

        Returns:
            DataFrame with features
        """
        date_str = date.strftime("%Y-%m-%d")
        gcs_path = f"{feature_type}/by_date/day={date_str}/features.parquet"

        df = self.download_from_gcs(gcs_path)

        if instrument_key and not df.empty and "instrument_key" in df.columns:
            df = df[df["instrument_key"] == instrument_key]

        return df
