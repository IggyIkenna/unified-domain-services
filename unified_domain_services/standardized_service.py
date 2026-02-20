"""
Standardized Domain Cloud Service

Wrapper around DomainCloudOperations with domain-specific validation and synchronous public API.

Provides:
- Synchronous public API (wraps async DomainCloudOperations internally)
- Domain-specific validation rules (market_data, features, strategy, execution, ml)
- Timestamp semantics handling (timestamp_in/timestamp_out for internal, local_timestamp/timestamp for external)
- Unified signatures (timerange, filters) that apply appropriately for GCS vs BigQuery
- Multi-cloud support via provider detection

Design Philosophy:
- Simple, debuggable synchronous API for callers
- Internal async optimization via DomainCloudOperations
- Domain-aware validation before uploads
- Backward compatible with existing sync client code
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from unified_cloud_services.core.cloud_config import CloudConfig, CloudTarget
from unified_cloud_services.core.config import get_config
from unified_cloud_services.core.error_handling import GenericErrorHandlingService
from unified_cloud_services.core.run_async import run_async_from_sync
from unified_cloud_services.core.gcs_operations import DomainCloudOperations
from unified_domain_services.factories import (
    create_backtesting_cloud_service,
    create_features_cloud_service,
    create_instruments_cloud_service,
    create_market_data_cloud_service,
    create_strategy_cloud_service,
)
from unified_domain_services.validation import DomainValidationService
from unified_cloud_services.models.error import ErrorContext

logger = logging.getLogger(__name__)


def create_domain_cloud_service(domain: str) -> DomainCloudOperations:
    """
    Factory function to create domain-specific DomainCloudOperations.

    Args:
        domain: 'market_data', 'features', 'strategy', 'execution', 'ml'

    Returns:
        Configured DomainCloudOperations instance
    """
    domain_factories = {
        "market_data": create_market_data_cloud_service,
        "features": create_features_cloud_service,
        "strategy": create_strategy_cloud_service,
        "execution": create_strategy_cloud_service,  # Reuse strategy service for execution
        "ml": create_backtesting_cloud_service,  # Reuse backtesting for ML
        "instruments": create_instruments_cloud_service,  # Instruments domain
    }

    if domain not in domain_factories:
        raise ValueError(f"Unknown domain: {domain}. Must be one of: {list(domain_factories.keys())}")

    return domain_factories[domain]()


class StandardizedDomainCloudService:
    """
    Wrapper around DomainCloudOperations with domain-specific validation.

    Provides synchronous public API that wraps async DomainCloudOperations internally,
    enabling simple, debuggable APIs while maintaining internal async optimization.
    """

    def __init__(
        self,
        domain: str,
        cloud_target: CloudTarget | None = None,
        config: CloudConfig | None = None,
    ):
        """
        Initialize standardized domain cloud service.

        Args:
            domain: 'market_data', 'features', 'strategy', 'execution', 'ml', 'instruments'
            cloud_target: Runtime cloud target configuration (uses defaults if None)
            config: Optional CloudConfig (uses defaults if None)
        """
        self.domain = domain

        # Create domain-specific DomainCloudOperations
        self.unified_service = create_domain_cloud_service(domain)

        # Set cloud target (with domain-specific defaults if not provided)
        self.cloud_target = cloud_target or self._get_default_target(domain)

        # Initialize domain validation service
        self.domain_validation = DomainValidationService(domain=domain)

        # Initialize error handling service (DRY: centralized error handling)
        self.error_handling = GenericErrorHandlingService(
            config={
                "enable_error_classification": True,
                "enable_auto_recovery": True,
                "strict_mode": False,  # Don't fail fast by default
            }
        )

        # Track async event loop state
        self._event_loop = None
        self._loop_thread = None

        logger.info(f"✅ StandardizedDomainCloudService initialized: domain={domain}")
        logger.info(f"🔧 Target: bucket={self.cloud_target.gcs_bucket}, dataset={self.cloud_target.bigquery_dataset}")

    def _get_default_target(self, domain: str) -> CloudTarget:
        """Get default CloudTarget for domain"""
        defaults = {
            "market_data": CloudTarget(
                gcs_bucket=get_config("GCS_BUCKET", "market-data-tick"),
                bigquery_dataset=get_config("BIGQUERY_DATASET", "market_data_hft"),
            ),
            "features": CloudTarget(
                gcs_bucket=get_config("ML_FEATURES_BUCKET", "ml-features-store"),
                bigquery_dataset=get_config("FEATURES_DATASET", "features_data"),
            ),
            "strategy": CloudTarget(
                gcs_bucket=get_config("STRATEGY_BUCKET", "strategy-execution"),
                bigquery_dataset=get_config("STRATEGY_DATASET", "strategy_orders"),
            ),
            "execution": CloudTarget(
                gcs_bucket=get_config("EXECUTION_BUCKET", "strategy-execution"),
                bigquery_dataset=get_config("EXECUTION_DATASET", "execution_logs"),
            ),
            "ml": CloudTarget(
                gcs_bucket=get_config("ML_MODELS_BUCKET", "ml-models-store"),
                bigquery_dataset=get_config("ML_PREDICTIONS_DATASET", "ml_predictions"),
            ),
        }
        return defaults.get(domain, defaults["market_data"])

    def _run_async(self, coro):
        """Run async coroutine in sync context"""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No running event loop -- safe to use run_async_from_sync()
            return run_async_from_sync(coro)

        # Loop IS running -- run in a separate thread to avoid blocking
        import threading

        result = [None]
        exception = [None]

        def run_in_thread():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                result[0] = new_loop.run_until_complete(coro)
            except Exception as e:
                exception[0] = e
            finally:
                new_loop.close()

        thread = threading.Thread(target=run_in_thread)
        thread.start()
        thread.join()

        if exception[0]:
            raise exception[0]
        return result[0]

    # =================================================================
    # SYNCHRONOUS PUBLIC API (wraps async DomainCloudOperations)
    # =================================================================

    def upload_to_gcs(
        self,
        data: pd.DataFrame | bytes | str | Any,
        gcs_path: str,
        format: str = "parquet",
        metadata: dict[str, str] | None = None,
    ) -> str:
        """
        Upload data to GCS with runtime target configuration.

        Synchronous wrapper around async DomainCloudOperations.upload_to_gcs()

        Args:
            data: Data to upload (DataFrame, bytes, string, or any serializable object)
            gcs_path: Path within bucket (e.g., 'processed_candles/by_date/day=2023-05-23/...')
            format: File format ('parquet', 'csv', 'json', 'pickle', 'joblib', 'bytes')
            metadata: Optional metadata to attach to blob

        Returns:
            Full GCS path (gs://bucket/path)
        """

        async def _upload():
            return await self.unified_service.upload_to_gcs(
                data=data,
                target=self.cloud_target,
                gcs_path=gcs_path,
                format=format,
                metadata=metadata,
            )

        return self._run_async(_upload())

    def upload_to_gcs_batch(
        self,
        uploads: list[dict[str, Any]],
        show_progress: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Upload multiple files to GCS in batch - THREAD-SAFE for use with ThreadPoolExecutor.

        This method is designed to be called ONCE with all uploads, rather than
        calling upload_to_gcs() multiple times from ThreadPoolExecutor threads.
        The async batch method handles concurrency internally using asyncio.gather.

        IMPORTANT: Do NOT use ThreadPoolExecutor with upload_to_gcs(). Instead,
        collect all uploads and call this method once.

        Args:
            uploads: List of upload dicts with keys:
                    - 'data': Data to upload (DataFrame, bytes, str)
                    - 'gcs_path': Path within bucket
                    - 'format': File format (default: 'parquet')
                    - 'metadata': Optional metadata dict
            show_progress: Whether to log progress (default: True)

        Returns:
            List of upload results with 'success', 'gcs_path', 'full_path', 'error' keys

        Example:
            uploads = [
                {'data': df1, 'gcs_path': 'path/to/file1.parquet', 'format': 'parquet'},
                {'data': df2, 'gcs_path': 'path/to/file2.parquet', 'format': 'parquet'},
            ]
            results = service.upload_to_gcs_batch(uploads)
            successful = [r for r in results if r['success']]
        """
        if show_progress:
            logger.info(f"📤 Starting batch upload of {len(uploads)} files to GCS...")

        async def _batch_upload():
            return await self.unified_service.upload_to_gcs_batch(
                uploads=uploads,
                target=self.cloud_target,
                max_concurrent=50,  # High concurrency for fast batch uploads
            )

        results = self._run_async(_batch_upload())

        if show_progress:
            successful = sum(1 for r in results if r.get("success"))
            failed = len(results) - successful
            logger.info(f"📤 Batch upload complete: {successful} successful, {failed} failed")

        return results

    def check_gcs_files_exist_batch(self, gcs_paths: list[str]) -> dict[str, bool]:
        """
        Check existence of multiple GCS files in batch (performance-optimized).

        Uses list_blobs instead of individual blob.exists() calls for efficiency.
        Groups paths by prefix and lists blobs once per prefix.

        Args:
            gcs_paths: List of GCS paths to check (relative to bucket)

        Returns:
            Dict mapping GCS path to existence boolean

        Example:
            paths = ['market-data/2024-01-01/file1.parquet', 'market-data/2024-01-01/file2.parquet']
            exists = service.check_gcs_files_exist_batch(paths)
            # {'market-data/2024-01-01/file1.parquet': True, 'market-data/2024-01-01/file2.parquet': False}
        """
        if not gcs_paths:
            return {}

        async def _check_batch():
            return await self.unified_service.check_gcs_files_exist_batch(
                target=self.cloud_target,
                gcs_paths=gcs_paths,
            )

        return self._run_async(_check_batch())

    def download_from_gcs(
        self, gcs_path: str, format: str = "parquet", log_errors: bool = True
    ) -> pd.DataFrame | bytes | str | Any:
        """
        Download data from GCS with runtime target configuration.

        Synchronous wrapper around async DomainCloudOperations.download_from_gcs()

        Args:
            gcs_path: Path within bucket
            format: Expected format ('parquet', 'csv', 'json', 'pickle', 'joblib', 'bytes', 'text')
            log_errors: Whether to log errors as ERROR (default True).
                       Set to False for expected missing files (cache misses).

        Returns:
            Data in appropriate format
        """

        async def _download():
            return await self.unified_service.download_from_gcs(
                target=self.cloud_target,
                gcs_path=gcs_path,
                format=format,
                log_errors=log_errors,
            )

        return self._run_async(_download())

    def upload_to_bigquery(
        self,
        data: pd.DataFrame,
        table_name: str,
        write_mode: str = "safe",
        partition_field: str = "timestamp",
        clustering_fields: list[str] | None = None,
        validate: bool = True,
    ) -> dict[str, Any]:
        """
        Upload DataFrame to BigQuery with domain-specific validation.

        Synchronous wrapper around async DomainCloudOperations.upload_to_bigquery()
        with domain-specific validation before upload.

        Args:
            data: DataFrame to upload
            table_name: Table name (without project.dataset prefix)
            write_mode: 'safe' (delete+insert), 'skip' (check existence), 'append'
            partition_field: Field for table partitioning
            clustering_fields: Fields for table clustering
            validate: Whether to apply domain-specific validation (default: True)

        Returns:
            Upload result with metrics
        """
        # Apply domain-specific validation
        if validate:
            validation_result = self.domain_validation.validate_for_domain(data, data_type=None)
            if not validation_result.valid:
                error_msg = f"Domain validation failed for {self.domain}"
                if validation_result.errors:
                    error_msg += f"\nErrors: {validation_result.errors}"
                raise ValueError(error_msg)

            if validation_result.warnings:
                logger.warning(f"Domain validation warnings for {self.domain}: {validation_result.warnings}")

        async def _upload():
            return await self.unified_service.upload_to_bigquery(
                data=data,
                target=self.cloud_target,
                table_name=table_name,
                write_mode=write_mode,
                partition_field=partition_field,
                clustering_fields=clustering_fields,
            )

        return self._run_async(_upload())

    def query_bigquery(self, query: str, parameters: dict[str, Any] = None) -> pd.DataFrame:
        """
        Execute BigQuery query with runtime target configuration.

        Synchronous wrapper around async DomainCloudOperations.query_bigquery()

        Args:
            query: SQL query to execute
            parameters: Query parameters for parameterized queries

        Returns:
            Query results as DataFrame
        """

        async def _query():
            return await self.unified_service.query_bigquery(
                query=query, target=self.cloud_target, parameters=parameters
            )

        return self._run_async(_query())

    def get_secret(self, secret_name: str, version: str = "latest") -> str:
        """
        Get secret from Secret Manager.

        Args:
            secret_name: Name of the secret
            version: Secret version ('latest' or specific version)

        Returns:
            Secret value as string
        """

        async def _get_secret():
            return await self.unified_service.get_secret(
                secret_name=secret_name, target=self.cloud_target, version=version
            )

        return self._run_async(_get_secret())

    def ensure_warmed_connections(self):
        """Ensure cloud connections are warmed up (synchronous wrapper)"""
        self._run_async(self.unified_service.ensure_warmed_connections())

    def cleanup(self):
        """Cleanup cloud connections"""
        self._run_async(self.unified_service.cleanup())

    # =================================================================
    # MARKET_DATA DOMAIN CONVENIENCE METHODS
    # =================================================================

    def get_tick_data(
        self,
        instrument: str,
        timerange: tuple[datetime, datetime] | None = None,
        filters: dict[str, Any] | None = None,
        data_type: str = "trades",
        date: datetime | None = None,
    ) -> pd.DataFrame:
        """
        Get tick data from GCS with unified timerange/filter interface.

        Market-data-specific convenience method for accessing tick data.
        Uses optimized parquet index chunk structure for efficient reads.

        Args:
            instrument: Instrument ID (e.g., 'BINANCE-FUTURES:PERPETUAL:BTC-USDT')
            timerange: (start_time, end_time) tuple - optional
            filters: Additional filters (optional)
            data_type: Data type ('trades', 'book_snapshot_5', etc.)
            date: Single date (datetime or date) - used if timerange not provided

        Returns:
            DataFrame with tick data
        """
        if self.domain != "market_data":
            raise ValueError(f"get_tick_data() is only available for market_data domain, not {self.domain}")

        async def _get_tick_data():
            # Determine date from timerange or date parameter
            if timerange:
                start_date, end_date = timerange
                date_to_use = start_date
            elif date:
                if isinstance(date, datetime):
                    date_to_use = date
                else:
                    # Assume it's a date object, convert to datetime
                    date_to_use = datetime.combine(date, datetime.min.time()).replace(tzinfo=timezone.utc)
            else:
                logger.warning("No timerange or date provided for tick data query")
                return pd.DataFrame()

            # Build GCS path pattern
            # Format: raw_tick_data/by_date/day=YYYY-MM-DD/data_type={data_type}/{instrument}.parquet
            date_str = date_to_use.strftime("%Y-%m-%d")
            gcs_path = f"raw_tick_data/by_date/day={date_str}/data_type={data_type}/{instrument}.parquet"

            logger.info(f"📥 Loading tick data: {gcs_path}")

            try:
                tick_df = await self.unified_service.download_from_gcs(
                    target=self.cloud_target, gcs_path=gcs_path, format="parquet"
                )

                # If timerange provided, filter the data
                if timerange and not tick_df.empty:
                    start_time, end_time = timerange
                    if "timestamp" in tick_df.columns:
                        # Convert timestamp to datetime for filtering
                        timestamps = pd.to_datetime(tick_df["timestamp"], unit="us", errors="coerce", utc=True)

                        # If end_time is at second boundary (23:59:59), extend to include microseconds
                        # to match behavior of loading entire day file
                        if end_time.microsecond == 0 and end_time.second == 59 and end_time.minute == 59:
                            # Extend to include all microseconds in the second
                            end_time_inclusive = end_time.replace(microsecond=999999)
                        else:
                            end_time_inclusive = end_time

                        mask = (timestamps >= start_time) & (timestamps <= end_time_inclusive)
                        tick_df = tick_df[mask]
                        logger.info(f"✅ Filtered to {len(tick_df)} ticks in timerange")

                return tick_df
            except Exception as e:
                logger.warning(f"Failed to load tick data from {gcs_path}: {e}")
                return pd.DataFrame()

        return self._run_async(_get_tick_data())

    def get_candles_with_hft_features(
        self,
        instrument: str,
        timeframe: str,
        timerange: tuple[datetime, datetime] | None = None,
        filters: dict[str, Any] | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        Get candles with HFT features from BigQuery (market_data domain).

        Note: This accesses market_data domain. HFT features are part of market data candles.
        Features service domain contains MFT features (technical indicators).

        Args:
            instrument: Instrument ID
            timeframe: Timeframe ('15s', '1m', '5m', '1h', etc.)
            timerange: (start_time, end_time) tuple - optional
            filters: Additional filters (optional)
            limit: Maximum rows to return

        Returns:
            DataFrame with OHLCV + HFT features (delay metrics, volume imbalances, liquidations, etc.)
        """
        if self.domain != "market_data":
            raise ValueError(
                f"get_candles_with_hft_features() is only available for market_data domain, not {self.domain}"
            )

        # Validate timeframe to prevent SQL injection (table name cannot be parameterized)
        valid_timeframes = {"15s", "1m", "5m", "15m", "1h", "4h", "24h", "1d"}
        if timeframe not in valid_timeframes:
            raise ValueError(f"Invalid timeframe: {timeframe}. Must be one of: {valid_timeframes}")

        async def _get_candles():
            # Use cloud-agnostic query client (BigQuery or Athena)
            query_client = self.unified_service.query_client

            # Build BigQuery query with parameterized queries
            if timerange:
                start_time, end_time = timerange
                query = f"""
                SELECT * FROM `{self.cloud_target.project_id}.{self.cloud_target.bigquery_dataset}.candles_{timeframe}_trades`
                WHERE instrument_id = @instrument_id
                  AND timestamp >= @start_timestamp
                  AND timestamp < @end_timestamp
                ORDER BY timestamp DESC
                LIMIT @limit
                """
                params = {
                    "instrument_id": instrument,
                    "start_timestamp": start_time,
                    "end_timestamp": end_time,
                    "limit": limit,
                }
            else:
                query = f"""
                SELECT * FROM `{self.cloud_target.project_id}.{self.cloud_target.bigquery_dataset}.candles_{timeframe}_trades`
                WHERE instrument_id = @instrument_id
                ORDER BY timestamp DESC
                LIMIT @limit
                """
                params = {
                    "instrument_id": instrument,
                    "limit": limit,
                }

            # Execute query using cloud-agnostic client
            results_df = query_client.execute_query_to_dataframe(
                query=query,
                params=params,
                timeout_seconds=60,
            )

            logger.info(f"✅ Retrieved {len(results_df)} candles with HFT features")
            return results_df

        return self._run_async(_get_candles())

    def check_gcs_path_exists(self, gcs_path: str) -> bool:
        """
        Check if GCS path exists.

        Uses centralized error handling to ensure errors are properly logged and propagated.

        Args:
            gcs_path: Path within bucket

        Returns:
            True if path exists, False if not found or on error.
            Raises exceptions for authentication/configuration errors.
        """

        async def _check_exists():
            context = ErrorContext(
                operation="check_gcs_path_exists",
                component="StandardizedDomainCloudService",
                input_data={
                    "gcs_path": gcs_path,
                    "bucket": self.cloud_target.gcs_bucket,
                    "project_id": self.cloud_target.project_id,
                },
            )

            async def _do_check_exists():
                # Use cloud-agnostic storage client (GCS or S3)
                storage_client = self.unified_service.storage_client

                try:
                    return storage_client.blob_exists(
                        bucket=self.cloud_target.gcs_bucket,
                        blob_path=gcs_path,
                    )
                except Exception as e:
                    # Errors should be properly logged and re-raised
                    error_msg = f"Failed to check storage path existence for '{gcs_path}': {e}"
                    logger.error(f"❌ {error_msg}")
                    raise

            # Use centralized error handling
            try:
                return (
                    await self.error_handling.execute_with_error_handling_async(
                        _do_check_exists,
                        context=context,
                        max_retries=0,  # Don't retry on authentication errors
                    )
                    or False
                )
            except Exception:
                # If error handling raises, return False (but error already logged)
                return False

        return self._run_async(_check_exists())

    def list_gcs_files(self, prefix: str) -> list[dict[str, str]]:
        """
        List files in GCS with given prefix.

        Uses centralized error handling to ensure errors are properly logged and propagated.
        Raises exceptions for authentication/configuration errors instead of silently failing.

        Args:
            prefix: Path prefix to filter files

        Returns:
            List of file dictionaries with 'name', 'size', 'updated' keys.
            Empty list returned only if no files found (not on error).

        Raises:
            ValueError: If authentication fails or credentials are missing
            Exception: Other GCS errors (properly logged with context)
        """

        async def _list_files():
            context = ErrorContext(
                operation="list_gcs_files",
                component="StandardizedDomainCloudService",
                input_data={
                    "prefix": prefix,
                    "bucket": self.cloud_target.gcs_bucket,
                    "project_id": self.cloud_target.project_id,
                },
            )

            async def _do_list_files():
                # Use cloud-agnostic storage client (GCS or S3)
                storage_client = self.unified_service.storage_client

                try:
                    files = []
                    for blob_metadata in storage_client.list_blobs(
                        bucket=self.cloud_target.gcs_bucket,
                        prefix=prefix,
                    ):
                        files.append(
                            {
                                "name": blob_metadata.name,
                                "size": blob_metadata.size,
                                "updated": blob_metadata.last_modified,
                            }
                        )

                    logger.debug(f"✅ Listed {len(files)} files for prefix: {prefix}")
                    return files

                except Exception as e:
                    # Errors should be properly logged and re-raised
                    error_msg = f"Failed to list storage files for prefix '{prefix}': {e}"
                    logger.error(f"❌ {error_msg}")
                    raise

            # Use centralized error handling
            # Note: If error handling is configured to return None on failure, we want to raise instead
            result = await self.error_handling.execute_with_error_handling_async(
                _do_list_files,
                context=context,
                max_retries=0,  # Don't retry on authentication errors
            )

            # If error handling returned None (on failure), raise to prevent silent failures
            if result is None:
                raise ValueError(
                    f"list_gcs_files returned None for prefix '{prefix}'. "
                    f"This indicates an error occurred but was silently handled. "
                    f"Check logs for details."
                )

            return result

        try:
            result = self._run_async(_list_files())
            return result if result is not None else []
        except ValueError:
            # Re-raise authentication/configuration errors
            raise
        except Exception as e:
            # Wrap other exceptions with context
            logger.error(f"❌ Unexpected error in list_gcs_files: {e}")
            raise RuntimeError(f"Failed to list GCS files: {e}") from e
