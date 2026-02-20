"""Test package imports and basic exports."""


def test_import_unified_domain_services():
    """Test that unified_domain_services can be imported."""
    import unified_domain_services

    assert hasattr(unified_domain_services, "StandardizedDomainCloudService")
    assert hasattr(unified_domain_services, "create_market_data_cloud_service")
    assert hasattr(unified_domain_services, "validate_timestamp_date_alignment")


def test_date_validator_import():
    """Test DateValidator and related exports."""
    from unified_domain_services import DateValidator, get_validator, should_skip_date

    assert DateValidator is not None
    assert callable(get_validator)
    assert callable(should_skip_date)
