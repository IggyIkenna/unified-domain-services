# unified-domain-services

Trading domain services library: clients, validation, cloud service wrappers, and `StandardizedDomainCloudService`.

## Install

```bash
# From Artifact Registry (Cloud Build)
pip install unified-domain-services

# Local development
uv pip install -e ".[dev]"
```

## Dependencies

- unified-cloud-services>=1.5.0

## Usage

```python
from unified_domain_services import (
    StandardizedDomainCloudService,
    create_market_data_cloud_service,
    validate_timestamp_date_alignment,
    DateValidator,
)
```

## Quality Gates

```bash
bash scripts/quality-gates.sh
bash scripts/quality-gates.sh --no-fix
```

## Quick Merge

```bash
bash scripts/quickmerge.sh "commit message"
```
