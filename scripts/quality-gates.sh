#!/bin/bash
# Quality Gates - Format, Lint, Test
# Three-stage consistency: Local, GitHub Actions, Cloud Build (all use ruff==0.15.0)

set -e

NO_FIX=false
QUICK=false

while [[ "$#" -gt 0 ]]; do
  case $1 in
    --no-fix) NO_FIX=true ;;
    --quick) QUICK=true ;;
    *) echo "Unknown parameter: $1"; exit 1 ;;
  esac
  shift
done

echo "======================================================================"
echo "UNIFIED-DOMAIN-SERVICES QUALITY GATES"
echo "======================================================================"

# Step 0: Bootstrap UV if needed
if ! command -v uv &> /dev/null; then
  echo "Installing UV..."
  pip install uv --quiet
fi

# Step 1: Install dependencies (clone deps if in CI with GH_PAT)
if [ -d "deps/unified-config-interface" ] && [ -d "deps/unified-cloud-services" ]; then
  echo "Installing from cloned deps..."
  uv pip install --system -e deps/unified-config-interface
  uv pip install --system -e deps/unified-cloud-services
elif [ -d "../unified-config-interface" ] && [ -d "../unified-cloud-services" ]; then
  uv pip install --system -e ../unified-config-interface
  uv pip install --system -e ../unified-cloud-services
fi

# Step 2: Install self (unified-cloud-services from PyPI/Artifact Registry if deps not cloned)
uv pip install --system -e ".[dev]" --quiet

# Step 3: Format
if [ "$NO_FIX" = false ]; then
  ruff format .
else
  ruff format --check .
fi

# Step 4: Lint
if [ "$NO_FIX" = false ]; then
  ruff check --fix .
else
  ruff check .
fi

# Step 5: Tests
if [ "$QUICK" = false ]; then
  pytest -v --tb=short
else
  pytest -v --maxfail=1 -x
fi

echo ""
echo "Quality gates passed!"
