#!/bin/bash
# Auto-bump library version if code changes
# Used as pre-commit hook for library repos

set -e

if [ ! -f "pyproject.toml" ]; then
    exit 0
fi

CURRENT_VERSION=$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')

if [ -z "$CURRENT_VERSION" ]; then
    echo "Could not extract version from pyproject.toml"
    exit 0
fi

CODE_CHANGED=$(git diff --cached --name-only | grep -E "^(unified_|.*\.py$)" | grep -v "^tests/" | wc -l | tr -d ' ')

if [ "$CODE_CHANGED" -eq 0 ]; then
    echo "No code changes, skipping version bump"
    exit 0
fi

IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"
NEW_PATCH=$((PATCH + 1))
NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"

echo "Code changes detected - bumping version: $CURRENT_VERSION -> $NEW_VERSION"

if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" pyproject.toml
else
    sed -i "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" pyproject.toml
fi

git add pyproject.toml
echo "Version bumped to $NEW_VERSION and staged for commit"
