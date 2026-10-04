#!/usr/bin/env bash
# ==============================================================================
# Script: bump_version.sh
# Purpose: Auto-increments package version (patch/minor/major) in Cargo.toml,
#          updates Cargo.lock, and scaffolds CHANGELOG.md.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

BUMP_TYPE="${1:-patch}"

if [[ "$BUMP_TYPE" != "patch" && "$BUMP_TYPE" != "minor" && "$BUMP_TYPE" != "major" ]]; then
    echo "Usage: $0 [patch|minor|major] (default: patch)" >&2
    exit 1
fi

CURRENT_VERSION=$(grep -m1 '^[[:space:]]*version[[:space:]]*=' Cargo.toml | awk -F'"' '{print $2}')
if [[ -z "${CURRENT_VERSION}" ]]; then
    echo "Error: Could not parse version from Cargo.toml" >&2
    exit 1
fi

IFS='.' read -r MAJOR MINOR PATCH <<< "${CURRENT_VERSION%%-*}"
MAJOR="${MAJOR:-0}"
MINOR="${MINOR:-0}"
PATCH="${PATCH:-0}"

case "$BUMP_TYPE" in
    patch)
        PATCH=$((PATCH + 1))
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        ;;
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
TODAY=$(date +%Y-%m-%d)

echo "Bumping version: ${CURRENT_VERSION} -> ${NEW_VERSION} (${BUMP_TYPE})"

# Update Cargo.toml (only package version line at top)
sed -i '' -E '1,/^version[[:space:]]*=/ s/^version[[:space:]]*=[[:space:]]*"[^"]+"/version = "'"${NEW_VERSION}"'"/' Cargo.toml

# Update CHANGELOG.md if not already present
if ! grep -q "## \[${NEW_VERSION}\]" CHANGELOG.md; then
    TEMP_FILE=$(mktemp)
    awk -v new_ver="## [${NEW_VERSION}] - ${TODAY}\n\n### Changed\n- Version bump to ${NEW_VERSION}.\n" '
    /^## \[/ && !inserted {
        print new_ver
        inserted=1
    }
    { print }
    ' CHANGELOG.md > "${TEMP_FILE}"
    mv "${TEMP_FILE}" CHANGELOG.md
    echo "Added entry for [${NEW_VERSION}] to CHANGELOG.md"
fi

# Update Cargo.lock
cargo check --quiet

echo "✅ Successfully auto-incremented to ${NEW_VERSION}!"
