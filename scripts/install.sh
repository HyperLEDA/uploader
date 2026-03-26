#!/bin/sh
set -eu

REPO="HyperLEDA/uploader"
TOOL_NAME="uploader"
UV_INSTALL_URL="https://docs.astral.sh/uv/getting-started/installation/"

if ! command -v uv >/dev/null 2>&1; then
    echo "Error: uv is required but was not found in PATH." >&2
    echo "Install uv first: ${UV_INSTALL_URL}" >&2
    exit 1
fi

if [ -n "${VERSION:-}" ]; then
    TAG="$VERSION"
    case "$TAG" in
        v*) ;;
        *) TAG="v$TAG" ;;
    esac
else
    TAG=$(
        curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
            | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' \
            | head -n 1
    )
fi

if [ -z "$TAG" ]; then
    echo "Error: failed to determine release tag." >&2
    exit 1
fi

WHEEL_URL=$(
    curl -fsSL "https://api.github.com/repos/${REPO}/releases/tags/${TAG}" \
        | sed -n 's/.*"browser_download_url":[[:space:]]*"\([^"]*\.whl\)".*/\1/p' \
        | head -n 1
)

if [ -z "$WHEEL_URL" ]; then
    echo "Error: wheel asset not found for ${TAG}." >&2
    exit 1
fi

TMPDIR="$(mktemp -d)"
cleanup() {
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

WHEEL_PATH="${TMPDIR}/$(basename "$WHEEL_URL")"
curl -fsSL "$WHEEL_URL" -o "$WHEEL_PATH"

uv tool install --force --from "$WHEEL_PATH" "$TOOL_NAME"

echo "${TOOL_NAME} installed from ${TAG}"
