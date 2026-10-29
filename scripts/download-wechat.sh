#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT=${WECHAT_DEB_OUTPUT:-$ROOT_DIR/docker/wechat.deb}
VERSION=4.1.13.23

case "${WECHAT_ARCH:-$(uname -m)}" in
  amd64|x86_64) ARCH=amd64 ;;
  arm64|aarch64) ARCH=arm64 ;;
  *) echo "Unsupported WeChat architecture: ${WECHAT_ARCH:-$(uname -m)}" >&2; exit 1 ;;
esac

VERIFY="$ROOT_DIR/docker/verify-wechat-package.sh"
mkdir -p "$(dirname "$OUT")"

if [[ -e "$OUT" ]]; then
  "$VERIFY" "$VERSION" "$ARCH" "$OUT"
  echo "Verified existing package at $OUT"
  exit 0
fi

SOURCE=${WECHAT_DEB_PATH:-}
if [[ -z "$SOURCE" ]]; then
  case "$ARCH" in
    amd64) URL=${WECHAT_DEB_URL:-${WECHAT_DEB_AMD64_URL:-}} ;;
    arm64) URL=${WECHAT_DEB_URL:-${WECHAT_DEB_ARM64_URL:-}} ;;
  esac
  if [[ -z "${URL:-}" ]]; then
    echo "No verified WeChat $VERSION $ARCH package available; set WECHAT_DEB_PATH or WECHAT_DEB_${ARCH^^}_URL" >&2
    exit 1
  fi
else
  URL=
fi

TMP=$(mktemp "${OUT}.tmp.XXXXXX")
cleanup() { rm -f "$TMP"; }
trap cleanup EXIT

if [[ -n "$SOURCE" ]]; then
  if [[ ! -f "$SOURCE" ]]; then
    echo "Local WeChat package does not exist: $SOURCE" >&2
    exit 1
  fi
  cp -- "$SOURCE" "$TMP"
else
  curl --fail --location --silent --show-error --output "$TMP" "$URL"
fi

"$VERIFY" "$VERSION" "$ARCH" "$TMP"
mv -- "$TMP" "$OUT"
trap - EXIT
echo "Saved verified WeChat $VERSION $ARCH package to $OUT"
