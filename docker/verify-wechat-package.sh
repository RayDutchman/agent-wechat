#!/usr/bin/env bash
set -euo pipefail

VERSION=${1:-}
ARCH=${2:-}
PACKAGE=${3:-}

if [[ "$VERSION" != "4.1.13.23" ]]; then
  echo "Unsupported WeChat version: ${VERSION:-<missing>}" >&2
  exit 1
fi

case "$ARCH" in
  amd64)
    EXPECTED_SHA256=b7d0f8d53e9f648bc2c77a6096a04100d008f2d9f0d3988a2a4859b5992aca0a
    ;;
  arm64)
    EXPECTED_SHA256=51784a262c725ef1595dd833f456190e913583dd81c24dd8fe587532bc91c0dc
    ;;
  *)
    echo "Unsupported WeChat architecture: ${ARCH:-<missing>}" >&2
    exit 1
    ;;
esac

if [[ ! -f "$PACKAGE" ]]; then
  echo "WeChat package is missing: $PACKAGE" >&2
  exit 1
fi

ACTUAL_SHA256=$(sha256sum "$PACKAGE" | cut -d ' ' -f 1)
if [[ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]]; then
  echo "WeChat package SHA256 mismatch for $VERSION $ARCH" >&2
  exit 1
fi

if ! command -v dpkg-deb >/dev/null 2>&1; then
  echo "dpkg-deb is required to verify package metadata" >&2
  exit 1
fi
PACKAGE_VERSION=$(dpkg-deb -f "$PACKAGE" Version)
PACKAGE_ARCH=$(dpkg-deb -f "$PACKAGE" Architecture)
PACKAGE_NAME=$(dpkg-deb -f "$PACKAGE" Package)
if [[ "$PACKAGE_NAME" != "wechat" || "$PACKAGE_VERSION" != "$VERSION" || "$PACKAGE_ARCH" != "$ARCH" ]]; then
  echo "WeChat package metadata mismatch: expected $VERSION/$ARCH, got $PACKAGE_VERSION/$PACKAGE_ARCH" >&2
  exit 1
fi

echo "Verified WeChat $VERSION $ARCH package: $ACTUAL_SHA256"
