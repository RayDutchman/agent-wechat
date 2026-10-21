#!/usr/bin/env bash
set -euo pipefail

# Generate TypeScript types from Rust definitions using ts-rs
# Usage: ./scripts/generate-types.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RUST_PKG="$ROOT_DIR/packages/agent-server-rust"
GENERATED_DIR="$ROOT_DIR/packages/shared/src/types/generated"
TEMP_GENERATED_DIR="$(mktemp -d /tmp/opencode/agent-wechat-generated.XXXXXX)"
trap 'rm -rf "$TEMP_GENERATED_DIR"' EXIT

echo "Generating TypeScript types from Rust..."

# Fail before modifying existing generated bindings when cargo is unavailable.
if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required to generate Rust-exported types" >&2
  exit 127
fi

# Run ts-rs export tests with output directed to shared package
cd "$RUST_PKG"
TS_RS_EXPORT_DIR="$TEMP_GENERATED_DIR" cargo test --quiet 2>/dev/null

# Fix imports: ts-rs generates `from "./Foo"` but NodeNext requires `from "./Foo.js"`
for f in "$TEMP_GENERATED_DIR"/*.ts; do
  if grep -q 'from "./' "$f"; then
    sed -i 's|from "\(\./[^"]*\)"|from "\1.js"|g' "$f"
  fi
done

# Create barrel file that re-exports all generated types
BARREL="$TEMP_GENERATED_DIR/index.ts"
echo "// Auto-generated barrel file — do not edit manually" > "$BARREL"
echo "// Generated from packages/agent-server-rust/src/ia/types.rs via ts-rs" >> "$BARREL"
echo "" >> "$BARREL"

for f in "$TEMP_GENERATED_DIR"/*.ts; do
  base="$(basename "$f" .ts)"
  if [ "$base" = "index" ]; then
    continue
  fi
  echo "export type { $base } from \"./$base.js\";" >> "$BARREL"
done

echo ""
echo "Generated types in $GENERATED_DIR:"
ls -1 "$GENERATED_DIR"
echo ""
echo "Done."

python3 - "$ROOT_DIR/packages/agent-server-rust/src/message-type-dictionary.json" "$ROOT_DIR/packages/shared/src/message-types.generated.ts" <<'PY'
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
dictionary = json.loads(source.read_text(encoding="utf-8"))
target.write_text(
    "// Generated from packages/agent-server-rust/src/message-type-dictionary.json; do not edit manually.\n"
    "export const messageTypeDictionary = "
    + json.dumps(dictionary, ensure_ascii=False, indent=2)
    + " as const;\n",
    encoding="utf-8",
)
print(f"Generated shared message dictionary in {target}")
PY

# Publish only after the Rust export tests and dictionary generation succeeded.
mkdir -p "$GENERATED_DIR"
cp "$TEMP_GENERATED_DIR"/*.ts "$GENERATED_DIR"/
