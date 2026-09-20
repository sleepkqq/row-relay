#!/bin/sh
set -eu

repo=https://github.com/NikolayS/PgQue.git
ref=e8ee488d2c1d87ab09eed2581ec4bbc1e68315f6
target=.slim/clonedeps/NikolayS__PgQue

if [ ! -d "$target" ]; then
  mkdir -p .slim/clonedeps
  temporary=$(mktemp -d .slim/clonedeps/pgque.XXXXXX)
  trap 'rmdir "$temporary" 2>/dev/null || true' EXIT
  git clone --depth 1 --branch v0.2.0 "$repo" "$temporary/source"
  test "$(git -C "$temporary/source" rev-parse HEAD)" = "$ref"
  mv "$temporary/source" "$target"
fi

test "$(git -C "$target" remote get-url origin)" = "$repo"
test "$(git -C "$target" rev-parse HEAD)" = "$ref"
test -z "$(git -C "$target" status --porcelain)"
printf 'Verified PgQue v0.2.0 (%s)\n' "$ref"
