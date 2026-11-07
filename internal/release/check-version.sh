#!/usr/bin/env bash
# Refuses to release a version the repository does not declare, or one that
# has already been released.
set -euo pipefail

version="${1:?usage: check-version.sh VERSION}"
if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "\"$version\" is not a version such as 0.0.1" >&2
  exit 1
fi

module=$(sed -n 's/^module(name = "protovcs", version = "\([^"]*\)").*/\1/p' MODULE.bazel)
if [[ "$module" != "$version" ]]; then
  echo "MODULE.bazel declares version \"$module\", not \"$version\"" >&2
  exit 1
fi

# The Rust workspace, where there is one, carries the same version.
if [[ -f rust/Cargo.toml ]]; then
  crates=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)".*/\1/p' rust/Cargo.toml)
  if [[ "$crates" != "$version" ]]; then
    echo "rust/Cargo.toml declares version \"$crates\", not \"$version\"" >&2
    exit 1
  fi
fi

if [[ -f rust/MODULE.bazel ]]; then
  rustmod=$(sed -n 's/^module(name = "protovcs_rust", version = "\([^"]*\)").*/\1/p' rust/MODULE.bazel)
  if [[ "$rustmod" != "$version" ]]; then
    echo "rust/MODULE.bazel declares version \"$rustmod\", not \"$version\"" >&2
    exit 1
  fi
fi

if git rev-parse -q --verify "refs/tags/v$version" >/dev/null; then
  echo "v$version is already tagged" >&2
  exit 1
fi
echo "releasing $version"
