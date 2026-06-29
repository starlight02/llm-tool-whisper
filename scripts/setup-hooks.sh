#!/usr/bin/env bash
#
# One-time setup script to enable git hooks via lefthook.
#
# Prerequisites:
#   Install lefthook first (choose one):
#     macOS:   brew install lefthook
#     Linux:   curl -sSfL https://raw.githubusercontent.com/evilmartians/lefthook/master/install.sh | sh
#     Cargo:   cargo binstall lefthook   (or download binary from releases)
#
# Then run:
#   ./scripts/setup-hooks.sh
#
# This will:
#   - Unset any previous core.hooksPath (migration from old .githooks setup)
#   - Run `lefthook install`
#
# After setup, `cargo fmt -- --check` and `cargo clippy` will run automatically
# before every commit. If they fail, the commit is rejected.

set -euo pipefail

echo "Setting up lefthook for git hooks..."

# Migrate away from old core.hooksPath if it was previously set
if git config --get core.hooksPath >/dev/null 2>&1; then
  echo "→ Removing legacy core.hooksPath (was pointing to .githooks)"
  git config --unset core.hooksPath || true
fi

# Check if lefthook is available
if ! command -v lefthook >/dev/null 2>&1; then
  echo ""
  echo "❌ lefthook is not installed."
  echo ""
  echo "Please install it first:"
  echo "  macOS:   brew install lefthook"
  echo "  Linux:   curl -sSfL https://raw.githubusercontent.com/evilmartians/lefthook/master/install.sh | sh"
  echo "  Other:   https://github.com/evilmartians/lefthook#install"
  echo ""
  echo "Then re-run: ./scripts/setup-hooks.sh"
  exit 1
fi

echo "→ Installing lefthook hooks..."
lefthook install

echo ""
echo "✅ lefthook git hooks enabled!"
echo "   'cargo fmt -- --check' and 'cargo clippy --locked --all-targets -- -D warnings'"
echo "   will now run (in parallel) before every commit."
echo "   Failed checks will block the commit."
echo ""
echo "You can also run manually with:"
echo "   lefthook run pre-commit"
echo ""
echo "Or run individual checks:"
echo "   lefthook run pre-commit format"
echo "   lefthook run pre-commit clippy"
