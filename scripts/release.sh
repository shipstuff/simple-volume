#!/usr/bin/env bash
set -euo pipefail

NEW="${1:-}"
TAG_MSG="${2:-Release v${NEW}}"

if ! [[ "${NEW}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 <X.Y.Z> [\"tag message\"]" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

branch="$(git rev-parse --abbrev-ref HEAD)"
if [ "${branch}" != "main" ]; then
  echo "error: must be on main to cut a release (currently on '${branch}')" >&2
  exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "error: working tree has uncommitted changes; commit or stash first" >&2
  git status --short >&2
  exit 1
fi

if git rev-parse "v${NEW}" >/dev/null 2>&1; then
  echo "error: tag v${NEW} already exists locally" >&2
  exit 1
fi
if git ls-remote --exit-code --tags origin "v${NEW}" >/dev/null 2>&1; then
  echo "error: tag v${NEW} already exists on origin" >&2
  exit 1
fi

sed -i -E "s|^(version: )[0-9]+\\.[0-9]+\\.[0-9]+|\\1${NEW}|" helm/simple-volume/Chart.yaml
sed -i -E "s|^(appVersion: \")[0-9]+\\.[0-9]+\\.[0-9]+(\")|\\1${NEW}\\2|" helm/simple-volume/Chart.yaml
sed -i -E "s|^(  tag: \")[0-9]+\\.[0-9]+\\.[0-9]+(\")|\\1${NEW}\\2|" helm/simple-volume/values.yaml

go test ./...
helm lint ./helm/simple-volume
helm template simple-volume ./helm/simple-volume >/dev/null

git add helm/simple-volume/Chart.yaml helm/simple-volume/values.yaml
git commit -m "release: pin to v${NEW}"
git tag -a "v${NEW}" -m "${TAG_MSG}"

echo "ready to publish v${NEW}: git push --follow-tags origin main"

