#!/usr/bin/env bash
set -euo pipefail

# Build/publish helper for tg-file-monitor Docker image.
# Examples:
#   ./scripts/docker_release.sh --image y1nf0x/tg-file-monitor --tag 0.4.19 --push
#   ./scripts/docker_release.sh --image y1nf0x/tg-file-monitor --tag 0.4.19 --save
#   ./scripts/docker_release.sh --image y1nf0x/tg-file-monitor --tag 0.4.19 --push --platform linux/amd64

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"

IMAGE_REPO=""
IMAGE_TAG=""
PLATFORM=""
DO_PUSH="0"
DO_SAVE="0"
AUTO_BUMP_ON_DUP="1"

usage() {
  cat <<'EOF'
Usage:
  docker_release.sh --image <repo/name> --tag <version> [options]
  docker_release.sh --image <repo/name> [options]

Required:
  --image <repo/name>   Docker image repo, e.g. y1nf0x/tg-file-monitor
  --tag <version>       Image tag, e.g. 0.4.19 (optional; default from app/app.py VERSION)

Options:
  --platform <platform> Build platform, e.g. linux/amd64
  --push                Push image to registry
  --save                Export image tar.gz to dist/
  --allow-duplicate-tag Allow using an existing tag without auto-bump
  -h, --help            Show this help

Notes:
  - Always tags both <tag> and latest.
  - If --push is set, script uses buildx --push.
  - If --push is not set, script builds to local daemon.
  - By default, if <tag> already exists (git tag or registry tag), auto-bumps patch version.
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[ERROR] command not found: $1"
    exit 1
  fi
}

detect_version() {
  local version_file="$ROOT_DIR/app/app.py"
  if [[ -f "$version_file" ]]; then
    local line
    line="$(grep -E '^VERSION[[:space:]]*=' "$version_file" | head -n 1 || true)"
    if [[ -n "$line" ]]; then
      local v
      v="$(echo "$line" | sed -E 's/^[^"]*"([^"]+)".*$/\1/')"
      if [[ -n "$v" && "$v" != "$line" ]]; then
        echo "$v"
        return 0
      fi
    fi
  fi
  return 1
}

set_version() {
  local target_version="$1"
  local version_file="$ROOT_DIR/app/app.py"
  if [[ ! -f "$version_file" ]]; then
    echo "[ERROR] version file not found: $version_file" >&2
    return 1
  fi

  python3 - "$version_file" "$target_version" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
target = sys.argv[2]
text = path.read_text(encoding="utf-8")
updated, count = re.subn(r'^VERSION\s*=\s*"[^"]*"', f'VERSION = "{target}"', text, count=1, flags=re.M)
if count != 1:
    raise SystemExit("VERSION assignment not found in app/app.py")
path.write_text(updated, encoding="utf-8")
PY
}

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

is_semver_like() {
  [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

bump_patch() {
  local v="$1"
  local major minor patch
  IFS='.' read -r major minor patch <<<"$v"
  patch=$((patch + 1))
  echo "${major}.${minor}.${patch}"
}

local_tag_exists() {
  local tag="$1"
  if git rev-parse --git-dir >/dev/null 2>&1; then
    if git rev-parse -q --verify "refs/tags/v${tag}" >/dev/null 2>&1; then
      return 0
    fi
    if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null 2>&1; then
      return 0
    fi
  fi
  return 1
}

remote_tag_exists() {
  local repo="$1"
  local tag="$2"
  docker buildx imagetools inspect "${repo}:${tag}" >/dev/null 2>&1
}

resolve_non_duplicate_tag() {
  local repo="$1"
  local base_tag="$2"
  local resolved="$base_tag"
  local tries=0
  local max_tries=200

  if [[ "$AUTO_BUMP_ON_DUP" != "1" ]]; then
    echo "$resolved"
    return 0
  fi

  if ! is_semver_like "$resolved"; then
    if local_tag_exists "$resolved" || remote_tag_exists "$repo" "$resolved"; then
      echo "[ERROR] tag '$resolved' already exists and cannot auto-bump (not semver X.Y.Z)." >&2
      echo "[ERROR] Please provide a new tag or use --allow-duplicate-tag." >&2
      return 1
    fi
    echo "$resolved"
    return 0
  fi

  while local_tag_exists "$resolved" || remote_tag_exists "$repo" "$resolved"; do
    tries=$((tries + 1))
    if (( tries > max_tries )); then
      echo "[ERROR] unable to find available tag after ${max_tries} attempts from ${base_tag}" >&2
      return 1
    fi
    resolved="$(bump_patch "$resolved")"
  done

  if [[ "$resolved" != "$base_tag" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Tag '$base_tag' already exists, auto-bumped to '$resolved'" >&2
  fi

  echo "$resolved"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      IMAGE_REPO="${2:-}"
      shift 2
      ;;
    --tag)
      IMAGE_TAG="${2:-}"
      shift 2
      ;;
    --platform)
      PLATFORM="${2:-}"
      shift 2
      ;;
    --push)
      DO_PUSH="1"
      shift
      ;;
    --save)
      DO_SAVE="1"
      shift
      ;;
    --allow-duplicate-tag)
      AUTO_BUMP_ON_DUP="0"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$IMAGE_REPO" ]]; then
  echo "[ERROR] --image is required"
  usage
  exit 1
fi
if [[ -z "$IMAGE_TAG" ]]; then
  IMAGE_TAG="$(detect_version || true)"
  if [[ -n "$IMAGE_TAG" ]]; then
    log "Auto-detected version from app/app.py: $IMAGE_TAG"
  else
    echo "[ERROR] --tag is required (and VERSION not found in app/app.py)"
    usage
    exit 1
  fi
fi

require_cmd docker
require_cmd git
require_cmd python3
mkdir -p "$DIST_DIR"

IMAGE_TAG="$(resolve_non_duplicate_tag "$IMAGE_REPO" "$IMAGE_TAG")"

CURRENT_VERSION="$(detect_version || true)"
if [[ "$CURRENT_VERSION" != "$IMAGE_TAG" ]]; then
  set_version "$IMAGE_TAG"
  log "Synced app/app.py VERSION: ${CURRENT_VERSION:-unknown} -> $IMAGE_TAG"
fi

TAG_IMAGE="$IMAGE_REPO:$IMAGE_TAG"
LATEST_IMAGE="$IMAGE_REPO:latest"

log "Repository: $ROOT_DIR"
log "Build image: $TAG_IMAGE"
log "Also tag: $LATEST_IMAGE"

if [[ "$DO_PUSH" == "1" ]]; then
  require_cmd docker
  if ! docker buildx version >/dev/null 2>&1; then
    echo "[ERROR] docker buildx is required for --push mode"
    exit 1
  fi

  build_args=(buildx build "$ROOT_DIR" -f "$ROOT_DIR/Dockerfile" -t "$TAG_IMAGE" -t "$LATEST_IMAGE" --push)
  if [[ -n "$PLATFORM" ]]; then
    build_args+=(--platform "$PLATFORM")
  fi

  log "Running: docker ${build_args[*]}"
  docker "${build_args[@]}"
  log "Push completed"
else
  build_args=(build "$ROOT_DIR" -f "$ROOT_DIR/Dockerfile" -t "$TAG_IMAGE" -t "$LATEST_IMAGE")
  if [[ -n "$PLATFORM" ]]; then
    build_args+=(--platform "$PLATFORM")
  fi

  log "Running: docker ${build_args[*]}"
  docker "${build_args[@]}"
  log "Local build completed"
fi

if [[ "$DO_SAVE" == "1" ]]; then
  if [[ "$DO_PUSH" == "1" ]]; then
    log "--push mode does not load local image; pulling $TAG_IMAGE for export"
    docker pull "$TAG_IMAGE"
  fi

  ts="$(date +%Y%m%d_%H%M%S)"
  out_tar="$DIST_DIR/$(basename "$IMAGE_REPO")-${IMAGE_TAG}-${ts}.tar"
  out_tgz="${out_tar}.gz"

  log "Saving image to tar: $out_tar"
  docker save -o "$out_tar" "$TAG_IMAGE"

  log "Compressing image tar: $out_tgz"
  gzip -f "$out_tar"
  log "Export completed: $out_tgz"
fi

log "Done"
