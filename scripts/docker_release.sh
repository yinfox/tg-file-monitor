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

usage() {
  cat <<'EOF'
Usage:
  docker_release.sh --image <repo/name> --tag <version> [options]

Required:
  --image <repo/name>   Docker image repo, e.g. y1nf0x/tg-file-monitor
  --tag <version>       Image tag, e.g. 0.4.19

Options:
  --platform <platform> Build platform, e.g. linux/amd64
  --push                Push image to registry
  --save                Export image tar.gz to dist/
  -h, --help            Show this help

Notes:
  - Always tags both <tag> and latest.
  - If --push is set, script uses buildx --push.
  - If --push is not set, script builds to local daemon.
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[ERROR] command not found: $1"
    exit 1
  fi
}

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
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

if [[ -z "$IMAGE_REPO" || -z "$IMAGE_TAG" ]]; then
  echo "[ERROR] --image and --tag are required"
  usage
  exit 1
fi

require_cmd docker
mkdir -p "$DIST_DIR"

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
