#!/usr/bin/env bash
set -euo pipefail

# Deploy helper on netcup VPS. It pulls the image and (re)starts via docker compose.
# Examples:
#   ./scripts/netcup_deploy.sh --image y1nf0x/tg-file-monitor --tag latest
#   ./scripts/netcup_deploy.sh --image y1nf0x/tg-file-monitor --tag 0.4.19

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"

IMAGE_REPO=""
IMAGE_TAG="latest"

usage() {
  cat <<'EOF'
Usage:
  netcup_deploy.sh --image <repo/name> [--tag <tag>]

Required:
  --image <repo/name>   Docker image repo, e.g. y1nf0x/tg-file-monitor

Options:
  --tag <tag>           Image tag, default: latest
  -h, --help            Show this help
EOF
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

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "[ERROR] docker-compose.yml not found: $COMPOSE_FILE"
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "[ERROR] docker not found"
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "[ERROR] docker compose / docker-compose not found"
  exit 1
fi

TARGET_IMAGE="$IMAGE_REPO:$IMAGE_TAG"
log "Pull image: $TARGET_IMAGE"
docker pull "$TARGET_IMAGE"

log "Update docker-compose image reference to $TARGET_IMAGE"
sed -i -E "s|(^\s*image:\s*).*$|\1$TARGET_IMAGE|" "$COMPOSE_FILE"

log "Restart service"
"${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" up -d

log "Current service status"
"${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" ps

log "Done"
