#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
COOKIES_FILE="$ROOT_DIR/config/cookies.txt"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <youtube_url> [tag]"
  exit 1
fi

URL="$1"
TAG="${2:-A}"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="/tmp/yt_ab_${TAG}_${TS}"
mkdir -p "$OUT_DIR"

echo "[INFO] output dir: $OUT_DIR"
echo "[INFO] url: $URL"

{
  echo "timestamp: $(date -Is)"
  echo "tag: $TAG"
  echo "yt-dlp version: $($PYTHON_BIN -m yt_dlp --version 2>/dev/null || echo unknown)"
  echo "public ipv4: $(curl -4s --max-time 8 ifconfig.me || echo unknown)"
  echo "public ipv6: $(curl -6s --max-time 8 ifconfig.me || echo unknown)"
  echo "cookies exists: $([[ -f "$COOKIES_FILE" ]] && echo yes || echo no)"
} > "$OUT_DIR/env.txt"

run_case() {
  local name="$1"
  shift
  local log="$OUT_DIR/${name}.log"
  echo "[INFO] running case: $name"
  "$PYTHON_BIN" -m yt_dlp -v --skip-download "$@" "$URL" > "$log" 2>&1 || true

  {
    echo "=== $name ==="
    grep -E "playability status|Sign in to confirm|No supported JavaScript runtime|ERROR:|WARNING:" "$log" || true
    echo
  } >> "$OUT_DIR/summary.txt"
}

if [[ -f "$COOKIES_FILE" ]]; then
  run_case "with_cookies_default" --cookies "$COOKIES_FILE"
  run_case "with_cookies_impersonate" --impersonate chrome --cookies "$COOKIES_FILE"
  run_case "with_cookies_tv" --cookies "$COOKIES_FILE" --extractor-args "youtube:player_client=tv_downgraded"
else
  echo "[WARN] cookies file not found, skipping cookie cases" | tee -a "$OUT_DIR/summary.txt"
fi

run_case "no_cookies_tv" --extractor-args "youtube:player_client=tv_downgraded"

echo "[INFO] done. summary: $OUT_DIR/summary.txt"
cat "$OUT_DIR/summary.txt"
