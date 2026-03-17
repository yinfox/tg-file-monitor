#!/usr/bin/env python3
"""
Auto-generate the README version history summary block from existing release entries.

Usage:
  python scripts/update_version_history.py --readme README.md --max 8
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple


HEADER_RE = re.compile(r"^###\s+v(?P<ver>\d+\.\d+\.\d+)\s*\((?P<date>[^)]+)\)(?:\s*-\s*(?P<tag>.*))?$")
SECTION_RE = re.compile(r"^##\s+")
START_MARK = "<!-- AUTO-GEN:VERSION_HISTORY:START -->"
END_MARK = "<!-- AUTO-GEN:VERSION_HISTORY:END -->"


def _semver_key(version: str) -> Tuple[int, int, int]:
    parts = version.split(".")
    try:
        return (int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        return (0, 0, 0)


def _parse_entries(text: str) -> Dict[str, Dict[str, object]]:
    lines = text.splitlines()
    entries: Dict[str, Dict[str, object]] = {}
    i = 0
    while i < len(lines):
        m = HEADER_RE.match(lines[i])
        if not m:
            i += 1
            continue
        ver = m.group("ver")
        date = m.group("date").strip()
        bullets: List[str] = []
        j = i + 1
        while j < len(lines):
            if HEADER_RE.match(lines[j]) or SECTION_RE.match(lines[j]):
                break
            if lines[j].startswith("- "):
                bullets.append(lines[j])
            j += 1

        existing = entries.get(ver)
        if existing is None or len(bullets) > len(existing.get("bullets", [])):
            entries[ver] = {"ver": ver, "date": date, "bullets": bullets}
        i = j
    return entries


def _build_block(entries: Dict[str, Dict[str, object]], max_entries: int, current_label: str) -> str:
    versions = sorted(entries.keys(), key=_semver_key, reverse=True)
    versions = versions[: max_entries]
    out_lines: List[str] = []
    for idx, ver in enumerate(versions):
        data = entries[ver]
        date = data.get("date") or ""
        if idx == 0:
            out_lines.append(f"### v{ver} ({date}) - {current_label}")
        else:
            out_lines.append(f"### v{ver} ({date})")
        bullets = data.get("bullets") or []
        for line in bullets:
            out_lines.append(line)
        out_lines.append("")
    if out_lines and out_lines[-1] == "":
        out_lines.pop()
    return "\n".join(out_lines)


def _replace_block(text: str, new_block: str) -> str:
    if START_MARK not in text or END_MARK not in text:
        raise ValueError("Version history markers not found in README.")
    before, rest = text.split(START_MARK, 1)
    _old, after = rest.split(END_MARK, 1)
    return f"{before}{START_MARK}\n{new_block}\n{END_MARK}{after}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--readme", default="README.md", help="README file to update")
    parser.add_argument("--max", type=int, default=8, help="Max entries to include")
    args = parser.parse_args()

    readme_path = Path(args.readme)
    text = readme_path.read_text(encoding="utf-8")
    entries = _parse_entries(text)
    if not entries:
        raise SystemExit("No release entries found (### vX.Y.Z ...).")

    current_label = "当前版本" if "版本历史" in text else "Current"
    new_block = _build_block(entries, max_entries=max(1, args.max), current_label=current_label)
    updated = _replace_block(text, new_block)
    readme_path.write_text(updated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
