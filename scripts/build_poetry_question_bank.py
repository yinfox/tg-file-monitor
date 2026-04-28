#!/usr/bin/env python3
"""Build the poetry red-packet lookup database from chinese-poetry JSON files."""

import argparse
import glob
import json
import os
import re
import sqlite3
import tempfile
import time

try:
    from opencc import OpenCC
except Exception:  # pragma: no cover - generation quality fallback
    OpenCC = None


DEFAULT_ROOTS = [
    "五代诗词",
    "元曲",
    "全唐诗",
    "四书五经",
    "宋词",
    "幽梦影",
    "御定全唐詩",
    "曹操诗集",
    "楚辞",
    "水墨唐诗",
    "纳兰性德",
    "蒙学",
    "论语",
    "诗经",
]

SKIP_KEYS = {
    "paragraphs",
    "content",
    "title",
    "author",
    "id",
    "dynasty",
    "chapter",
    "section",
    "rhythmic",
    "notes",
    "tags",
}

CHINESE_RE = re.compile(r"[\u4e00-\u9fff]+")


def iter_body_texts(obj):
    if isinstance(obj, dict):
        for key in ("paragraphs", "content"):
            value = obj.get(key)
            if isinstance(value, str):
                yield value
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        yield item
                    elif isinstance(item, (dict, list)):
                        yield from iter_body_texts(item)
        for key, value in obj.items():
            if key in SKIP_KEYS:
                continue
            if isinstance(value, (dict, list)):
                yield from iter_body_texts(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_body_texts(item)


def add_phrases(raw_text, phrases, convert):
    if not raw_text:
        return
    text = convert(str(raw_text))
    parts = CHINESE_RE.findall(text)
    if not parts:
        return

    full = "".join(parts)
    if 2 <= len(full) <= 64:
        phrases.add(full)

    for part in parts:
        if 2 <= len(part) <= 32:
            phrases.add(part)

    for first, second in zip(parts, parts[1:]):
        combined = first + second
        if 4 <= len(combined) <= 48:
            phrases.add(combined)


def collect_json_files(source_dir, roots):
    files = []
    for root in roots:
        files.extend(glob.glob(os.path.join(source_dir, root, "**", "*.json"), recursive=True))
    return sorted(files)


def build_phrases(source_dir, roots):
    if OpenCC is not None:
        converter = OpenCC("t2s")
        convert = converter.convert
    else:
        print("WARN: opencc is not installed; generated bank will not convert traditional Chinese to simplified.")
        convert = lambda value: value

    phrases = set()
    files = collect_json_files(source_dir, roots)
    bad_files = 0
    body_text_count = 0
    started = time.time()

    for path in files:
        if os.path.basename(path).startswith("author."):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception as exc:
            bad_files += 1
            print(f"WARN: skip unreadable json {path}: {exc}")
            continue
        for text in iter_body_texts(data):
            body_text_count += 1
            add_phrases(text, phrases, convert)

    elapsed = time.time() - started
    print(
        f"collected files={len(files)} body_texts={body_text_count} "
        f"phrases={len(phrases)} bad_files={bad_files} seconds={elapsed:.1f}"
    )
    return sorted(phrases), {
        "source": "https://github.com/chinese-poetry/chinese-poetry",
        "source_roots": ",".join(roots),
        "phrase_count": str(len(phrases)),
        "body_text_count": str(body_text_count),
        "bad_file_count": str(bad_files),
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "traditional_to_simplified": str(OpenCC is not None).lower(),
    }


def write_sqlite(output_path, phrases, metadata):
    output_dir = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(output_dir, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".poetry_question_bank.", suffix=".sqlite", dir=output_dir)
    os.close(fd)
    try:
        conn = sqlite3.connect(temp_path)
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("CREATE TABLE phrases (phrase TEXT PRIMARY KEY) WITHOUT ROWID")
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID")

        batch_size = 10000
        for offset in range(0, len(phrases), batch_size):
            batch = phrases[offset:offset + batch_size]
            conn.executemany("INSERT INTO phrases(phrase) VALUES (?)", ((phrase,) for phrase in batch))

        conn.executemany("INSERT INTO meta(key, value) VALUES (?, ?)", sorted(metadata.items()))
        conn.commit()
        conn.execute("VACUUM")
        conn.close()
        os.replace(temp_path, output_path)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise


def main():
    parser = argparse.ArgumentParser(description="Build poetry red-packet SQLite question bank.")
    parser.add_argument("--source", required=True, help="Path to a checked-out chinese-poetry repository.")
    parser.add_argument(
        "--output",
        default=os.path.join("app", "data", "poetry_question_bank.sqlite"),
        help="Output SQLite path.",
    )
    parser.add_argument(
        "--roots",
        nargs="*",
        default=DEFAULT_ROOTS,
        help="Top-level chinese-poetry directories to include.",
    )
    args = parser.parse_args()

    phrases, metadata = build_phrases(args.source, args.roots)
    write_sqlite(args.output, phrases, metadata)
    size_mb = os.path.getsize(args.output) / 1024 / 1024
    print(f"wrote {args.output} ({size_mb:.1f} MiB)")


if __name__ == "__main__":
    main()
