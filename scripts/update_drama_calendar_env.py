#!/usr/bin/env python3
import argparse
import datetime
import html
import json
import os
import re
import shutil
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin

import requests

HOME_URL = "https://blog.922928.de/"
MAOYAN_BOX_OFFICE_URL = "https://piaofang.maoyan.com/box-office?ver=normal"
MAOYAN_WEB_HEAT_URL = "https://piaofang.maoyan.com/web-heat"
DOUBAN_AMERICAN_TV_URL = "https://m.douban.com/subject_collection/tv_american"
DOUBAN_KOREAN_TV_URL = "https://m.douban.com/subject_collection/tv_korean"
DOUBAN_JAPANESE_TV_URL = "https://m.douban.com/subject_collection/tv_japanese"
DOUBAN_DOMESTIC_TV_URL = "https://m.douban.com/subject_collection/tv_domestic"
DOUBAN_VARIETY_SHOW_URL = "https://m.douban.com/subject_collection/tv_variety_show"
DOUBAN_ANIMATION_URL = "https://m.douban.com/subject_collection/tv_animation"
DEFAULT_STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "drama_calendar_state.json")
DEFAULT_TMDB_CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "tmdb_tv_status_cache.json")
FINISH_KEYWORDS = ("完结", "收官", "大结局")
FINISH_EXCLUDE_KEYWORDS = (
    "未完结",
    "未收官",
    "未大结局",
    "即将完结",
    "即将收官",
    "将完结",
    "将收官",
)
TMDB_FINISHED_STATUSES = {"ended", "canceled", "cancelled"}
TMDB_MIN_CONFIDENCE_SCORE = 70
TMDB_MAX_WORKERS_DEFAULT = max(1, min(8, int(os.environ.get("DRAMA_TMDB_MAX_WORKERS", "6") or 6)))
SOURCE_CHOICES = ("calendar", "maoyan", "douban", "douban_asia", "douban_domestic", "douban_variety", "douban_animation", "all")
_TMDB_CACHE_LOCK = threading.Lock()


def _normalize_sources(raw_sources: Sequence[str]) -> List[str]:
    normalized: List[str] = []
    seen: Set[str] = set()
    for raw in raw_sources or []:
        for item in str(raw or "").split(","):
            source = item.strip()
            if not source:
                continue
            if source not in SOURCE_CHOICES:
                raise ValueError(f"不支持的数据源: {source}")
            if source == "all":
                return ["all"]
            if source not in seen:
                normalized.append(source)
                seen.add(source)
    return normalized or ["calendar"]


def _normalize_douban_collection_url(raw_url: str, fallback_url: str = DOUBAN_AMERICAN_TV_URL) -> str:
    text = (raw_url or "").strip()
    if not text:
        return fallback_url

    url_matches = re.findall(r"https?://m\.douban\.com/subject_collection/[^,\s?#]+", text, flags=re.IGNORECASE)
    if url_matches:
        return url_matches[0]

    slug_match = re.search(r"/subject_collection/([^,\s/?#]+)", text)
    if slug_match:
        return f"https://m.douban.com/subject_collection/{slug_match.group(1).strip()}"

    return fallback_url


def _parse_keyword_filters(raw_keywords: str) -> List[str]:
    if not raw_keywords:
        return []
    items: List[str] = []
    seen: Set[str] = set()
    for part in re.split(r"[\n,]+", str(raw_keywords or "")):
        keyword = _clean_extracted_title(part)
        if not keyword or keyword in seen:
            continue
        items.append(keyword)
        seen.add(keyword)
    return items


def _apply_source_keyword_filters(
    titles: Sequence[str],
    *,
    source_name: str,
    whitelist_keywords: Sequence[str],
    blacklist_keywords: Sequence[str],
) -> Tuple[List[str], List[Tuple[str, str]], List[Tuple[str, str]]]:
    whitelist = [str(item).strip() for item in (whitelist_keywords or []) if str(item).strip()]
    blacklist = [str(item).strip() for item in (blacklist_keywords or []) if str(item).strip()]
    if not whitelist and not blacklist:
        return list(titles), [], []

    kept: List[str] = []
    removed_by_whitelist: List[Tuple[str, str]] = []
    removed_by_blacklist: List[Tuple[str, str]] = []

    for title in titles:
        current = str(title or "").strip()
        if not current:
            continue
        if whitelist:
            matched_whitelist = next((keyword for keyword in whitelist if keyword in current), "")
            if not matched_whitelist:
                removed_by_whitelist.append((current, "未命中任何白名单"))
                continue
        matched_blacklist = next((keyword for keyword in blacklist if keyword in current), "")
        if matched_blacklist:
            removed_by_blacklist.append((current, matched_blacklist))
            continue
        kept.append(current)

    print(
        f"[INFO] {source_name} 过滤: whitelist={len(whitelist)} blacklist={len(blacklist)} "
        f"removed_by_whitelist={len(removed_by_whitelist)} removed_by_blacklist={len(removed_by_blacklist)} kept={len(kept)}"
    )
    if removed_by_whitelist:
        for item, reason in removed_by_whitelist[:10]:
            print(f"  - whitelist剔除: {item} [原因: {reason}]")
    if removed_by_blacklist:
        for item, reason in removed_by_blacklist[:10]:
            print(f"  - blacklist剔除: {item} [命中: {reason}]")
    return kept, removed_by_whitelist, removed_by_blacklist


def _clean_extracted_title(title: str) -> str:
    s = (title or "").strip()
    if not s:
        return ""
    s = html.unescape(s)
    s = s.replace("\u3000", " ")
    # 统一去掉尾部季数后缀：例如“极乐凶间 第二季” -> “极乐凶间”。
    s = re.sub(r"\s+第[一二三四五六七八九十百零两0-9]+季$", "", s).strip()
    # 去掉常见装饰性括号和特殊符号，只保留中英文、数字与空格。
    s = re.sub(r"[`~!@#$%^&*=|\\\\/;\"'<>,.?•…\-—_]", "", s)
    s = re.sub(r"[！＠＃￥％……＆＊［\[\]］【】｛{}｝「」『』《》〈〉〔〕〖〗“”‘’、，。；？～｜]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_date_from_post_url(post_url: str) -> Optional[datetime.date]:
    if not post_url:
        return None
    m = re.search(r"/(\d{4})-(\d{2})-(\d{2})-", post_url)
    if not m:
        return None
    try:
        return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return None


def parse_date_from_line(line: str, fallback_year: int) -> Optional[datetime.date]:
    if not line:
        return None

    m = re.search(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})", line)
    if m:
        try:
            return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            pass

    m = re.search(r"(\d{1,2})月(\d{1,2})日", line)
    if m:
        try:
            return datetime.date(int(fallback_year), int(m.group(1)), int(m.group(2)))
        except Exception:
            pass

    return None


def _normalize_title_for_match(title: str) -> str:
    t = (title or "").strip().lower()
    t = re.sub(r"[\s\-_:：·\.\,，。\(\)\[\]【】]+", "", t)
    return t


def _parse_title_alias_map(raw: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not raw:
        return mapping
    for line in str(raw).splitlines():
        text = line.strip()
        if not text:
            continue
        sep = None
        for token in ("=>", "->", "=", "：", ":"):
            if token in text:
                sep = token
                break
        if not sep:
            continue
        left, right = text.split(sep, 1)
        key = _clean_extracted_title(left)
        val = _clean_extracted_title(right)
        if not key or not val:
            continue
        mapping[_normalize_title_for_match(key)] = val
    return mapping


def _apply_title_aliases(titles: Sequence[str], alias_map: Dict[str, str]) -> Tuple[List[str], int]:
    if not titles:
        return [], 0
    if not alias_map:
        return list(titles), 0
    replaced = 0
    result: List[str] = []
    for title in titles:
        norm = _normalize_title_for_match(title)
        alias = alias_map.get(norm)
        if alias:
            if alias != title:
                replaced += 1
            result.append(alias)
        else:
            result.append(title)
    return result, replaced


def _load_tmdb_cache(cache_file: str) -> Dict[str, Any]:
    if not cache_file or not os.path.exists(cache_file):
        return {"items": {}}
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("items"), dict):
            return data
    except Exception:
        pass
    return {"items": {}}


def _save_tmdb_cache(cache_file: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(cache_file) or ".", exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _tmdb_pick_best_result(
    title: str,
    results: Sequence[Dict[str, Any]],
    *,
    preferred_region: str,
    reference_year: Optional[int],
    year_tolerance: int,
) -> Tuple[Optional[Dict[str, Any]], int, str]:
    if not results:
        return None, 0, "no_results"
    norm_title = _normalize_title_for_match(title)
    pref_region = (preferred_region or "").strip().upper()
    year_tol = max(0, int(year_tolerance or 0))

    def _score(item: Dict[str, Any]) -> Tuple[int, str]:
        name = str(item.get("name") or item.get("original_name") or "")
        norm_name = _normalize_title_for_match(name)
        if not norm_name:
            return 0, "empty_name"

        score = 0
        reasons: List[str] = []
        if norm_name == norm_title:
            score += 120
            reasons.append("exact_title")
        elif norm_title and norm_title in norm_name:
            score += 80
            reasons.append("title_contains")
        elif norm_name and norm_name in norm_title:
            score += 70
            reasons.append("title_contained")
        else:
            score += 10
            reasons.append("weak_title")

        origin_countries = item.get("origin_country") if isinstance(item.get("origin_country"), list) else []
        origin_countries = [str(c).upper() for c in origin_countries if str(c).strip()]
        if pref_region and pref_region in origin_countries:
            score += 20
            reasons.append("region_match")

        candidate_date = _parse_iso_date(str(item.get("first_air_date") or item.get("release_date") or ""))
        if reference_year and isinstance(candidate_date, datetime.date):
            diff = abs(candidate_date.year - int(reference_year))
            if diff == 0:
                score += 35
                reasons.append("year_exact")
            elif diff == 1:
                score += 25
                reasons.append("year_close")
            elif diff <= year_tol:
                score += 15
                reasons.append("year_tolerated")
            else:
                score -= 25
                reasons.append("year_far")

        popularity = float(item.get("popularity") or 0)
        if popularity > 0:
            score += min(10, int(popularity // 5))

        return score, "+".join(reasons)

    ranked_with_score: List[Tuple[Dict[str, Any], int, str]] = []
    for r in results:
        s, reason = _score(r)
        ranked_with_score.append((r, s, reason))
    ranked_with_score.sort(key=lambda x: x[1], reverse=True)
    if not ranked_with_score:
        return None, 0, "not_ranked"
    best, best_score, best_reason = ranked_with_score[0]
    return best, best_score, best_reason


def _parse_iso_date(value: str) -> Optional[datetime.date]:
    if not value:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", str(value).strip())
    if not m:
        return None
    try:
        return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return None


def _tmdb_fetch_finish_state(
    title: str,
    api_key: str,
    language: str,
    region: str,
    timeout: int,
    reference_year: Optional[int],
    year_tolerance: int,
    min_confidence_score: int,
) -> Tuple[Optional[bool], Optional[datetime.date], str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; drama-calendar-bot/1.0)",
        "Accept": "application/json",
    }
    search_resp = requests.get(
        "https://api.themoviedb.org/3/search/tv",
        params={
            "api_key": api_key,
            "query": title,
            "language": language,
            "region": region,
            "include_adult": "false",
        },
        headers=headers,
        timeout=timeout,
    )
    search_resp.raise_for_status()
    payload = search_resp.json() if search_resp.content else {}
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return None, None, "tmdb_no_match"

    picked, score, score_reason = _tmdb_pick_best_result(
        title,
        results,
        preferred_region=region,
        reference_year=reference_year,
        year_tolerance=year_tolerance,
    )
    if not picked or not picked.get("id"):
        return None, None, "tmdb_no_match"
    if score < int(min_confidence_score or TMDB_MIN_CONFIDENCE_SCORE):
        return None, None, f"tmdb_low_confidence(score={score},reason={score_reason})"

    tv_id = int(picked.get("id"))
    detail_resp = requests.get(
        f"https://api.themoviedb.org/3/tv/{tv_id}",
        params={
            "api_key": api_key,
            "language": language,
        },
        headers=headers,
        timeout=timeout,
    )
    detail_resp.raise_for_status()
    detail = detail_resp.json() if detail_resp.content else {}
    status_raw = str(detail.get("status") or "").strip().lower()
    last_air_date = _parse_iso_date(str(detail.get("last_air_date") or ""))
    is_finished = status_raw in TMDB_FINISHED_STATUSES
    return is_finished, last_air_date, f"tmdb_status={status_raw or 'unknown'},score={score},reason={score_reason}"


def _resolve_tmdb_finish_state(
    title: str,
    *,
    api_key: str,
    language: str,
    region: str,
    timeout: int,
    cache: Dict[str, Any],
    cache_ttl_hours: int,
    reference_year: Optional[int],
    year_tolerance: int,
    min_confidence_score: int,
) -> Tuple[Optional[bool], Optional[datetime.date], str]:
    cache_key = f"{language}|{region}|{reference_year or 0}|{title}"
    now_ts = datetime.datetime.now().timestamp()
    ttl_seconds = max(1, int(cache_ttl_hours)) * 3600

    with _TMDB_CACHE_LOCK:
        cache_items = cache.setdefault("items", {})
        hit = cache_items.get(cache_key)
    if isinstance(hit, dict):
        ts = float(hit.get("ts") or 0)
        if ts > 0 and (now_ts - ts) < ttl_seconds:
            cached_finished = hit.get("finished")
            cached_date = _parse_iso_date(str(hit.get("finished_date") or ""))
            cached_note = str(hit.get("note") or "tmdb_cache")
            if isinstance(cached_finished, bool) or cached_finished is None:
                print(f"[INFO] TMDB 缓存命中: title={title} result={cached_finished} date={(cached_date.isoformat() if isinstance(cached_date, datetime.date) else '')}")
                return cached_finished, cached_date, cached_note

    print(f"[INFO] TMDB 发起请求: title={title} language={language} region={region} ref_year={reference_year or 0}")
    finished, finished_date, note = _tmdb_fetch_finish_state(
        title,
        api_key=api_key,
        language=language,
        region=region,
        timeout=timeout,
        reference_year=reference_year,
        year_tolerance=year_tolerance,
        min_confidence_score=min_confidence_score,
    )

    with _TMDB_CACHE_LOCK:
        cache_items = cache.setdefault("items", {})
        cache_items[cache_key] = {
            "ts": now_ts,
            "finished": finished,
            "finished_date": (finished_date.isoformat() if isinstance(finished_date, datetime.date) else ""),
            "note": note,
        }
    return finished, finished_date, note


def _tmdb_fetch_movie_release_date(
    title: str,
    api_key: str,
    language: str,
    region: str,
    timeout: int,
    reference_year: Optional[int],
    year_tolerance: int,
    min_confidence_score: int,
) -> Tuple[Optional[datetime.date], str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; drama-calendar-bot/1.0)",
        "Accept": "application/json",
    }
    search_resp = requests.get(
        "https://api.themoviedb.org/3/search/movie",
        params={
            "api_key": api_key,
            "query": title,
            "language": language,
            "region": region,
            "include_adult": "false",
        },
        headers=headers,
        timeout=timeout,
    )
    search_resp.raise_for_status()
    payload = search_resp.json() if search_resp.content else {}
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return None, "tmdb_movie_no_match"

    picked, score, score_reason = _tmdb_pick_best_result(
        title,
        results,
        preferred_region=region,
        reference_year=reference_year,
        year_tolerance=year_tolerance,
    )
    if not picked or not picked.get("id"):
        return None, "tmdb_movie_no_match"
    if score < int(min_confidence_score or TMDB_MIN_CONFIDENCE_SCORE):
        return None, f"tmdb_movie_low_confidence(score={score},reason={score_reason})"

    movie_id = int(picked.get("id"))
    detail_resp = requests.get(
        f"https://api.themoviedb.org/3/movie/{movie_id}",
        params={
            "api_key": api_key,
            "language": language,
        },
        headers=headers,
        timeout=timeout,
    )
    detail_resp.raise_for_status()
    detail = detail_resp.json() if detail_resp.content else {}
    release_date = _parse_iso_date(str(detail.get("release_date") or ""))
    return release_date, f"tmdb_movie_release(score={score},reason={score_reason})"


def _resolve_tmdb_movie_release_date(
    title: str,
    *,
    api_key: str,
    language: str,
    region: str,
    timeout: int,
    cache: Dict[str, Any],
    cache_ttl_hours: int,
    reference_year: Optional[int],
    year_tolerance: int,
    min_confidence_score: int,
) -> Tuple[Optional[datetime.date], str]:
    cache_key = f"movie|{language}|{region}|{reference_year or 0}|{title}"
    now_ts = datetime.datetime.now().timestamp()
    ttl_seconds = max(1, int(cache_ttl_hours)) * 3600

    with _TMDB_CACHE_LOCK:
        cache_items = cache.setdefault("items", {})
        hit = cache_items.get(cache_key)
    if isinstance(hit, dict):
        ts = float(hit.get("ts") or 0)
        if ts > 0 and (now_ts - ts) < ttl_seconds:
            cached_date = _parse_iso_date(str(hit.get("release_date") or ""))
            cached_note = str(hit.get("note") or "tmdb_movie_cache")
            print(f"[INFO] TMDB 电影缓存命中: title={title} release={(cached_date.isoformat() if isinstance(cached_date, datetime.date) else '')}")
            return cached_date, cached_note

    print(f"[INFO] TMDB 电影发起请求: title={title} language={language} region={region} ref_year={reference_year or 0}")
    release_date, note = _tmdb_fetch_movie_release_date(
        title,
        api_key=api_key,
        language=language,
        region=region,
        timeout=timeout,
        reference_year=reference_year,
        year_tolerance=year_tolerance,
        min_confidence_score=min_confidence_score,
    )

    with _TMDB_CACHE_LOCK:
        cache_items = cache.setdefault("items", {})
        cache_items[cache_key] = {
            "ts": now_ts,
            "release_date": (release_date.isoformat() if isinstance(release_date, datetime.date) else ""),
            "note": note,
        }
    return release_date, note


def _resolve_tmdb_finish_state_batch(
    title_specs: Sequence[Tuple[str, Optional[int]]],
    *,
    api_key: str,
    language: str,
    region: str,
    timeout: int,
    cache: Dict[str, Any],
    cache_ttl_hours: int,
    year_tolerance: int,
    min_confidence_score: int,
    max_workers: int,
) -> Tuple[Dict[Tuple[str, int], Tuple[Optional[bool], Optional[datetime.date], str]], Dict[Tuple[str, int], Exception], bool]:
    ordered_specs: List[Tuple[str, int]] = []
    seen_specs: Set[Tuple[str, int]] = set()
    for title, reference_year in title_specs:
        key = (str(title or "").strip(), int(reference_year or 0))
        if not key[0] or key in seen_specs:
            continue
        ordered_specs.append(key)
        seen_specs.add(key)

    if not ordered_specs:
        return {}, {}, False

    resolved: Dict[Tuple[str, int], Tuple[Optional[bool], Optional[datetime.date], str]] = {}
    errors: Dict[Tuple[str, int], Exception] = {}
    worker_count = max(1, min(int(max_workers or 1), len(ordered_specs)))

    def _task(spec_key: Tuple[str, int]):
        title, reference_year = spec_key
        return spec_key, _resolve_tmdb_finish_state(
            title,
            api_key=api_key,
            language=language,
            region=region,
            timeout=timeout,
            cache=cache,
            cache_ttl_hours=cache_ttl_hours,
            reference_year=(reference_year or None),
            year_tolerance=year_tolerance,
            min_confidence_score=min_confidence_score,
        )

    if worker_count == 1:
        for spec_key in ordered_specs:
            try:
                key, result = _task(spec_key)
                resolved[key] = result
            except Exception as e:
                errors[spec_key] = e
        return resolved, errors, True

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="tmdb-tv") as executor:
        future_map = {executor.submit(_task, spec_key): spec_key for spec_key in ordered_specs}
        for future in as_completed(future_map):
            spec_key = future_map[future]
            try:
                key, result = future.result()
                resolved[key] = result
            except Exception as e:
                errors[spec_key] = e

    return resolved, errors, True


def _resolve_tmdb_movie_release_date_batch(
    titles: Sequence[str],
    *,
    api_key: str,
    language: str,
    region: str,
    timeout: int,
    cache: Dict[str, Any],
    cache_ttl_hours: int,
    year_tolerance: int,
    min_confidence_score: int,
    max_workers: int,
) -> Tuple[Dict[str, Tuple[Optional[datetime.date], str]], Dict[str, Exception], bool]:
    ordered_titles: List[str] = []
    seen_titles: Set[str] = set()
    for title in titles:
        normalized = str(title or "").strip()
        if not normalized or normalized in seen_titles:
            continue
        ordered_titles.append(normalized)
        seen_titles.add(normalized)

    if not ordered_titles:
        return {}, {}, False

    resolved: Dict[str, Tuple[Optional[datetime.date], str]] = {}
    errors: Dict[str, Exception] = {}
    worker_count = max(1, min(int(max_workers or 1), len(ordered_titles)))

    def _task(title: str):
        return title, _resolve_tmdb_movie_release_date(
            title,
            api_key=api_key,
            language=language,
            region=region,
            timeout=timeout,
            cache=cache,
            cache_ttl_hours=cache_ttl_hours,
            reference_year=None,
            year_tolerance=year_tolerance,
            min_confidence_score=min_confidence_score,
        )

    if worker_count == 1:
        for title in ordered_titles:
            try:
                key, result = _task(title)
                resolved[key] = result
            except Exception as e:
                errors[title] = e
        return resolved, errors, True

    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="tmdb-movie") as executor:
        future_map = {executor.submit(_task, title): title for title in ordered_titles}
        for future in as_completed(future_map):
            title = future_map[future]
            try:
                key, result = future.result()
                resolved[key] = result
            except Exception as e:
                errors[title] = e

    return resolved, errors, True


def fetch_text(url: str, timeout: int = 20) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; drama-calendar-bot/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or resp.encoding or "utf-8"
    return resp.text


def find_latest_calendar_post_url(home_html: str, home_url: str = HOME_URL) -> str:
    # 优先匹配标题中带“追剧日历”的文章链接。
    pattern = re.compile(
        r'<a[^>]+href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )

    seen: Set[str] = set()
    for m in pattern.finditer(home_html):
        raw_title = strip_html(m.group("title"))
        if "追剧日历" not in raw_title:
            continue

        href = (m.group("href") or "").strip()
        if not href:
            continue
        if href.lower().startswith("javascript:"):
            continue

        full_url = urljoin(home_url, href)
        parsed_path = re.sub(r"https?://[^/]+", "", full_url)
        if not re.search(r"/\d{4}-\d{2}-\d{2}-", parsed_path):
            continue
        if full_url in seen:
            continue
        seen.add(full_url)
        return full_url

    raise RuntimeError("未在首页找到包含“追剧日历”的文章链接")


def strip_html(raw_html: str) -> str:
    if not raw_html:
        return ""

    text = raw_html
    text = re.sub(r"<\s*br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</\s*(p|div|li|h[1-6]|tr|section|article)\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<script[\s\S]*?</script>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\u3000", " ")
    return text


def extract_titles_from_text(text: str, include_keywords: Sequence[str]) -> Tuple[List[str], List[str]]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    target_lines: List[str] = []
    titles: List[str] = []
    seen_titles: Set[str] = set()

    for line in lines:
        if not any(kw in line for kw in include_keywords):
            continue

        target_lines.append(line)
        for title in re.findall(r"《([^》]+)》", line):
            cleaned = _clean_extracted_title(title)
            if not cleaned:
                continue
            if cleaned in seen_titles:
                continue
            seen_titles.add(cleaned)
            titles.append(cleaned)

    return titles, target_lines


def extract_calendar_title_states(
    text: str,
    include_keywords: Sequence[str],
    post_date: Optional[datetime.date],
    finish_keywords: Sequence[str],
    finish_exclude_keywords: Sequence[str],
) -> Tuple[List[str], List[str], Dict[str, Dict[str, object]]]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    target_lines: List[str] = []
    title_states: Dict[str, Dict[str, object]] = {}

    fallback_year = (post_date.year if post_date else datetime.date.today().year)
    fallback_finish_date = post_date

    def _contains_any(line_text: str, words: Sequence[str]) -> bool:
        return any(w and (w in line_text) for w in words)

    def _line_marks_finished(line_text: str) -> bool:
        if not _contains_any(line_text, finish_keywords):
            return False
        if _contains_any(line_text, finish_exclude_keywords):
            return False
        return True

    for line in lines:
        has_include_kw = any(kw in line for kw in include_keywords)
        has_finish_kw = _line_marks_finished(line)
        if not has_include_kw and not has_finish_kw:
            continue

        line_titles = [_clean_extracted_title(t) for t in re.findall(r"《([^》]+)》", line)]
        line_titles = [t for t in line_titles if t]
        if not line_titles:
            continue

        if has_include_kw:
            target_lines.append(line)

        include_line_date = parse_date_from_line(line, fallback_year) or post_date

        finished_date = parse_date_from_line(line, fallback_year) or fallback_finish_date

        for title in line_titles:
            state = title_states.get(title)
            if not state:
                state = {
                    "finished": False,
                    "finished_date": None,
                    "seen_in_include_line": False,
                    "reference_date": None,
                }
                title_states[title] = state

            if has_include_kw:
                state["seen_in_include_line"] = True
                if include_line_date:
                    existing_ref = state.get("reference_date")
                    if isinstance(existing_ref, datetime.date):
                        if include_line_date > existing_ref:
                            state["reference_date"] = include_line_date
                    else:
                        state["reference_date"] = include_line_date

            if has_finish_kw:
                state["finished"] = True
                if finished_date:
                    existing = state.get("finished_date")
                    if isinstance(existing, datetime.date):
                        if finished_date > existing:
                            state["finished_date"] = finished_date
                    else:
                        state["finished_date"] = finished_date

    ordered_titles: List[str] = []
    seen: Set[str] = set()
    for line in lines:
        for title in re.findall(r"《([^》]+)》", line):
            cleaned = _clean_extracted_title(title)
            if not cleaned or cleaned in seen:
                continue
            st = title_states.get(cleaned)
            if st and bool(st.get("seen_in_include_line")):
                ordered_titles.append(cleaned)
                seen.add(cleaned)

    return ordered_titles, target_lines, title_states


def extract_maoyan_movie_names(raw_html: str) -> List[str]:
    names: List[str] = []
    seen: Set[str] = set()

    # 1) Prefer structured fields if present.
    for m in re.findall(r'"movieName"\s*:\s*"(.*?)"', raw_html, flags=re.IGNORECASE):
        name = _clean_extracted_title(html.unescape(m))
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)

    # 2) Fallback from visible text lines, e.g. "飞驰人生3 上映23天..."
    plain = strip_html(raw_html)
    for line in [ln.strip() for ln in plain.splitlines() if ln.strip()]:
        mm = re.match(r'^(.+?)\s+上映\d+天', line)
        if not mm:
            continue
        name = _clean_extracted_title(mm.group(1).strip().strip('|').strip())
        if not name:
            continue
        # Skip obvious non-title rows.
        if name in {"票房排名", "今天", "票房", "排片", "上映日历", "影库", "影院"}:
            continue
        if name in seen:
            continue
        seen.add(name)
        names.append(name)

    return names


def extract_maoyan_web_heat_names(raw_html: str) -> List[str]:
    names: List[str] = []
    seen: Set[str] = set()

    blocked_names = {
        "网播热度", "电视剧", "热播", "电影", "综艺", "动漫", "纪录片",
        "今日", "昨日", "排名", "热度", "趋势", "全部", "更多",
    }

    for m in re.findall(r'"name"\s*:\s*"(.*?)"', raw_html, flags=re.IGNORECASE):
        name = _clean_extracted_title(html.unescape(m))
        if not name or name in seen or name in blocked_names:
            continue
        if len(name) <= 1:
            continue
        if re.fullmatch(r"\d+", name):
            continue
        seen.add(name)
        names.append(name)

    return names


def extract_douban_collection_titles(raw_html: str) -> List[str]:
    names: List[str] = []
    seen: Set[str] = set()

    blocked_names = {
        "豆瓣", "豆瓣电影", "豆瓣评分", "全部", "更多", "加载中", "立即打开",
        "电视剧", "电影", "综艺", "动漫", "纪录片", "热门", "最新",
    }

    for m in re.findall(r'"title"\s*:\s*"(.*?)"', raw_html, flags=re.IGNORECASE):
        name = _clean_extracted_title(html.unescape(m))
        if not name or name in blocked_names:
            continue
        if len(name) <= 1:
            continue
        if re.fullmatch(r"\d+", name):
            continue
        if name in seen:
            continue
        seen.add(name)
        names.append(name)

    if names:
        return names

    # Fallback: parse anchor text that points to /subject/<id>/
    for m in re.findall(r'<a[^>]+href=["\'][^"\']*/subject/\d+/?[^"\']*["\'][^>]*>(.*?)</a>', raw_html, flags=re.IGNORECASE | re.DOTALL):
        name = _clean_extracted_title(strip_html(m).strip())
        if not name or name in blocked_names:
            continue
        if len(name) <= 1:
            continue
        if re.fullmatch(r"\d+", name):
            continue
        if name in seen:
            continue
        seen.add(name)
        names.append(name)

    return names


def fetch_douban_collection_titles(douban_url: str, *, count: int, timeout: int = 20) -> List[str]:
    douban_url = _normalize_douban_collection_url(douban_url or "", fallback_url=DOUBAN_AMERICAN_TV_URL)
    m = re.search(r"/subject_collection/([^,\s/?#]+)", douban_url or "")
    if not m:
        raise RuntimeError(f"无效的豆瓣合集 URL: {douban_url}")

    slug = m.group(1).strip()
    api_url = f"https://m.douban.com/rexxar/api/v2/subject_collection/{slug}/items"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; drama-calendar-bot/1.0)",
        "Referer": (douban_url or f"https://m.douban.com/subject_collection/{slug}"),
        "Accept": "application/json",
    }
    req_count = max(1, min(200, int(count or 50)))
    resp = requests.get(
        api_url,
        params={"start": 0, "count": req_count},
        headers=headers,
        timeout=timeout,
    )
    resp.raise_for_status()

    payload = resp.json() if resp.content else {}
    items = payload.get("subject_collection_items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return []

    names: List[str] = []
    seen: Set[str] = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        name = _clean_extracted_title(str(it.get("title") or ""))
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def merge_unique(primary: Sequence[str], secondary: Sequence[str]) -> List[str]:
    result: List[str] = []
    seen: Set[str] = set()
    for item in list(primary) + list(secondary):
        s = (item or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        result.append(s)
    return result


def dedupe_titles_normalized(items: Sequence[str]) -> List[str]:
    result: List[str] = []
    seen_norm: Set[str] = set()
    for item in items:
        s = (item or "").strip()
        if not s:
            continue
        norm = _normalize_title_for_match(s)
        if not norm or norm in seen_norm:
            continue
        seen_norm.add(norm)
        result.append(s)
    return result


def _extract_env_value_by_key(env_content: str, key: str) -> str:
    for line in (env_content or "").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        left, raw_v = line.split("=", 1)
        if left.strip() == key:
            return _strip_quotes(raw_v)
    return ""


def _extract_titles_from_regex_value(regex_value: str) -> List[str]:
    if not regex_value:
        return []
    found: List[str] = []
    seen: Set[str] = set()
    for token in re.findall(r"\^\(\?=\.\*(.*?)\)\.\*\$", regex_value):
        # 将 re.escape 产生的反斜杠恢复为原始字符。
        title = re.sub(r"\\(.)", r"\1", token).strip()
        title = _clean_extracted_title(title)
        if not title or title in seen:
            continue
        seen.add(title)
        found.append(title)
    return found


def _merge_regex_values(existing_value: str, add_value: str, remove_values: Sequence[str]) -> str:
    existing_titles = _extract_titles_from_regex_value(existing_value)
    remove_norms: Set[str] = set()
    for old_value in remove_values or []:
        for title in _extract_titles_from_regex_value(old_value):
            norm = _normalize_title_for_match(title)
            if norm:
                remove_norms.add(norm)

    kept_titles: List[str] = []
    kept_norms: Set[str] = set()
    for title in existing_titles:
        norm = _normalize_title_for_match(title)
        if not norm or norm in remove_norms or norm in kept_norms:
            continue
        kept_titles.append(title)
        kept_norms.add(norm)

    for title in _extract_titles_from_regex_value(add_value):
        norm = _normalize_title_for_match(title)
        if not norm or norm in kept_norms:
            continue
        kept_titles.append(title)
        kept_norms.add(norm)

    return build_regex_from_titles(kept_titles)


def _collect_existing_monitored_titles(env_files: Sequence[str], env_key: str) -> Set[str]:
    existing_norm: Set[str] = set()
    for env_path in env_files:
        if not env_path or not os.path.exists(env_path):
            continue
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                env_content = f.read()
        except Exception:
            continue
        value = _extract_env_value_by_key(env_content, env_key)
        if not value:
            continue
        for title in _extract_titles_from_regex_value(value):
            norm = _normalize_title_for_match(title)
            if norm:
                existing_norm.add(norm)
    return existing_norm


def apply_top_n(items: List[str], top_n: int) -> List[str]:
    if top_n <= 0:
        return items
    return items[:top_n]


def _remove_finished_titles_by_tmdb(
    titles: Sequence[str],
    *,
    remove_days: int,
    tmdb_enabled: bool,
    finish_detect_mode: str,
    tmdb_api_key: str,
    tmdb_language: str,
    tmdb_region: str,
    tmdb_timeout: int,
    tmdb_cache: Dict[str, Any],
    tmdb_cache_ttl_hours: int,
    tmdb_year_tolerance: int,
    tmdb_min_score: int,
    tmdb_marked_finished_titles: List[str],
    tmdb_max_workers: int,
) -> Tuple[List[str], List[Tuple[str, int]], bool]:
    if remove_days < 0:
        return list(titles), [], False

    today = datetime.date.today()
    kept: List[str] = []
    removed: List[Tuple[str, int]] = []
    cache_dirty = False
    batch_results: Dict[Tuple[str, int], Tuple[Optional[bool], Optional[datetime.date], str]] = {}
    batch_errors: Dict[Tuple[str, int], Exception] = {}

    if tmdb_enabled and finish_detect_mode in ("tmdb", "hybrid"):
        batch_results, batch_errors, cache_dirty = _resolve_tmdb_finish_state_batch(
            [(title, None) for title in titles],
            api_key=tmdb_api_key,
            language=tmdb_language,
            region=tmdb_region,
            timeout=tmdb_timeout,
            cache=tmdb_cache,
            cache_ttl_hours=tmdb_cache_ttl_hours,
            year_tolerance=tmdb_year_tolerance,
            min_confidence_score=tmdb_min_score,
            max_workers=tmdb_max_workers,
        )

    for title in titles:
        finished = False
        finish_date: Optional[datetime.date] = None

        if tmdb_enabled and finish_detect_mode in ("tmdb", "hybrid"):
            if (title, 0) in batch_errors:
                print(f"[WARN] TMDB 查询失败，title={title}: {batch_errors[(title, 0)]}")
            else:
                tmdb_finished, tmdb_date, tmdb_note = batch_results.get((title, 0), (None, None, "tmdb_not_checked"))
                if tmdb_finished is True:
                    finished = True
                    finish_date = tmdb_date
                    tmdb_marked_finished_titles.append(f"{title} ({tmdb_note})")

        if not finished:
            kept.append(title)
            continue

        if not isinstance(finish_date, datetime.date):
            kept.append(title)
            continue

        age_days = (today - finish_date).days
        if age_days >= remove_days:
            removed.append((title, age_days))
            continue
        kept.append(title)

    return kept, removed, cache_dirty


def _remove_old_movie_titles_by_tmdb(
    titles: Sequence[str],
    *,
    older_than_days: int,
    tmdb_api_key: str,
    tmdb_language: str,
    tmdb_region: str,
    tmdb_timeout: int,
    tmdb_cache: Dict[str, Any],
    tmdb_cache_ttl_hours: int,
    tmdb_year_tolerance: int,
    tmdb_min_score: int,
    tmdb_max_workers: int,
) -> Tuple[List[str], List[Tuple[str, int]], bool]:
    if older_than_days < 0:
        return list(titles), [], False
    if not tmdb_api_key:
        print("[WARN] 未提供 TMDB_API_KEY，无法执行电影首映日期剔除")
        return list(titles), [], False

    today = datetime.date.today()
    kept: List[str] = []
    removed: List[Tuple[str, int]] = []
    release_results, release_errors, cache_dirty = _resolve_tmdb_movie_release_date_batch(
        titles,
        api_key=tmdb_api_key,
        language=tmdb_language,
        region=tmdb_region,
        timeout=tmdb_timeout,
        cache=tmdb_cache,
        cache_ttl_hours=tmdb_cache_ttl_hours,
        year_tolerance=tmdb_year_tolerance,
        min_confidence_score=tmdb_min_score,
        max_workers=tmdb_max_workers,
    )

    for title in titles:
        if title in release_errors:
            print(f"[WARN] TMDB 电影首映日期查询失败，title={title}: {release_errors[title]}")
            kept.append(title)
            continue
        release_date, note = release_results.get(title, (None, "tmdb_movie_not_checked"))

        if not isinstance(release_date, datetime.date):
            kept.append(title)
            continue

        age_days = (today - release_date).days
        if age_days >= older_than_days:
            removed.append((title, age_days))
            print(f"[INFO] 电影首映超期剔除: {title} ({note}, age_days={age_days})")
            continue
        kept.append(title)

    return kept, removed, cache_dirty


def _escape_title_for_regex(title: str) -> str:
    # Escape regex meta characters, but keep spaces readable.
    return re.sub(r"([.^$*+?{}\\[\\]\\\\|()])", r"\\\\\\1", title)


def build_regex_from_titles(titles: Sequence[str]) -> str:
    if not titles:
        return ""

    unique_titles: List[str] = []
    seen_norm: Set[str] = set()
    for t in titles:
        s = (t or "").strip()
        if not s:
            continue
        norm = _normalize_title_for_match(s)
        if not norm or norm in seen_norm:
            continue
        seen_norm.add(norm)
        unique_titles.append(s)

    escaped = [_escape_title_for_regex(t) for t in unique_titles]
    escaped.sort(key=len, reverse=True)
    # Keep legacy style for compatibility with existing ENV_FILTER_115 rules.
    return "|".join([f"^(?=.*{item}).*$" for item in escaped])


def _load_state(state_file: str) -> Dict[str, object]:
    if not state_file or not os.path.exists(state_file):
        return {"records": []}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("records"), list):
            return data
    except Exception:
        pass
    return {"records": []}


def _save_state(state_file: str, data: Dict[str, object]) -> None:
    os.makedirs(os.path.dirname(state_file) or ".", exist_ok=True)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _update_state_source_titles(
    state_file: str,
    env_files: Sequence[str],
    env_key: str,
    source_title_map: Dict[str, Sequence[str]],
) -> None:
    if not state_file:
        return
    state = _load_state(state_file)
    source_titles = state.get("source_titles") if isinstance(state, dict) else None
    if not isinstance(source_titles, dict):
        source_titles = {}

    for env_path in env_files:
        abs_env = os.path.abspath(env_path)
        env_entry = source_titles.get(abs_env)
        if not isinstance(env_entry, dict):
            env_entry = {}
        key_entry = env_entry.get(env_key)
        if not isinstance(key_entry, dict):
            key_entry = {}
        for source, titles in (source_title_map or {}).items():
            key_entry[source] = list(titles or [])
        env_entry[env_key] = key_entry
        source_titles[abs_env] = env_entry

    state["source_titles"] = source_titles
    _save_state(state_file, state)


def _remove_value_from_csv(existing: str, value: str) -> str:
    items = [x.strip() for x in (existing or "").split(",") if x.strip()]
    kept = [x for x in items if x != value]
    return ",".join(kept)


def _replace_managed_append_value(
    env_content: str,
    key: str,
    source_tag: str,
    env_path: str,
    new_value: str,
    state_file: str,
    managed_scope: str,
) -> str:
    state = _load_state(state_file)
    records = state.get("records") if isinstance(state, dict) else None
    if not isinstance(records, list):
        records = []

    old_values: List[str] = []
    new_records: List[Dict[str, str]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        rec_env = str(rec.get("env_path") or "")
        rec_key = str(rec.get("key") or "")
        rec_source = str(rec.get("source") or "")
        rec_value = str(rec.get("value") or "")
        should_replace = False
        if managed_scope == "key":
            should_replace = (rec_env == env_path and rec_key == key)
        else:
            should_replace = (rec_env == env_path and rec_key == key and rec_source == source_tag)

        if should_replace:
            if rec_value:
                old_values.append(rec_value)
            continue
        new_records.append(rec)

    lines = env_content.splitlines()
    replaced_lines: List[str] = []
    found_key = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in line:
            replaced_lines.append(line)
            continue
        left, raw_v = line.split("=", 1)
        if left.strip() != key:
            replaced_lines.append(line)
            continue
        found_key = True
        quote_style = _detect_quote_style(raw_v)
        current = _strip_quotes(raw_v)
        merged = _merge_regex_values(current, new_value, old_values)
        replaced_lines.append(f"{left}={_format_env_value(merged, quote_style)}")

    if not found_key:
        if replaced_lines and replaced_lines[-1].strip() != "":
            replaced_lines.append("")
        replaced_lines.append(f'{key}="{new_value}"')

    new_records.append(
        {
            "env_path": env_path,
            "key": key,
            "source": source_tag,
            "value": new_value,
            "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    state["records"] = new_records
    _save_state(state_file, state)

    return "\n".join(replaced_lines) + "\n"


def update_env_content(env_content: str, key: str, value: str) -> str:
    lines = env_content.splitlines()
    updated = False
    new_lines: List[str] = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in line:
            new_lines.append(line)
            continue

        left, raw_v = line.split("=", 1)
        if left.strip() == key:
            quote_style = _detect_quote_style(raw_v)
            new_lines.append(f"{left}={_format_env_value(value, quote_style)}")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        if new_lines and new_lines[-1].strip() != "":
            new_lines.append("")
        new_lines.append(f'{key}="{value}"')

    return "\n".join(new_lines) + "\n"


def _strip_quotes(v: str) -> str:
    v = (v or '').strip()
    if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
        return v[1:-1]
    return v


def _detect_quote_style(v: str) -> str:
    s = (v or '').strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return 'double'
    if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
        return 'single'
    return 'bare'


def _format_env_value(value: str, style: str) -> str:
    if style == 'single':
        safe = (value or '').replace("'", "'\\''")
        return f"'{safe}'"
    if style == 'bare':
        return value or ''
    safe = (value or '').replace('"', '\\"')
    return f'"{safe}"'


def update_env_content_append(env_content: str, key: str, value: str) -> str:
    lines = env_content.splitlines()
    updated = False
    new_lines: List[str] = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in line:
            new_lines.append(line)
            continue

        left, raw_v = line.split("=", 1)
        if left.strip() == key:
            quote_style = _detect_quote_style(raw_v)
            existing = _strip_quotes(raw_v)
            if not existing:
                merged = value
            elif value in [x.strip() for x in existing.split(',') if x.strip()] or value in existing:
                merged = existing
            else:
                merged = f"{existing},{value}"
            new_lines.append(f"{left}={_format_env_value(merged, quote_style)}")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        if new_lines and new_lines[-1].strip() != "":
            new_lines.append("")
        new_lines.append(f'{key}="{value}"')

    return "\n".join(new_lines) + "\n"


def _make_backup_if_needed(env_path: str) -> str:
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{env_path}.bak_{ts}"
    shutil.copy2(env_path, backup_path)
    return backup_path


def normalize_env_regex_file(
    env_path: str,
    key: str,
    *,
    dry_run: bool,
    backup: bool,
    require_existing: bool,
) -> bool:
    env_exists = os.path.exists(env_path)
    if require_existing and not env_exists:
        raise RuntimeError(f"目标 .env 不存在，已阻止写入: {env_path}")

    original = ""
    if env_exists:
        with open(env_path, "r", encoding="utf-8") as f:
            original = f.read()

    current = _extract_env_value_by_key(original, key)
    if not current:
        return False

    normalized = build_regex_from_titles(_extract_titles_from_regex_value(current))
    if normalized == current:
        return False

    updated = update_env_content(original, key, normalized)

    before_non_target_keys = _collect_non_target_env_keys(original, key)
    after_non_target_keys = _collect_non_target_env_keys(updated, key)
    lost_keys = sorted(before_non_target_keys - after_non_target_keys)
    if lost_keys:
        preview = ", ".join(lost_keys[:8])
        if len(lost_keys) > 8:
            preview += ", ..."
        raise RuntimeError(
            "检测到规范化将导致其他环境变量丢失，已中止写入。"
            f" 丢失键: {preview}"
        )

    if dry_run:
        print(f"[DRY-RUN] 将规范化 {env_path}: {key}")
        return True

    os.makedirs(os.path.dirname(env_path) or ".", exist_ok=True)
    if env_exists:
        emergency_backup = f"{env_path}.autosnap_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy2(env_path, emergency_backup)
        print(f"[INFO] 已创建应急快照: {emergency_backup}")
    if backup and env_exists:
        backup_path = _make_backup_if_needed(env_path)
        print(f"[INFO] 已备份: {backup_path}")
    with open(env_path, "w", encoding="utf-8") as f:
        f.write(updated)
    print(f"[OK] 已规范化 {os.path.abspath(env_path)}: {key}")
    return True


def _extract_env_key_from_line(line: str) -> Optional[str]:
    stripped = (line or "").strip()
    if not stripped or stripped.startswith("#"):
        return None
    if "=" not in stripped:
        return None

    left = stripped.split("=", 1)[0].strip()
    if left.startswith("export "):
        left = left[len("export "):].strip()
    if not left:
        return None
    return left


def _collect_non_target_env_keys(env_content: str, target_key: str) -> Set[str]:
    keys: Set[str] = set()
    for line in (env_content or "").splitlines():
        k = _extract_env_key_from_line(line)
        if not k:
            continue
        if k == target_key:
            continue
        keys.add(k)
    return keys


def write_env_file(
    env_path: str,
    key: str,
    value: str,
    dry_run: bool,
    backup: bool,
    append_mode: bool,
    source_tag: str,
    state_file: str,
    managed_scope: str,
    require_existing: bool,
) -> None:
    original = ""
    env_exists = os.path.exists(env_path)
    if require_existing and not env_exists:
        raise RuntimeError(f"目标 .env 不存在，已阻止写入: {env_path}")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            original = f.read()

    if dry_run:
        print(f"[DRY-RUN] 将更新 {env_path}: {key}=\"{value}\"")
        return

    if append_mode:
        updated = _replace_managed_append_value(
            env_content=original,
            key=key,
            source_tag=source_tag,
            env_path=env_path,
            new_value=value,
            state_file=state_file,
            managed_scope=managed_scope,
        )
    else:
        updated = update_env_content(original, key, value)

    if updated == original:
        print(f"[INFO] 已无变化，跳过写入: {os.path.abspath(env_path)}")
        return

    # 防误清空保护：任何情况下都不允许非目标键数量下降。
    before_non_target_keys = _collect_non_target_env_keys(original, key)
    after_non_target_keys = _collect_non_target_env_keys(updated, key)
    lost_keys = sorted(before_non_target_keys - after_non_target_keys)
    if lost_keys:
        preview = ", ".join(lost_keys[:8])
        if len(lost_keys) > 8:
            preview += ", ..."
        raise RuntimeError(
            "检测到写入将导致其他环境变量丢失，已中止写入。"
            f" 丢失键: {preview}"
        )

    os.makedirs(os.path.dirname(env_path) or ".", exist_ok=True)
    if env_exists:
        emergency_backup = f"{env_path}.autosnap_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy2(env_path, emergency_backup)
        print(f"[INFO] 已创建应急快照: {emergency_backup}")
    if backup and env_exists:
        backup_path = _make_backup_if_needed(env_path)
        print(f"[INFO] 已备份: {backup_path}")
    with open(env_path, "w", encoding="utf-8") as f:
        f.write(updated)
    print(f"[OK] 已写入 {os.path.abspath(env_path)}: {key}")


def main() -> int:
    parser = argparse.ArgumentParser(description="抓取追剧日历/票房影片并更新一个或多个 .env 的正则变量")
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="数据源：支持重复传参或逗号分隔，如 --source calendar --source maoyan 或 --source calendar,maoyan；all 表示全部合并",
    )
    parser.add_argument("--home-url", default=HOME_URL, help="博客首页 URL")
    parser.add_argument("--post-url", default="", help="指定文章 URL（不传则自动取最新追剧日历）")
    parser.add_argument("--calendar-whitelist-keywords", default="", help="追剧日历白名单关键词，逗号或换行分隔")
    parser.add_argument("--calendar-blacklist-keywords", default="", help="追剧日历黑名单关键词，逗号或换行分隔")
    parser.add_argument("--maoyan-url", default=MAOYAN_BOX_OFFICE_URL, help="猫眼票房 URL")
    parser.add_argument("--maoyan-top-n", type=int, default=0, help="猫眼票房仅提取前N名，0表示不限制")
    parser.add_argument("--include-maoyan-web-heat", action="store_true", help="猫眼来源时同时抓取网播热度榜")
    parser.add_argument("--maoyan-web-heat-url", default=MAOYAN_WEB_HEAT_URL, help="猫眼网播热度 URL")
    parser.add_argument("--maoyan-web-heat-top-n", type=int, default=0, help="猫眼网播热度仅提取前N名，0表示不限制")
    parser.add_argument("--maoyan-whitelist-keywords", default="", help="猫眼来源白名单关键词，逗号或换行分隔")
    parser.add_argument("--maoyan-blacklist-keywords", default="", help="猫眼来源黑名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-url", default=DOUBAN_AMERICAN_TV_URL, help="豆瓣热播美剧 URL")
    parser.add_argument("--douban-top-n", type=int, default=0, help="豆瓣热播美剧仅提取前N名，0表示不限制")
    parser.add_argument("--douban-asia-top-n", type=int, default=None, help="豆瓣日韩剧仅提取前N名，默认跟随 douban-top-n")
    parser.add_argument("--douban-domestic-top-n", type=int, default=None, help="豆瓣国产剧仅提取前N名，默认跟随 douban-top-n")
    parser.add_argument("--douban-variety-top-n", type=int, default=None, help="豆瓣综艺仅提取前N名，默认跟随 douban-top-n")
    parser.add_argument("--douban-animation-top-n", type=int, default=None, help="豆瓣动漫仅提取前N名，默认跟随 douban-top-n")
    parser.add_argument("--douban-whitelist-keywords", default="", help="豆瓣美剧白名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-blacklist-keywords", default="", help="豆瓣美剧黑名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-asia-whitelist-keywords", default="", help="豆瓣日韩剧白名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-asia-blacklist-keywords", default="", help="豆瓣日韩剧黑名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-domestic-whitelist-keywords", default="", help="豆瓣国产剧白名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-domestic-blacklist-keywords", default="", help="豆瓣国产剧黑名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-variety-whitelist-keywords", default="", help="豆瓣综艺白名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-variety-blacklist-keywords", default="", help="豆瓣综艺黑名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-animation-whitelist-keywords", default="", help="豆瓣动漫白名单关键词，逗号或换行分隔")
    parser.add_argument("--douban-animation-blacklist-keywords", default="", help="豆瓣动漫黑名单关键词，逗号或换行分隔")
    parser.add_argument("--remove-movie-premiere-after-days", type=int, default=365, help="仅猫眼来源生效：电影首映超过多少天后移除；-1表示不移除，默认365")
    parser.add_argument("--remove-finished-after-days", type=int, default=-1, help="仅对追剧日历生效：完结多少天后从结果中移除；-1表示不移除")
    parser.add_argument(
        "--finish-keywords",
        default=",".join(FINISH_KEYWORDS),
        help="用于判定完结的关键词，逗号分隔（默认：完结,收官,大结局）",
    )
    parser.add_argument(
        "--finish-exclude-keywords",
        default=",".join(FINISH_EXCLUDE_KEYWORDS),
        help="完结判定排除词（命中则不视为完结），逗号分隔",
    )
    parser.add_argument(
        "--finish-detect-mode",
        default="hybrid",
        choices=["keyword", "tmdb", "hybrid"],
        help="完结判定方式：keyword(仅关键词) / tmdb(仅TMDB) / hybrid(关键词+TMDB)",
    )
    parser.add_argument("--tmdb-api-key", default="", help="TMDB API Key（不传则尝试读取环境变量 TMDB_API_KEY）")
    parser.add_argument("--tmdb-language", default="zh-CN", help="TMDB 查询语言，默认 zh-CN")
    parser.add_argument("--tmdb-region", default="CN", help="TMDB 查询地区，默认 CN")
    parser.add_argument("--tmdb-timeout", type=int, default=15, help="TMDB 请求超时秒数")
    parser.add_argument("--tmdb-cache-file", default=DEFAULT_TMDB_CACHE_FILE, help="TMDB 缓存文件路径")
    parser.add_argument("--tmdb-cache-ttl-hours", type=int, default=24, help="TMDB 缓存有效时长（小时）")
    parser.add_argument("--tmdb-year-tolerance", type=int, default=2, help="TMDB 同名剧年份容差（年）")
    parser.add_argument("--tmdb-min-score", type=int, default=TMDB_MIN_CONFIDENCE_SCORE, help="TMDB 匹配最低置信分，低于该分不判完结")
    parser.add_argument("--tmdb-max-workers", type=int, default=TMDB_MAX_WORKERS_DEFAULT, help="TMDB 并发查询数，默认 6，最大 8")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE, help="追加模式状态文件，用于替换旧的自动生成值")
    parser.add_argument(
        "--managed-scope",
        default="source",
        choices=["source", "key"],
        help="追加替换范围：source=仅替换同数据源写入值；key=替换同变量下全部自动值",
    )
    parser.add_argument(
        "--line-keywords",
        default="上线,开播",
        help="用于筛选行的关键词，逗号分隔（默认：上线,开播）",
    )
    parser.add_argument(
        "--title-alias-map",
        default="",
        help="识别词替换映射，每行一个，格式: 原名=新名 或 原名=>新名",
    )
    parser.add_argument(
        "--env-files",
        required=True,
        help="目标 .env 文件路径，多个用逗号分隔",
    )
    parser.add_argument(
        "--env-key",
        default="DRAMA_CALENDAR_REGEX",
        help="写入到 .env 的变量名（默认：DRAMA_CALENDAR_REGEX）",
    )
    parser.add_argument("--backup", action="store_true", help="写入前备份已存在的 .env 文件")
    parser.add_argument("--append", action="store_true", help="将结果追加到目标变量（适用于关键词白名单）")
    parser.add_argument("--allow-create-env", action="store_true", help="允许自动创建不存在的 .env 文件（默认禁止）")
    parser.add_argument("--dry-run", action="store_true", help="仅打印结果，不落盘")
    args = parser.parse_args()
    try:
        args.source = _normalize_sources(args.source)
    except ValueError as e:
        parser.error(str(e))

    include_keywords = [k.strip() for k in args.line_keywords.split(",") if k.strip()]
    if not include_keywords:
        print("[ERROR] line-keywords 不能为空", file=sys.stderr)
        return 2

    finish_keywords = [k.strip() for k in str(args.finish_keywords or '').split(',') if k.strip()]
    if not finish_keywords:
        finish_keywords = list(FINISH_KEYWORDS)
    finish_exclude_keywords = [k.strip() for k in str(args.finish_exclude_keywords or '').split(',') if k.strip()]
    if not finish_exclude_keywords:
        finish_exclude_keywords = list(FINISH_EXCLUDE_KEYWORDS)

    title_alias_map = _parse_title_alias_map(args.title_alias_map)

    env_files = [p.strip() for p in args.env_files.split(",") if p.strip()]
    if not env_files:
        print("[ERROR] env-files 不能为空", file=sys.stderr)
        return 2

    try:
        source_url = ""
        target_lines: List[str] = []
        removed_finished_titles: List[Tuple[str, int]] = []
        tmdb_marked_finished_titles: List[str] = []
        calendar_titles: List[str] = []
        maoyan_titles: List[str] = []
        douban_titles: List[str] = []
        douban_asia_titles: List[str] = []
        douban_domestic_titles: List[str] = []
        douban_variety_titles: List[str] = []
        douban_animation_titles: List[str] = []
        calendar_source_url = ""
        maoyan_source_url = ""
        douban_source_url = ""
        douban_asia_source_url = ""
        douban_domestic_source_url = ""
        douban_variety_source_url = ""
        douban_animation_source_url = ""

        finish_detect_mode = (args.finish_detect_mode or "hybrid").strip().lower()
        tmdb_api_key = (args.tmdb_api_key or os.environ.get("TMDB_API_KEY") or "").strip()
        tmdb_language = (args.tmdb_language or "zh-CN").strip() or "zh-CN"
        tmdb_region = (args.tmdb_region or "CN").strip() or "CN"
        tmdb_timeout = max(5, int(args.tmdb_timeout or 15))
        tmdb_year_tolerance = max(0, int(args.tmdb_year_tolerance or 2))
        tmdb_min_score = max(1, int(args.tmdb_min_score or TMDB_MIN_CONFIDENCE_SCORE))
        tmdb_max_workers = max(1, min(8, int(args.tmdb_max_workers or TMDB_MAX_WORKERS_DEFAULT)))
        tmdb_cache = _load_tmdb_cache(args.tmdb_cache_file)
        tmdb_cache_dirty = False
        tmdb_enabled = bool(tmdb_api_key) and finish_detect_mode in ("tmdb", "hybrid")
        tmdb_movie_enabled = bool(tmdb_api_key)

        if finish_detect_mode in ("tmdb", "hybrid"):
            if tmdb_enabled:
                print(
                    f"[INFO] TMDB 已启用: mode={finish_detect_mode} language={tmdb_language} region={tmdb_region} "
                    f"cache_ttl_hours={int(args.tmdb_cache_ttl_hours or 24)} workers={tmdb_max_workers}"
                )
            else:
                reason = "未提供 TMDB_API_KEY" if not tmdb_api_key else "finish-detect-mode 非 tmdb/hybrid"
                print(f"[INFO] TMDB 未启用: {reason}")

        if finish_detect_mode in ("tmdb", "hybrid") and not tmdb_api_key:
            print("[WARN] 未提供 TMDB_API_KEY，完结判定将回退为关键词模式")
            if finish_detect_mode == "tmdb":
                finish_detect_mode = "keyword"

        selected_sources = set(args.source)
        include_all_sources = "all" in selected_sources

        if "calendar" in selected_sources or include_all_sources:
            home_html = fetch_text(args.home_url)
            post_url = args.post_url.strip() or find_latest_calendar_post_url(home_html, args.home_url)
            post_html = fetch_text(post_url)

            text = strip_html(post_html)
            post_date = parse_date_from_post_url(post_url)
            titles, target_lines, title_states = extract_calendar_title_states(
                text,
                include_keywords,
                post_date,
                finish_keywords,
                finish_exclude_keywords,
            )

            remove_days = int(args.remove_finished_after_days or -1)
            if remove_days >= 0:
                today = datetime.date.today()
                kept_titles: List[str] = []
                batch_results: Dict[Tuple[str, int], Tuple[Optional[bool], Optional[datetime.date], str]] = {}
                batch_errors: Dict[Tuple[str, int], Exception] = {}
                title_specs = []
                for title in titles:
                    state = title_states.get(title, {})
                    finished_by_keyword = bool(state.get("finished", False))
                    finish_date = state.get("finished_date") if isinstance(state.get("finished_date"), datetime.date) else None
                    ref_date = state.get("reference_date") if isinstance(state.get("reference_date"), datetime.date) else None
                    reference_year = ref_date.year if isinstance(ref_date, datetime.date) else None
                    if ((not finished_by_keyword) or (finished_by_keyword and not isinstance(finish_date, datetime.date))) and tmdb_enabled:
                        title_specs.append((title, reference_year))

                if title_specs:
                    batch_results, batch_errors, batch_dirty = _resolve_tmdb_finish_state_batch(
                        title_specs,
                        api_key=tmdb_api_key,
                        language=tmdb_language,
                        region=tmdb_region,
                        timeout=tmdb_timeout,
                        cache=tmdb_cache,
                        cache_ttl_hours=int(args.tmdb_cache_ttl_hours or 24),
                        year_tolerance=tmdb_year_tolerance,
                        min_confidence_score=tmdb_min_score,
                        max_workers=tmdb_max_workers,
                    )
                    if batch_dirty:
                        tmdb_cache_dirty = True

                for title in titles:
                    state = title_states.get(title, {})
                    finished_by_keyword = bool(state.get("finished", False))
                    finished = finished_by_keyword
                    finish_date = state.get("finished_date") if isinstance(state.get("finished_date"), datetime.date) else None
                    ref_date = state.get("reference_date") if isinstance(state.get("reference_date"), datetime.date) else None
                    reference_year = ref_date.year if isinstance(ref_date, datetime.date) else None

                    if ((not finished) or (finished and not isinstance(finish_date, datetime.date))) and tmdb_enabled:
                        batch_key = (title, int(reference_year or 0))
                        if batch_key in batch_errors:
                            print(f"[WARN] TMDB 查询失败，title={title}: {batch_errors[batch_key]}")
                        else:
                            tmdb_finished, tmdb_date, tmdb_note = batch_results.get(batch_key, (None, None, "tmdb_not_checked"))
                            if tmdb_finished is True and finish_detect_mode in ("tmdb", "hybrid"):
                                finished = True
                                if isinstance(tmdb_date, datetime.date):
                                    finish_date = tmdb_date
                                tmdb_marked_finished_titles.append(f"{title} ({tmdb_note})")

                    if not finished:
                        kept_titles.append(title)
                        continue

                    if not isinstance(finish_date, datetime.date):
                        kept_titles.append(title)
                        continue

                    age_days = (today - finish_date).days
                    if age_days >= remove_days:
                        removed_finished_titles.append((title, age_days))
                        continue

                    kept_titles.append(title)
                titles = kept_titles
            else:
                if finish_detect_mode in ("tmdb", "hybrid"):
                    print("[INFO] 已关闭完结移除(remove_finished_after_days=-1)，本次不会触发 TMDB 完结检查")

            calendar_titles, _, _ = _apply_source_keyword_filters(
                titles,
                source_name="追剧日历",
                whitelist_keywords=_parse_keyword_filters(args.calendar_whitelist_keywords),
                blacklist_keywords=_parse_keyword_filters(args.calendar_blacklist_keywords),
            )
            if title_alias_map:
                calendar_titles, alias_count = _apply_title_aliases(calendar_titles, title_alias_map)
                if alias_count:
                    print(f"[INFO] 识别词替换(追剧日历): {alias_count}")
            calendar_source_url = post_url

        if tmdb_cache_dirty:
            try:
                _save_tmdb_cache(args.tmdb_cache_file, tmdb_cache)
            except Exception as e:
                print(f"[WARN] TMDB 缓存写入失败: {e}")

        if "maoyan" in selected_sources or include_all_sources:
            maoyan_url = (args.maoyan_url or MAOYAN_BOX_OFFICE_URL).strip()
            maoyan_html = fetch_text(maoyan_url)
            box_titles = extract_maoyan_movie_names(maoyan_html)
            box_titles = apply_top_n(box_titles, int(args.maoyan_top_n or 0))

            web_heat_titles: List[str] = []
            if bool(args.include_maoyan_web_heat):
                web_heat_url = (args.maoyan_web_heat_url or MAOYAN_WEB_HEAT_URL).strip()
                web_heat_html = fetch_text(web_heat_url)
                web_heat_titles = extract_maoyan_web_heat_names(web_heat_html)
                web_heat_titles = apply_top_n(web_heat_titles, int(args.maoyan_web_heat_top_n or 0))

            remove_days = int(args.remove_finished_after_days or -1)
            if remove_days >= 0 and web_heat_titles:
                web_heat_titles, removed_from_web_heat, maoyan_tv_cache_dirty = _remove_finished_titles_by_tmdb(
                    web_heat_titles,
                    remove_days=remove_days,
                    tmdb_enabled=tmdb_enabled,
                    finish_detect_mode=finish_detect_mode,
                    tmdb_api_key=tmdb_api_key,
                    tmdb_language=tmdb_language,
                    tmdb_region=tmdb_region,
                    tmdb_timeout=tmdb_timeout,
                    tmdb_cache=tmdb_cache,
                    tmdb_cache_ttl_hours=int(args.tmdb_cache_ttl_hours or 24),
                    tmdb_year_tolerance=tmdb_year_tolerance,
                    tmdb_min_score=tmdb_min_score,
                    tmdb_marked_finished_titles=tmdb_marked_finished_titles,
                    tmdb_max_workers=tmdb_max_workers,
                )
                if maoyan_tv_cache_dirty:
                    tmdb_cache_dirty = True
                if removed_from_web_heat:
                    removed_finished_titles.extend(removed_from_web_heat)
            else:
                if finish_detect_mode in ("tmdb", "hybrid") and web_heat_titles:
                    print("[INFO] 猫眼网播热度未开启完结移除(remove_finished_after_days=-1)，本次不会触发 TMDB 完结检查")

            movie_remove_days = int(args.remove_movie_premiere_after_days if args.remove_movie_premiere_after_days is not None else 365)
            if movie_remove_days >= 0:
                if tmdb_movie_enabled:
                    box_titles, removed_old_movies, movie_cache_dirty = _remove_old_movie_titles_by_tmdb(
                        box_titles,
                        older_than_days=movie_remove_days,
                        tmdb_api_key=tmdb_api_key,
                        tmdb_language=tmdb_language,
                        tmdb_region=tmdb_region,
                        tmdb_timeout=tmdb_timeout,
                        tmdb_cache=tmdb_cache,
                        tmdb_cache_ttl_hours=int(args.tmdb_cache_ttl_hours or 24),
                        tmdb_year_tolerance=tmdb_year_tolerance,
                        tmdb_min_score=tmdb_min_score,
                        tmdb_max_workers=tmdb_max_workers,
                    )
                    if movie_cache_dirty:
                        tmdb_cache_dirty = True
                    if removed_old_movies:
                        print(f"[INFO] 已移除超期电影: {len(removed_old_movies)} (阈值={movie_remove_days}天)")
                        for name, days in removed_old_movies[:30]:
                            print(f"  - {name} (首映已 {days} 天)")
                else:
                    print("[WARN] 猫眼电影首映日期剔除已启用，但未提供 TMDB_API_KEY，已跳过")
            else:
                print("[INFO] 已关闭电影首映日期剔除(remove_movie_premiere_after_days=-1)")

            maoyan_titles, _, _ = _apply_source_keyword_filters(
                merge_unique(box_titles, web_heat_titles),
                source_name="猫眼来源",
                whitelist_keywords=_parse_keyword_filters(args.maoyan_whitelist_keywords),
                blacklist_keywords=_parse_keyword_filters(args.maoyan_blacklist_keywords),
            )
            if title_alias_map:
                maoyan_titles, alias_count = _apply_title_aliases(maoyan_titles, title_alias_map)
                if alias_count:
                    print(f"[INFO] 识别词替换(猫眼): {alias_count}")
            maoyan_source_url = maoyan_url

        if "douban" in selected_sources or include_all_sources:
            douban_url = _normalize_douban_collection_url((args.douban_url or DOUBAN_AMERICAN_TV_URL).strip(), fallback_url=DOUBAN_AMERICAN_TV_URL)
            req_count = int(args.douban_top_n or 0)
            if req_count <= 0:
                req_count = 100
            titles = fetch_douban_collection_titles(douban_url, count=req_count, timeout=20)
            titles = apply_top_n(titles, int(args.douban_top_n or 0))

            remove_days = int(args.remove_finished_after_days or -1)
            if remove_days >= 0:
                douban_titles, removed_from_douban, douban_cache_dirty = _remove_finished_titles_by_tmdb(
                    titles,
                    remove_days=remove_days,
                    tmdb_enabled=tmdb_enabled,
                    finish_detect_mode=finish_detect_mode,
                    tmdb_api_key=tmdb_api_key,
                    tmdb_language=tmdb_language,
                    tmdb_region=tmdb_region,
                    tmdb_timeout=tmdb_timeout,
                    tmdb_cache=tmdb_cache,
                    tmdb_cache_ttl_hours=int(args.tmdb_cache_ttl_hours or 24),
                    tmdb_year_tolerance=tmdb_year_tolerance,
                    tmdb_min_score=tmdb_min_score,
                    tmdb_marked_finished_titles=tmdb_marked_finished_titles,
                    tmdb_max_workers=tmdb_max_workers,
                )
                if douban_cache_dirty:
                    tmdb_cache_dirty = True
                if removed_from_douban:
                    removed_finished_titles.extend(removed_from_douban)
            else:
                douban_titles = titles
                if finish_detect_mode in ("tmdb", "hybrid"):
                    print("[INFO] 豆瓣来源未开启完结移除(remove_finished_after_days=-1)，本次不会触发 TMDB 完结检查")

            douban_titles, _, _ = _apply_source_keyword_filters(
                douban_titles,
                source_name="豆瓣美剧",
                whitelist_keywords=_parse_keyword_filters(args.douban_whitelist_keywords),
                blacklist_keywords=_parse_keyword_filters(args.douban_blacklist_keywords),
            )
            if title_alias_map:
                douban_titles, alias_count = _apply_title_aliases(douban_titles, title_alias_map)
                if alias_count:
                    print(f"[INFO] 识别词替换(豆瓣美剧): {alias_count}")
            douban_source_url = douban_url

        if "douban_asia" in selected_sources or include_all_sources:
            top_n_asia = args.douban_asia_top_n if args.douban_asia_top_n is not None else args.douban_top_n
            req_count = int(top_n_asia or 0)
            if req_count <= 0:
                req_count = 100
            korean_titles = fetch_douban_collection_titles(DOUBAN_KOREAN_TV_URL, count=req_count, timeout=20)
            japanese_titles = fetch_douban_collection_titles(DOUBAN_JAPANESE_TV_URL, count=req_count, timeout=20)
            titles = merge_unique(korean_titles, japanese_titles)
            titles = apply_top_n(titles, int(top_n_asia or 0))

            remove_days = int(args.remove_finished_after_days or -1)
            if remove_days >= 0:
                douban_asia_titles, removed_from_douban_asia, douban_asia_cache_dirty = _remove_finished_titles_by_tmdb(
                    titles,
                    remove_days=remove_days,
                    tmdb_enabled=tmdb_enabled,
                    finish_detect_mode=finish_detect_mode,
                    tmdb_api_key=tmdb_api_key,
                    tmdb_language=tmdb_language,
                    tmdb_region=tmdb_region,
                    tmdb_timeout=tmdb_timeout,
                    tmdb_cache=tmdb_cache,
                    tmdb_cache_ttl_hours=int(args.tmdb_cache_ttl_hours or 24),
                    tmdb_year_tolerance=tmdb_year_tolerance,
                    tmdb_min_score=tmdb_min_score,
                    tmdb_marked_finished_titles=tmdb_marked_finished_titles,
                    tmdb_max_workers=tmdb_max_workers,
                )
                if douban_asia_cache_dirty:
                    tmdb_cache_dirty = True
                if removed_from_douban_asia:
                    removed_finished_titles.extend(removed_from_douban_asia)
            else:
                douban_asia_titles = titles
                if finish_detect_mode in ("tmdb", "hybrid"):
                    print("[INFO] 豆瓣日韩来源未开启完结移除(remove_finished_after_days=-1)，本次不会触发 TMDB 完结检查")

            douban_asia_titles, _, _ = _apply_source_keyword_filters(
                douban_asia_titles,
                source_name="豆瓣日韩剧",
                whitelist_keywords=_parse_keyword_filters(args.douban_asia_whitelist_keywords),
                blacklist_keywords=_parse_keyword_filters(args.douban_asia_blacklist_keywords),
            )
            if title_alias_map:
                douban_asia_titles, alias_count = _apply_title_aliases(douban_asia_titles, title_alias_map)
                if alias_count:
                    print(f"[INFO] 识别词替换(豆瓣日韩): {alias_count}")
            douban_asia_source_url = f"{DOUBAN_KOREAN_TV_URL},{DOUBAN_JAPANESE_TV_URL}"

        if "douban_domestic" in selected_sources or include_all_sources:
            top_n_domestic = args.douban_domestic_top_n if args.douban_domestic_top_n is not None else args.douban_top_n
            req_count = int(top_n_domestic or 0)
            if req_count <= 0:
                req_count = 100
            titles = fetch_douban_collection_titles(DOUBAN_DOMESTIC_TV_URL, count=req_count, timeout=20)
            titles = apply_top_n(titles, int(top_n_domestic or 0))

            remove_days = int(args.remove_finished_after_days or -1)
            if remove_days >= 0:
                douban_domestic_titles, removed_from_douban_domestic, douban_domestic_cache_dirty = _remove_finished_titles_by_tmdb(
                    titles,
                    remove_days=remove_days,
                    tmdb_enabled=tmdb_enabled,
                    finish_detect_mode=finish_detect_mode,
                    tmdb_api_key=tmdb_api_key,
                    tmdb_language=tmdb_language,
                    tmdb_region=tmdb_region,
                    tmdb_timeout=tmdb_timeout,
                    tmdb_cache=tmdb_cache,
                    tmdb_cache_ttl_hours=int(args.tmdb_cache_ttl_hours or 24),
                    tmdb_year_tolerance=tmdb_year_tolerance,
                    tmdb_min_score=tmdb_min_score,
                    tmdb_marked_finished_titles=tmdb_marked_finished_titles,
                    tmdb_max_workers=tmdb_max_workers,
                )
                if douban_domestic_cache_dirty:
                    tmdb_cache_dirty = True
                if removed_from_douban_domestic:
                    removed_finished_titles.extend(removed_from_douban_domestic)
            else:
                douban_domestic_titles = titles
                if finish_detect_mode in ("tmdb", "hybrid"):
                    print("[INFO] 豆瓣国产剧来源未开启完结移除(remove_finished_after_days=-1)，本次不会触发 TMDB 完结检查")

            douban_domestic_titles, _, _ = _apply_source_keyword_filters(
                douban_domestic_titles,
                source_name="豆瓣国产剧",
                whitelist_keywords=_parse_keyword_filters(args.douban_domestic_whitelist_keywords),
                blacklist_keywords=_parse_keyword_filters(args.douban_domestic_blacklist_keywords),
            )
            if title_alias_map:
                douban_domestic_titles, alias_count = _apply_title_aliases(douban_domestic_titles, title_alias_map)
                if alias_count:
                    print(f"[INFO] 识别词替换(豆瓣国产剧): {alias_count}")
            douban_domestic_source_url = DOUBAN_DOMESTIC_TV_URL

        if "douban_variety" in selected_sources or include_all_sources:
            top_n_variety = args.douban_variety_top_n if args.douban_variety_top_n is not None else args.douban_top_n
            req_count = int(top_n_variety or 0)
            if req_count <= 0:
                req_count = 100
            titles = fetch_douban_collection_titles(DOUBAN_VARIETY_SHOW_URL, count=req_count, timeout=20)
            titles = apply_top_n(titles, int(top_n_variety or 0))

            remove_days = int(args.remove_finished_after_days or -1)
            if remove_days >= 0:
                douban_variety_titles, removed_from_douban_variety, douban_variety_cache_dirty = _remove_finished_titles_by_tmdb(
                    titles,
                    remove_days=remove_days,
                    tmdb_enabled=tmdb_enabled,
                    finish_detect_mode=finish_detect_mode,
                    tmdb_api_key=tmdb_api_key,
                    tmdb_language=tmdb_language,
                    tmdb_region=tmdb_region,
                    tmdb_timeout=tmdb_timeout,
                    tmdb_cache=tmdb_cache,
                    tmdb_cache_ttl_hours=int(args.tmdb_cache_ttl_hours or 24),
                    tmdb_year_tolerance=tmdb_year_tolerance,
                    tmdb_min_score=tmdb_min_score,
                    tmdb_marked_finished_titles=tmdb_marked_finished_titles,
                    tmdb_max_workers=tmdb_max_workers,
                )
                if douban_variety_cache_dirty:
                    tmdb_cache_dirty = True
                if removed_from_douban_variety:
                    removed_finished_titles.extend(removed_from_douban_variety)
            else:
                douban_variety_titles = titles
                if finish_detect_mode in ("tmdb", "hybrid"):
                    print("[INFO] 豆瓣综艺来源未开启完结移除(remove_finished_after_days=-1)，本次不会触发 TMDB 完结检查")

            douban_variety_titles, _, _ = _apply_source_keyword_filters(
                douban_variety_titles,
                source_name="豆瓣综艺",
                whitelist_keywords=_parse_keyword_filters(args.douban_variety_whitelist_keywords),
                blacklist_keywords=_parse_keyword_filters(args.douban_variety_blacklist_keywords),
            )
            if title_alias_map:
                douban_variety_titles, alias_count = _apply_title_aliases(douban_variety_titles, title_alias_map)
                if alias_count:
                    print(f"[INFO] 识别词替换(豆瓣综艺): {alias_count}")
            douban_variety_source_url = DOUBAN_VARIETY_SHOW_URL

        if "douban_animation" in selected_sources or include_all_sources:
            top_n_animation = args.douban_animation_top_n if args.douban_animation_top_n is not None else args.douban_top_n
            req_count = int(top_n_animation or 0)
            if req_count <= 0:
                req_count = 100
            titles = fetch_douban_collection_titles(DOUBAN_ANIMATION_URL, count=req_count, timeout=20)
            titles = apply_top_n(titles, int(top_n_animation or 0))

            remove_days = int(args.remove_finished_after_days or -1)
            if remove_days >= 0:
                douban_animation_titles, removed_from_douban_animation, douban_animation_cache_dirty = _remove_finished_titles_by_tmdb(
                    titles,
                    remove_days=remove_days,
                    tmdb_enabled=tmdb_enabled,
                    finish_detect_mode=finish_detect_mode,
                    tmdb_api_key=tmdb_api_key,
                    tmdb_language=tmdb_language,
                    tmdb_region=tmdb_region,
                    tmdb_timeout=tmdb_timeout,
                    tmdb_cache=tmdb_cache,
                    tmdb_cache_ttl_hours=int(args.tmdb_cache_ttl_hours or 24),
                    tmdb_year_tolerance=tmdb_year_tolerance,
                    tmdb_min_score=tmdb_min_score,
                    tmdb_marked_finished_titles=tmdb_marked_finished_titles,
                    tmdb_max_workers=tmdb_max_workers,
                )
                if douban_animation_cache_dirty:
                    tmdb_cache_dirty = True
                if removed_from_douban_animation:
                    removed_finished_titles.extend(removed_from_douban_animation)
            else:
                douban_animation_titles = titles
                if finish_detect_mode in ("tmdb", "hybrid"):
                    print("[INFO] 豆瓣动漫来源未开启完结移除(remove_finished_after_days=-1)，本次不会触发 TMDB 完结检查")

            douban_animation_titles, _, _ = _apply_source_keyword_filters(
                douban_animation_titles,
                source_name="豆瓣动漫",
                whitelist_keywords=_parse_keyword_filters(args.douban_animation_whitelist_keywords),
                blacklist_keywords=_parse_keyword_filters(args.douban_animation_blacklist_keywords),
            )
            if title_alias_map:
                douban_animation_titles, alias_count = _apply_title_aliases(douban_animation_titles, title_alias_map)
                if alias_count:
                    print(f"[INFO] 识别词替换(豆瓣动漫): {alias_count}")
            douban_animation_source_url = DOUBAN_ANIMATION_URL

        if tmdb_cache_dirty:
            try:
                _save_tmdb_cache(args.tmdb_cache_file, tmdb_cache)
            except Exception as e:
                print(f"[WARN] TMDB 缓存写入失败: {e}")

        source_results = {
            "calendar": (calendar_titles, calendar_source_url),
            "maoyan": (maoyan_titles, maoyan_source_url),
            "douban": (douban_titles, douban_source_url),
            "douban_asia": (douban_asia_titles, douban_asia_source_url),
            "douban_domestic": (douban_domestic_titles, douban_domestic_source_url),
            "douban_variety": (douban_variety_titles, douban_variety_source_url),
            "douban_animation": (douban_animation_titles, douban_animation_source_url),
        }

        if len(args.source) == 1 and args.source[0] != "all":
            merge_order = [args.source[0]]
        else:
            merge_order = [name for name in SOURCE_CHOICES if name != "all" and (include_all_sources or name in selected_sources)]

        source_url_parts: List[str] = []
        if len(merge_order) == 1:
            source_url = source_results[merge_order[0]][1]
        else:
            for name in merge_order:
                source_url_parts.append(f"{name}={source_results[name][1]}")
            source_url = "; ".join(source_url_parts)

        source_title_map: Dict[str, List[str]] = {}
        seen_norm: Set[str] = set()
        for name in merge_order:
            part_titles = dedupe_titles_normalized(source_results[name][0])
            for title in part_titles:
                norm = _normalize_title_for_match(title)
                if not norm or norm in seen_norm:
                    continue
                seen_norm.add(norm)
                source_title_map.setdefault(name, []).append(title)

        titles = []
        for name in merge_order:
            titles.extend(source_title_map.get(name, []))
        titles = dedupe_titles_normalized(titles)
        titles_before_append = len(titles)

        skipped_existing_titles: List[str] = []
        if bool(args.append):
            existing_monitored_norm = _collect_existing_monitored_titles(env_files, args.env_key)
            if existing_monitored_norm:
                for title in titles:
                    norm = _normalize_title_for_match(title)
                    if norm and norm in existing_monitored_norm:
                        skipped_existing_titles.append(title)

        regex = build_regex_from_titles(titles)

        if not args.dry_run:
            _update_state_source_titles(
                args.state_file or DEFAULT_STATE_FILE,
                env_files,
                args.env_key,
                source_title_map,
            )

        source_label = ",".join(args.source)
        print(f"[INFO] 数据源: {source_label}")
        print(f"[INFO] 来源链接: {source_url}")
        if "calendar" in selected_sources or include_all_sources:
            print(f"[INFO] 关键词: {','.join(include_keywords)}")
            print(f"[INFO] 命中行数: {len(target_lines)}")
            print(f"[INFO] 完结判定词: {','.join(finish_keywords)}")
            print(f"[INFO] 完结排除词: {','.join(finish_exclude_keywords)}")
            print(f"[INFO] 完结判定模式: {finish_detect_mode}")
            print(f"[INFO] 完结移除天数: {int(args.remove_finished_after_days or -1)}")
            if tmdb_enabled:
                print(f"[INFO] TMDB 语言/地区: {tmdb_language}/{tmdb_region}")
                print(f"[INFO] TMDB 年份容差/最低分: {tmdb_year_tolerance}/{tmdb_min_score}")
        if "maoyan" in selected_sources or include_all_sources:
            print(f"[INFO] 猫眼提取前N: {int(args.maoyan_top_n or 0) if int(args.maoyan_top_n or 0) > 0 else '不限制'}")
            print(f"[INFO] 同步网播热度: {'是' if bool(args.include_maoyan_web_heat) else '否'}")
            print(f"[INFO] 电影首映剔除阈值: {int(args.remove_movie_premiere_after_days if args.remove_movie_premiere_after_days is not None else 365)} 天")
            if bool(args.include_maoyan_web_heat):
                print(f"[INFO] 网播热度链接: {(args.maoyan_web_heat_url or MAOYAN_WEB_HEAT_URL).strip()}")
                print(f"[INFO] 网播热度前N: {int(args.maoyan_web_heat_top_n or 0) if int(args.maoyan_web_heat_top_n or 0) > 0 else '不限制'}")
        if "douban" in selected_sources or include_all_sources:
            print(f"[INFO] 豆瓣链接: {(args.douban_url or DOUBAN_AMERICAN_TV_URL).strip()}")
            print(f"[INFO] 豆瓣前N: {int(args.douban_top_n or 0) if int(args.douban_top_n or 0) > 0 else '不限制'}")
        if "douban_asia" in selected_sources or include_all_sources:
            top_n_asia = args.douban_asia_top_n if args.douban_asia_top_n is not None else args.douban_top_n
            print(f"[INFO] 豆瓣日韩链接: {DOUBAN_KOREAN_TV_URL},{DOUBAN_JAPANESE_TV_URL}")
            print(f"[INFO] 豆瓣日韩前N: {int(top_n_asia or 0) if int(top_n_asia or 0) > 0 else '不限制'}")
        if "douban_domestic" in selected_sources or include_all_sources:
            top_n_domestic = args.douban_domestic_top_n if args.douban_domestic_top_n is not None else args.douban_top_n
            print(f"[INFO] 豆瓣国产剧链接: {DOUBAN_DOMESTIC_TV_URL}")
            print(f"[INFO] 豆瓣国产剧前N: {int(top_n_domestic or 0) if int(top_n_domestic or 0) > 0 else '不限制'}")
        if "douban_variety" in selected_sources or include_all_sources:
            top_n_variety = args.douban_variety_top_n if args.douban_variety_top_n is not None else args.douban_top_n
            print(f"[INFO] 豆瓣综艺链接: {DOUBAN_VARIETY_SHOW_URL}")
            print(f"[INFO] 豆瓣综艺前N: {int(top_n_variety or 0) if int(top_n_variety or 0) > 0 else '不限制'}")
        if "douban_animation" in selected_sources or include_all_sources:
            top_n_animation = args.douban_animation_top_n if args.douban_animation_top_n is not None else args.douban_top_n
            print(f"[INFO] 豆瓣动漫链接: {DOUBAN_ANIMATION_URL}")
            print(f"[INFO] 豆瓣动漫前N: {int(top_n_animation or 0) if int(top_n_animation or 0) > 0 else '不限制'}")
        print(f"[INFO] 提取剧名数: {len(titles)}")
        if skipped_existing_titles:
            print(f"[INFO] 已跳过历史已监控剧名: {len(skipped_existing_titles)}")
            for item in skipped_existing_titles[:20]:
                print(f"  - {item}")

        if removed_finished_titles:
            print(f"[INFO] 已移除完结剧: {len(removed_finished_titles)}")
            for name, days in removed_finished_titles:
                print(f"  - {name} (完结已 {days} 天)")
        if tmdb_marked_finished_titles:
            print(f"[INFO] TMDB 补充判定为完结: {len(tmdb_marked_finished_titles)}")
            for item in tmdb_marked_finished_titles[:20]:
                print(f"  - {item}")

        if not titles:
            # 有些场景并非抓取失败，而是“全部命中去重/剔除规则后无需更新”。
            if titles_before_append > 0 or skipped_existing_titles or removed_finished_titles:
                normalized_count = 0
                if bool(args.append):
                    for env_path in env_files:
                        changed = normalize_env_regex_file(
                            env_path,
                            args.env_key,
                            dry_run=args.dry_run,
                            backup=args.backup,
                            require_existing=(not bool(args.allow_create_env)),
                        )
                        if changed:
                            normalized_count += 1
                if normalized_count > 0:
                    print(f"[INFO] 本次无新增剧名可写入，已规范化 {normalized_count} 个 .env")
                else:
                    print("[INFO] 本次无新增剧名可写入，已跳过 .env 更新")
                return 0
            print("[WARN] 未提取到任何剧名，不更新 .env", file=sys.stderr)
            return 1

        for t in titles:
            print(f"  - {t}")

        print(f"[INFO] 生成正则: {regex}")

        for env_path in env_files:
            write_env_file(
                env_path,
                args.env_key,
                regex,
                dry_run=args.dry_run,
                backup=args.backup,
                append_mode=args.append,
                source_tag=source_label,
                state_file=(args.state_file or DEFAULT_STATE_FILE),
                managed_scope=(args.managed_scope or "source"),
                require_existing=(not bool(args.allow_create_env)),
            )

        return 0
    except requests.RequestException as e:
        print(f"[ERROR] 网络请求失败: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"[ERROR] 执行失败: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
