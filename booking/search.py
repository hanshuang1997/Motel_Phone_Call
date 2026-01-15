import json
import math
import os

from openai import OpenAIError

from .embeddings import EMBED_MODEL, _embed_query_cached
from .index import _default_db_path, _ensure_index, _get_conn
from .parsing import _parse_date_str, _query_tokens, _resolve_query_date, _tokenize


def _normalize_status(value):
    return (value or "").strip().lower()


def _normalize_room_type(value):
    return " ".join(_tokenize(value or "")).strip()


def _detect_room_type(query, rows):
    query_tokens = set(_tokenize(query))
    if not query_tokens:
        return None
    room_types = {}
    for row in rows:
        room_type = row.get("room_type") or ""
        normalized = _normalize_room_type(room_type)
        if normalized:
            room_types.setdefault(normalized, room_type)
    candidates = []
    for normalized, original in room_types.items():
        tokens = normalized.split()
        if tokens and set(tokens).issubset(query_tokens):
            candidates.append((len(tokens), len(normalized), original, normalized))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][2]


def _is_available(row):
    status = _normalize_status(row.get("status"))
    return status in {"available", "vacant"} or status.startswith("available")


def _filter_rows(rows, availability_only, room_type_filter=None):
    filtered = rows
    if availability_only:
        filtered = [row for row in filtered if _is_available(row)]
    if room_type_filter:
        normalized_filter = _normalize_room_type(room_type_filter)
        if normalized_filter:
            filtered = [
                row
                for row in filtered
                if _normalize_room_type(row.get("room_type")) == normalized_filter
            ]
    return filtered


def _sort_room_number(value):
    text = str(value or "").strip()
    if text.isdigit():
        return (0, int(text))
    return (1, text)


def _summarize_rows(
    rows,
    query_date=None,
    is_explicit_year=False,
    availability_only=False,
    room_type_filter=None,
    summary_complete=False,
):
    counts = {}
    for row in rows:
        room_type = (row.get("room_type") or "").strip() or "Unknown"
        counts[room_type] = counts.get(room_type, 0) + 1
    room_numbers = sorted(
        {str(row.get("room_number") or "").strip() for row in rows if row.get("room_number")},
        key=_sort_room_number,
    )
    date_label = None
    if query_date:
        date_label = (
            query_date.isoformat()
            if is_explicit_year
            else f"{query_date.strftime('%b')} {query_date.day}"
        )
    return {
        "total": len(rows),
        "room_type_counts": counts,
        "room_numbers": room_numbers,
        "date_label": date_label,
        "availability_only": availability_only,
        "room_type_filter": room_type_filter,
        "summary_complete": summary_complete,
    }


def _cosine_similarity(vec_a, vec_b):
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _load_index_rows(conn, csv_path):
    rows = conn.execute(
        """
        SELECT row_json, row_text, embedding_json
        FROM booking_vectors
        WHERE csv_path = ?
        """,
        (csv_path,),
    ).fetchall()
    return [
        {
            "row": json.loads(row["row_json"]),
            "row_text": row["row_text"],
            "embedding": json.loads(row["embedding_json"]),
            "tokens": set(_tokenize(row["row_text"])),
            "row_date": _parse_date_str(json.loads(row["row_json"]).get("date", "")),
        }
        for row in rows
    ]


def find_relevant_rows(
    query,
    csv_path,
    max_rows=5,
    db_path=None,
    include_summary=False,
    availability_only=False,
):
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return ([], None) if include_summary else []

    db_path = db_path or _default_db_path()
    _ensure_index(csv_path, db_path)

    conn = _get_conn(db_path)
    try:
        rows = _load_index_rows(conn, os.path.abspath(csv_path))
    finally:
        conn.close()

    if not rows:
        return ([], None) if include_summary else []

    try:
        query_text = query.strip()
        if not query_text:
            return ([], None) if include_summary else []
        query_date, is_explicit_year = _resolve_query_date(query_text)
        room_type_filter = _detect_room_type(query_text, [item["row"] for item in rows])
        if query_date:
            date_rows = [item for item in rows if item["row_date"] == query_date]
            if date_rows:
                date_rows.sort(key=lambda item: item["row"].get("room_number", ""))
                all_rows = [item["row"] for item in date_rows]
                filtered_rows = _filter_rows(
                    all_rows, availability_only, room_type_filter=room_type_filter
                )
                summary = _summarize_rows(
                    filtered_rows,
                    query_date=query_date,
                    is_explicit_year=is_explicit_year,
                    availability_only=availability_only,
                    room_type_filter=room_type_filter,
                    summary_complete=True,
                )
                limited_rows = filtered_rows[:max_rows]
                return (limited_rows, summary) if include_summary else limited_rows
            if not is_explicit_year:
                month_day_rows = [
                    item
                    for item in rows
                    if item["row_date"]
                    and item["row_date"].month == query_date.month
                    and item["row_date"].day == query_date.day
                ]
                if month_day_rows:
                    month_day_rows.sort(
                        key=lambda item: item["row"].get("room_number", "")
                    )
                    all_rows = [item["row"] for item in month_day_rows]
                    filtered_rows = _filter_rows(
                        all_rows, availability_only, room_type_filter=room_type_filter
                    )
                    summary = _summarize_rows(
                        filtered_rows,
                        query_date=query_date,
                        is_explicit_year=False,
                        availability_only=availability_only,
                        room_type_filter=room_type_filter,
                        summary_complete=True,
                    )
                    limited_rows = filtered_rows[:max_rows]
                    return (limited_rows, summary) if include_summary else limited_rows
        query_tokens = _query_tokens(query_text)
        candidates = rows
        if query_tokens:
            filtered = [item for item in rows if item["tokens"] & query_tokens]
            if filtered:
                candidates = filtered
        query_embedding = _embed_query_cached(EMBED_MODEL, query_text)
    except (OpenAIError, ValueError):
        return ([], None) if include_summary else []

    scored = []
    for item in candidates:
        score = _cosine_similarity(query_embedding, item["embedding"])
        scored.append((score, item["row"]))

    scored.sort(key=lambda item: item[0], reverse=True)
    all_rows = [item[1] for item in scored]
    filtered_rows = _filter_rows(
        all_rows, availability_only, room_type_filter=room_type_filter
    )
    limited_rows = filtered_rows[:max_rows]
    summary = (
        _summarize_rows(
            filtered_rows,
            availability_only=availability_only,
            room_type_filter=room_type_filter,
            summary_complete=False,
        )
        if include_summary
        else None
    )
    return (limited_rows, summary) if include_summary else limited_rows
