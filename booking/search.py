import json
import math
import os

from openai import OpenAIError

from .embeddings import EMBED_MODEL, _embed_query_cached
from .index import _default_db_path, _ensure_index, _get_conn
from .parsing import _parse_date_str, _query_tokens, _resolve_query_date, _tokenize


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


def find_relevant_rows(query, csv_path, max_rows=5, db_path=None):
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return []

    db_path = db_path or _default_db_path()
    _ensure_index(csv_path, db_path)

    conn = _get_conn(db_path)
    try:
        rows = _load_index_rows(conn, os.path.abspath(csv_path))
    finally:
        conn.close()

    if not rows:
        return []

    try:
        query_text = query.strip()
        if not query_text:
            return []
        query_date, is_explicit_year = _resolve_query_date(query_text)
        if query_date:
            date_rows = [item for item in rows if item["row_date"] == query_date]
            if date_rows:
                date_rows.sort(key=lambda item: item["row"].get("room_number", ""))
                return [item["row"] for item in date_rows]
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
                    return [item["row"] for item in month_day_rows]
        query_tokens = _query_tokens(query_text)
        candidates = rows
        if query_tokens:
            filtered = [item for item in rows if item["tokens"] & query_tokens]
            if filtered:
                candidates = filtered
        query_embedding = _embed_query_cached(EMBED_MODEL, query_text)
    except (OpenAIError, ValueError):
        return []

    scored = []
    for item in candidates:
        score = _cosine_similarity(query_embedding, item["embedding"])
        scored.append((score, item["row"]))

    scored.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in scored[:max_rows]]
