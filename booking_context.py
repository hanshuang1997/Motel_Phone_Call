import csv
import json
import math
import os
import sqlite3
import re
from functools import lru_cache
from datetime import date, timedelta

from openai import OpenAI, OpenAIError

EMBED_MODEL = os.environ.get("BOOKING_EMBED_MODEL", "text-embedding-3-small")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "at",
    "be",
    "can",
    "do",
    "for",
    "have",
    "i",
    "in",
    "is",
    "it",
    "me",
    "of",
    "on",
    "or",
    "our",
    "please",
    "the",
    "to",
    "us",
    "we",
    "with",
    "you",
    "your",
}
_DATE_RE = re.compile(r"\b(20\d{2})-(\d{1,2})-(\d{1,2})\b")
_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_MONTH_DAY_RE = re.compile(
    r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s*(?:of\s+)?"
    r"(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)"
    r"(?:\s*(?P<year>\d{4}))?\b"
)
_MONTH_DAY_RE_REV = re.compile(
    r"\b(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?)\s*"
    r"(?P<day>\d{1,2})(?:st|nd|rd|th)?"
    r"(?:\s*(?P<year>\d{4}))?\b"
)
_WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
_WEEKDAY_RE = re.compile(
    r"\b(?P<qualifier>next|this)?\s*"
    r"(?P<weekday>monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b"
)


def _default_db_path():
    return "/tmp/booking_vectors.db" if os.environ.get("VERCEL") else "booking_vectors.db"


def _get_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS booking_vectors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            csv_path TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            row_json TEXT NOT NULL,
            row_text TEXT NOT NULL,
            embedding_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS booking_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.commit()
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(booking_vectors)").fetchall()
    }
    if "row_text" not in columns:
        conn.execute("DROP TABLE IF EXISTS booking_vectors")
        conn.execute(
            """
            CREATE TABLE booking_vectors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                csv_path TEXT NOT NULL,
                row_index INTEGER NOT NULL,
                row_json TEXT NOT NULL,
                row_text TEXT NOT NULL,
                embedding_json TEXT NOT NULL
            )
            """
        )
        conn.commit()


def _get_meta(conn, key):
    row = conn.execute("SELECT value FROM booking_meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def _set_meta(conn, key, value):
    conn.execute(
        "INSERT OR REPLACE INTO booking_meta (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()


def _format_row_text(row):
    return (
        "date: {date}; room_number: {room_number}; room_type: {room_type}; "
        "status: {status}; booking_id: {booking_id}; guest_name: {guest_name}; "
        "check_in: {check_in}; check_out: {check_out}; channel: {channel}; "
        "nightly_rate_nzd: {nightly_rate_nzd}; notes: {notes}"
    ).format(
        date=row.get("date", ""),
        room_number=row.get("room_number", ""),
        room_type=row.get("room_type", ""),
        status=row.get("status", ""),
        booking_id=row.get("booking_id", ""),
        guest_name=row.get("guest_name", ""),
        check_in=row.get("check_in", ""),
        check_out=row.get("check_out", ""),
        channel=row.get("channel", ""),
        nightly_rate_nzd=row.get("nightly_rate_nzd", ""),
        notes=row.get("notes", ""),
    )


def _load_rows(csv_path):
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append({key: (value or "").strip() for key, value in row.items()})
    return rows


def _tokenize(text):
    return _TOKEN_RE.findall(text.lower())


def _query_tokens(query):
    return {token for token in _tokenize(query) if token not in _STOPWORDS}


def _parse_date_str(value):
    if not value:
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def _normalize_date(text):
    if not text:
        return None
    match = _DATE_RE.search(text)
    if not match:
        return None
    year, month, day = match.groups()
    try:
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    except ValueError:
        return None


def _resolve_query_date(text):
    if not text:
        return None, False
    normalized = _normalize_date(text)
    if normalized:
        parsed = _parse_date_str(normalized)
        return parsed, True

    lowered = text.lower()
    today = date.today()
    if "day after tomorrow" in lowered:
        return today + timedelta(days=2), False
    if "tomorrow" in lowered:
        return today + timedelta(days=1), False
    if "today" in lowered:
        return today, False

    match = _MONTH_DAY_RE.search(lowered) or _MONTH_DAY_RE_REV.search(lowered)
    if match:
        day = int(match.group("day"))
        month_token = match.group("month")
        month = _MONTHS.get(month_token[:3], _MONTHS.get(month_token, 0))
        year_text = match.group("year")
        year = int(year_text) if year_text else today.year
        try:
            return date(year, month, day), bool(year_text)
        except ValueError:
            return None, False

    match = _WEEKDAY_RE.search(lowered)
    if match:
        qualifier = match.group("qualifier")
        weekday = _WEEKDAYS[match.group("weekday")]
        days_ahead = (weekday - today.weekday()) % 7
        if qualifier == "next" and days_ahead == 0:
            days_ahead = 7
        return today + timedelta(days=days_ahead), False

    return None, False


def _embed_texts(client, texts):
    response = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [item.embedding for item in response.data]


@lru_cache(maxsize=128)
def _embed_query_cached(model, query):
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    response = client.embeddings.create(model=model, input=[query])
    return response.data[0].embedding


def _rebuild_index(conn, csv_path, rows):
    conn.execute("DELETE FROM booking_vectors WHERE csv_path = ?", (csv_path,))
    conn.commit()

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    batch_size = 100
    to_insert = []

    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        texts = [_format_row_text(row) for row in batch]
        embeddings = _embed_texts(client, texts)
        for offset, embedding in enumerate(embeddings):
            idx = start + offset
            to_insert.append(
                (
                    csv_path,
                    idx,
                    json.dumps(batch[offset]),
                    texts[offset],
                    json.dumps(embedding),
                )
            )

    conn.executemany(
        """
        INSERT INTO booking_vectors (
            csv_path,
            row_index,
            row_json,
            row_text,
            embedding_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        to_insert,
    )
    conn.commit()


def _ensure_index(csv_path, db_path):
    csv_path = os.path.abspath(csv_path)
    if not os.path.exists(csv_path):
        return

    try:
        mtime = str(os.path.getmtime(csv_path))
    except OSError:
        return

    conn = _get_conn(db_path)
    try:
        _init_db(conn)
        stored_path = _get_meta(conn, "csv_path")
        stored_mtime = _get_meta(conn, "csv_mtime")
        stored_model = _get_meta(conn, "embed_model")
        row_count = conn.execute(
            "SELECT COUNT(*) AS count FROM booking_vectors WHERE csv_path = ?",
            (csv_path,),
        ).fetchone()["count"]
        if (
            stored_path == csv_path
            and stored_mtime == mtime
            and stored_model == EMBED_MODEL
            and row_count > 0
        ):
            return

        rows = _load_rows(csv_path)
        if rows:
            try:
                _rebuild_index(conn, csv_path, rows)
            except OpenAIError:
                return
            _set_meta(conn, "csv_path", csv_path)
            _set_meta(conn, "csv_mtime", mtime)
            _set_meta(conn, "embed_model", EMBED_MODEL)
    finally:
        conn.close()


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


def build_booking_context(query, csv_path, max_rows=5, db_path=None):
    rows = find_relevant_rows(query, csv_path, max_rows=max_rows, db_path=db_path)
    if not rows:
        return ""

    lines = [
        "Relevant booking rows from the motel availability CSV.",
        "Use only these rows to answer availability or booking questions.",
    ]
    for row in rows:
        lines.append(
            "- date: {date}, room_number: {room_number}, room_type: {room_type}, "
            "status: {status}, check_in: {check_in}, check_out: {check_out}, "
            "guest_name: {guest_name}, booking_id: {booking_id}, "
            "nightly_rate_nzd: {nightly_rate_nzd}, notes: {notes}".format(
                date=row.get("date", ""),
                room_number=row.get("room_number", ""),
                room_type=row.get("room_type", ""),
                status=row.get("status", ""),
                check_in=row.get("check_in", ""),
                check_out=row.get("check_out", ""),
                guest_name=row.get("guest_name", ""),
                booking_id=row.get("booking_id", ""),
                nightly_rate_nzd=row.get("nightly_rate_nzd", ""),
                notes=row.get("notes", ""),
            )
        )
    return "\n".join(lines)
