import csv
import os
import re

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
_CACHE = {"path": None, "mtime": None, "rows": []}


def _tokenize(text):
    return _TOKEN_RE.findall(text.lower())


def _query_tokens(query):
    return {token for token in _tokenize(query) if token not in _STOPWORDS}


def _availability_intent(query_text, query_tokens):
    text = query_text.lower()
    wants_available = any(token in {"available", "vacant", "open"} for token in query_tokens) or (
        "availability" in text
    )
    wants_unavailable = any(
        token in {"occupied", "unavailable", "booked", "outofservice", "maintenance"}
        for token in query_tokens
    ) or ("not available" in text)
    return wants_available, wants_unavailable


def _load_rows(csv_path):
    csv_path = os.path.abspath(csv_path)
    try:
        mtime = os.path.getmtime(csv_path)
    except OSError:
        return []

    if _CACHE["path"] == csv_path and _CACHE["mtime"] == mtime:
        return _CACHE["rows"]

    rows = []
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            cleaned = {key: (value or "").strip() for key, value in row.items()}
            blob = " ".join(value for value in cleaned.values() if value).lower()
            tokens = set(_tokenize(blob))
            rows.append({"data": cleaned, "tokens": tokens, "blob": blob})

    _CACHE["path"] = csv_path
    _CACHE["mtime"] = mtime
    _CACHE["rows"] = rows
    return rows


def find_relevant_rows(query, csv_path, max_rows=5):
    rows = _load_rows(csv_path)
    if not rows:
        return []

    query_tokens = _query_tokens(query)
    query_text = " ".join(_tokenize(query))
    wants_available, wants_unavailable = _availability_intent(query, query_tokens)

    scored = []
    for row in rows:
        status = row["data"].get("status", "").lower()
        score = sum(1 for token in query_tokens if token in row["tokens"])
        if query_text and query_text in row["blob"]:
            score += 2
        if wants_available and status == "available":
            score += 2
        if wants_unavailable and status and status != "available":
            score += 2
        if score > 0:
            scored.append((score, row))

    if not scored and (wants_available or wants_unavailable or {"room", "rooms"} & query_tokens):
        for row in rows:
            status = row["data"].get("status", "").lower()
            if wants_available and status != "available":
                continue
            if wants_unavailable and status == "available":
                continue
            scored.append((1, row))

    scored.sort(
        key=lambda item: (
            -item[0],
            item[1]["data"].get("date", ""),
            item[1]["data"].get("room_number", ""),
        )
    )
    return [item[1]["data"] for item in scored[:max_rows]]


def build_booking_context(query, csv_path, max_rows=5):
    rows = find_relevant_rows(query, csv_path, max_rows=max_rows)
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
