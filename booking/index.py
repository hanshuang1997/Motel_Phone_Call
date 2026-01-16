import csv
import json
import os
import sqlite3

from openai import OpenAI, OpenAIError

from .embeddings import EMBED_MODEL, _embed_texts


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
        "nightly_rate_nzd: {nightly_rate_nzd}; notes: {notes}; "
        "floor: {floor}; bed_setup: {bed_setup}; max_guests: {max_guests}; "
        "room_size_sqm: {room_size_sqm}; kitchenette: {kitchenette}; "
        "amenities: {amenities}; view: {view}; accessible: {accessible}; "
        "room_type_description: {room_type_description}; rate_source: {rate_source}; "
        "pricing_reason: {pricing_reason}"
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
        floor=row.get("floor", ""),
        bed_setup=row.get("bed_setup", ""),
        max_guests=row.get("max_guests", ""),
        room_size_sqm=row.get("room_size_sqm", ""),
        kitchenette=row.get("kitchenette", ""),
        amenities=row.get("amenities", ""),
        view=row.get("view", ""),
        accessible=row.get("accessible", ""),
        room_type_description=row.get("room_type_description", ""),
        rate_source=row.get("rate_source", ""),
        pricing_reason=row.get("pricing_reason", ""),
    )


def _load_rows(csv_path):
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append({key: (value or "").strip() for key, value in row.items()})
    return rows


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
