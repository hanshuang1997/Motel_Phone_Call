import sqlite3

from .config import DB_PATH


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.commit()


def get_meta(conn, key):
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn, key, value):
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()


def delete_meta(conn, key):
    conn.execute("DELETE FROM meta WHERE key = ?", (key,))
    conn.commit()


def reset_conversation(conn):
    conn.execute("DELETE FROM messages")
    conn.commit()


def save_message(role, content):
    conn = get_db()
    try:
        init_db(conn)
        conn.execute(
            "INSERT INTO messages (role, content) VALUES (?, ?)",
            (role, content),
        )
        conn.commit()
    finally:
        conn.close()


def load_messages():
    conn = get_db()
    try:
        init_db(conn)
        rows = conn.execute(
            "SELECT role, content FROM messages ORDER BY id ASC"
        ).fetchall()
    finally:
        conn.close()
    return [{"role": row["role"], "content": row["content"]} for row in rows]


def get_last_assistant_message():
    conn = get_db()
    try:
        init_db(conn)
        row = conn.execute(
            "SELECT content FROM messages WHERE role = ? ORDER BY id DESC LIMIT 1",
            ("assistant",),
        ).fetchone()
    finally:
        conn.close()
    return row["content"] if row else ""
