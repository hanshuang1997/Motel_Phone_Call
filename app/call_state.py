from .db import (
    delete_meta,
    get_db,
    get_meta,
    init_db,
    reset_conversation,
    set_meta,
)


def get_current_call_sid(conn):
    return get_meta(conn, "call_sid")


def set_current_call_sid(conn, call_sid):
    set_meta(conn, "call_sid", call_sid)


def ensure_call_context(call_sid):
    if not call_sid:
        return
    conn = get_db()
    try:
        init_db(conn)
        current_call_sid = get_current_call_sid(conn)
        if current_call_sid != call_sid:
            reset_conversation(conn)
            set_current_call_sid(conn, call_sid)
    finally:
        conn.close()


def set_pending_user_text(content):
    conn = get_db()
    try:
        init_db(conn)
        set_meta(conn, "pending_user_text", content)
    finally:
        conn.close()


def pop_pending_user_text():
    conn = get_db()
    try:
        init_db(conn)
        value = get_meta(conn, "pending_user_text")
        delete_meta(conn, "pending_user_text")
    finally:
        conn.close()
    return value or ""
