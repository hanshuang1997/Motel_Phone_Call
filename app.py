import os
import sqlite3

from dotenv import load_dotenv
import re

from flask import Flask, Response, request
from openai import OpenAI, OpenAIError
from twilio.twiml.voice_response import Gather, VoiceResponse

from booking_context import build_booking_context
# This file depends on booking_context.py for building booking context from CSV
load_dotenv()

app = Flask(__name__)

DB_PATH = os.environ.get("CHAT_DB_PATH") or (
    "/tmp/chat.db" if os.environ.get("VERCEL") else "chat.db"
)
BOOKING_CSV_PATH = os.environ.get("BOOKING_CSV_PATH") or os.path.join(
    os.path.dirname(__file__), "motel_week_availability.csv"
)
BOOKING_DB_PATH = os.environ.get("BOOKING_DB_PATH")
try:
    BOOKING_TOP_K = int(os.environ.get("BOOKING_TOP_K", "10"))
except ValueError:
    BOOKING_TOP_K = 10
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
SYSTEM_PROMPT = (
    "You are a helpful phone call assistant for a motel. Keep responses concise, natural, "
    "the main goal is to guide for hotel bookings over the phone and any special requests. "
    "If random questions are asked, try to bring the topic back to hotel bookings and requests. "
    "Use the booking data provided in system context to answer availability questions. "
    "When asked for counts or availability, count rows explicitly from the provided context "
    "and do not guess. If information is missing or unclear, ask a brief follow-up or say you do not have it. "
    "Do not generate super long sentences, and response is suitable for being read aloud."
)
AVAILABILITY_KEYWORDS = {
    "available",
    "availability",
    "book",
    "booking",
    "check",
    "checkin",
    "checkout",
    "date",
    "night",
    "occupancy",
    "occupied",
    "reserve",
    "reservation",
    "room",
    "rooms",
    "stay",
    "today",
    "tomorrow",
    "tonight",
    "next",
    "vacant",
    "vacancy",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "jan",
    "january",
    "feb",
    "february",
    "mar",
    "march",
    "apr",
    "april",
    "may",
    "jun",
    "june",
    "jul",
    "july",
    "aug",
    "august",
    "sep",
    "sept",
    "september",
    "oct",
    "october",
    "nov",
    "november",
    "dec",
    "december",
}
DATE_RE = re.compile(r"\b20\d{2}-\d{1,2}-\d{1,2}\b")


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


def get_current_call_sid(conn):
    row = conn.execute("SELECT value FROM meta WHERE key = 'call_sid'").fetchone()
    return row["value"] if row else None


def set_current_call_sid(conn, call_sid):
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('call_sid', ?)",
        (call_sid,),
    )
    conn.commit()


def reset_conversation(conn):
    conn.execute("DELETE FROM messages")
    conn.commit()


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


def set_pending_user_text(content):
    conn = get_db()
    try:
        init_db(conn)
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('pending_user_text', ?)",
            (content,),
        )
        conn.commit()
    finally:
        conn.close()


def pop_pending_user_text():
    conn = get_db()
    try:
        init_db(conn)
        row = conn.execute(
            "SELECT value FROM meta WHERE key = 'pending_user_text'"
        ).fetchone()
        conn.execute("DELETE FROM meta WHERE key = 'pending_user_text'")
        conn.commit()
    finally:
        conn.close()
    return row["value"] if row else ""


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


def should_use_booking_context(user_text):
    if not user_text:
        return False
    text = user_text.lower()
    tokens = set(re.findall(r"[a-z0-9]+", text))
    if tokens & AVAILABILITY_KEYWORDS:
        return True
    return DATE_RE.search(text) is not None


def generate_reply(user_text, save_user=True):
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return "The assistant is not configured right now. Please try again later."

    try:
        client = OpenAI(api_key=api_key)
        if save_user and user_text:
            save_message("user", user_text)
        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        if should_use_booking_context(user_text):
            booking_context = build_booking_context(
                user_text,
                BOOKING_CSV_PATH,
                max_rows=BOOKING_TOP_K,
                db_path=BOOKING_DB_PATH,
            )
            if booking_context:
                messages.append({"role": "system", "content": booking_context})
        messages.extend(load_messages())
        completion = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            temperature=0,
        )
        reply = completion.choices[0].message.content.strip()
        if reply:
            save_message("assistant", reply)
        return reply or "Sorry, I don't have a response right now."
    except (OpenAIError, sqlite3.Error):
        return "Sorry, I'm having trouble right now. Please try again."


def build_gather():
    return Gather(
        input="speech",
        action="/voice/respond",
        method="POST",
        speech_timeout="auto",
    )


@app.route("/voice", methods=["POST"])
def voice():
    call_sid = request.form.get("CallSid")
    ensure_call_context(call_sid)

    resp = VoiceResponse()
    gather = build_gather()
    gather.say(
        "Hello! Thanks for calling. Welcome to superstar motel. How can I help you today?",
        voice="Polly.Joanna",
    )
    resp.append(gather)
    resp.redirect("/voice", method="POST")
    return Response(str(resp), mimetype="text/xml")


@app.route("/voice/respond", methods=["POST"])
def voice_respond():
    call_sid = request.form.get("CallSid")
    ensure_call_context(call_sid)

    user_text = request.form.get("SpeechResult", "").strip()
    resp = VoiceResponse()
    if not user_text:
        resp.say(
            "Sorry, I didn't catch that. Please say that again.",
            voice="Polly.Joanna",
        )
        resp.redirect("/voice", method="POST")
        return Response(str(resp), mimetype="text/xml")

    if should_use_booking_context(user_text):
        save_message("user", user_text)
        set_pending_user_text(user_text)
        resp.say(
            "Thanks, give me a moment while I check the right room for you.",
            voice="Polly.Joanna",
        )
        resp.redirect("/voice/answer", method="POST")
        return Response(str(resp), mimetype="text/xml")

    reply = generate_reply(user_text, save_user=True)
    resp.say(reply, voice="Polly.Joanna")
    gather = build_gather()
    resp.append(gather)
    resp.redirect("/voice", method="POST")
    return Response(str(resp), mimetype="text/xml")


@app.route("/voice/answer", methods=["POST"])
def voice_answer():
    call_sid = request.form.get("CallSid")
    ensure_call_context(call_sid)

    user_text = pop_pending_user_text()
    resp = VoiceResponse()
    if not user_text:
        resp.say(
            "Sorry, I didn't catch that. Please say that again.",
            voice="Polly.Joanna",
        )
        resp.redirect("/voice", method="POST")
        return Response(str(resp), mimetype="text/xml")

    reply = generate_reply(user_text, save_user=False)
    resp.say(reply, voice="Polly.Joanna")
    gather = build_gather()
    resp.append(gather)
    resp.redirect("/voice", method="POST")
    return Response(str(resp), mimetype="text/xml")


if __name__ == "__main__":
    app.run(debug=True)
