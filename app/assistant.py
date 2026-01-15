import os
import re
import sqlite3

from openai import OpenAI, OpenAIError

from booking.context import build_booking_context

from .config import (
    AVAILABILITY_KEYWORDS,
    BOOKING_CSV_PATH,
    BOOKING_DB_PATH,
    BOOKING_TOP_K,
    DATE_RE,
    MODEL,
    SYSTEM_PROMPT,
)
from .db import load_messages, save_message


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
