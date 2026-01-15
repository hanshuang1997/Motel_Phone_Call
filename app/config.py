import os
import re

from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.environ.get("CHAT_DB_PATH") or (
    "/tmp/chat.db" if os.environ.get("VERCEL") else "chat.db"
)
BOOKING_CSV_PATH = os.environ.get("BOOKING_CSV_PATH") or os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "motel_week_availability.csv"
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
    "When asked for counts or availability, use the availability summary counts in the context "
    "and do not guess or infer counts from sample rows. "
    "If a room-number list is provided in the context, use it verbatim and do not add or change rooms. "
    "If there are more than 3 options, summarize by room type and ask a brief preference question. "
    "Do not list more than 3 room numbers unless explicitly requested. "
    "If information is missing or unclear, ask a brief follow-up or say you do not have it. "
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
