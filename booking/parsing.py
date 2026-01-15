import re
from datetime import date, timedelta

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
