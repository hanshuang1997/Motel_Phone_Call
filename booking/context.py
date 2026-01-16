import re

from .search import find_relevant_rows

_AVAILABILITY_TERMS = {
    "available",
    "availability",
    "vacant",
    "vacancy",
    "open",
    "free",
}
_ROOM_NUMBER_RE = re.compile(r"\b(room numbers?|room #|room no\.?|which room)\b")


def _wants_availability(query):
    tokens = set(re.findall(r"[a-z0-9]+", (query or "").lower()))
    return bool(tokens & _AVAILABILITY_TERMS)


def _wants_room_numbers(query):
    return bool(_ROOM_NUMBER_RE.search((query or "").lower()))


def _format_room_number_list(room_numbers, preview_limit=4):
    if not room_numbers:
        return None, 0
    preview = room_numbers[:preview_limit]
    remaining = max(0, len(room_numbers) - len(preview))
    return ", ".join(preview), remaining


def build_booking_context(query, csv_path, max_rows=5, db_path=None):
    wants_room_numbers = _wants_room_numbers(query)
    availability_only = _wants_availability(query) or wants_room_numbers
    rows, summary = find_relevant_rows(
        query,
        csv_path,
        max_rows=max_rows,
        db_path=db_path,
        include_summary=True,
        availability_only=availability_only,
    )
    if not rows and not summary:
        return ""

    lines = [
        "Booking availability summary from the motel availability CSV.",
    ]
    if summary:
        if summary.get("date_label"):
            lines.append("Date: {date_label}.".format(**summary))
        if summary.get("room_type_filter"):
            lines.append(
                "Room type filter: {room_type}.".format(
                    room_type=summary["room_type_filter"]
                )
            )
        lines.append(
            "Status filter: {label}.".format(
                label="available only"
                if summary.get("availability_only")
                else "all statuses"
            )
        )
        if summary.get("summary_complete"):
            if summary.get("total", 0) == 0:
                lines.append("No matching rooms found.")
            else:
                lines.append("Counts by room_type:")
                for room_type in sorted(summary.get("room_type_counts", {})):
                    lines.append(
                        "- {room_type}: {count}".format(
                            room_type=room_type,
                            count=summary["room_type_counts"][room_type],
                        )
                    )
                lines.append(
                    "Total rooms: {total}.".format(total=summary.get("total", 0))
                )
            lines.append(
                "Use the counts above for availability questions. Do not infer counts from sample rows."
            )
            lines.append(
                "If multiple room types are available, ask a preference before listing rooms. "
                "Do not list more than 3 room numbers unless explicitly requested."
            )
        else:
            lines.append(
                "Note: retrieval is partial; do not provide exact counts unless explicitly listed."
            )

        if wants_room_numbers:
            if summary.get("summary_complete"):
                preview, remaining = _format_room_number_list(
                    summary.get("room_numbers", [])
                )
                if preview:
                    suffix = (
                        " (and {remaining} more)".format(remaining=remaining)
                        if remaining > 0
                        else ""
                    )
                    lines.append(
                        "Room numbers (use verbatim): {preview}{suffix}.".format(
                            preview=preview,
                            suffix=suffix,
                        )
                    )
                    if remaining > 0:
                        lines.append("Ask if they want the full list.")
            else:
                lines.append(
                    "Room numbers requested, but full list is unavailable; ask a follow-up."
                )

    if rows:
        lines.append("Sample rows (up to {max_rows}):".format(max_rows=max_rows))
        for row in rows:
            lines.append(
                "- date: {date}, room_number: {room_number}, room_type: {room_type}, "
                "status: {status}, check_in: {check_in}, check_out: {check_out}, "
                "guest_name: {guest_name}, booking_id: {booking_id}, "
                "nightly_rate_nzd: {nightly_rate_nzd}, notes: {notes}, "
                "floor: {floor}, bed_setup: {bed_setup}, max_guests: {max_guests}, "
                "room_size_sqm: {room_size_sqm}, kitchenette: {kitchenette}, "
                "amenities: {amenities}, view: {view}, accessible: {accessible}, "
                "room_type_description: {room_type_description}, rate_source: {rate_source}, "
                "pricing_reason: {pricing_reason}".format(
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
            )
    return "\n".join(lines)
