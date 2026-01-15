from .search import find_relevant_rows


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
