from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from mma_api import get_fights_for_season


def parse_fight_datetime(fight_record: dict) -> datetime | None:
    fight_date = fight_record.get("date")

    if not fight_date:
        return None

    return datetime.fromisoformat(fight_date.replace("Z", "+00:00"))


def is_upcoming_ufc_fight(fight_record: dict) -> bool:
    event_name = str(fight_record.get("slug") or "")
    fight_datetime = parse_fight_datetime(fight_record)
    fight_status = fight_record.get("status") or {}
    short_status = fight_status.get("short")

    if not event_name.upper().startswith("UFC"):
        return False

    if fight_datetime is None:
        return False

    if short_status == "CANC":
        return False

    return fight_datetime > datetime.now(timezone.utc)


async def main() -> None:
    current_season = datetime.now(timezone.utc).year
    all_fight_records = await get_fights_for_season(current_season)

    upcoming_ufc_fights = [
        fight_record
        for fight_record in all_fight_records
        if is_upcoming_ufc_fight(fight_record)
    ]

    if not upcoming_ufc_fights:
        print("No upcoming UFC fights were found.")
        return

    upcoming_ufc_fights.sort(
        key=lambda fight_record: parse_fight_datetime(fight_record)
    )

    next_event_name = upcoming_ufc_fights[0]["slug"]

    next_event_fights = [
        fight_record
        for fight_record in upcoming_ufc_fights
        if fight_record.get("slug") == next_event_name
    ]

    next_event_fights.sort(
        key=lambda fight_record: parse_fight_datetime(fight_record)
    )

    event_datetime = parse_fight_datetime(next_event_fights[0])
    local_event_datetime = event_datetime.astimezone()

    print(f"Next UFC event: {next_event_name}")
    print(
        "Earliest listed fight time: "
        f"{local_event_datetime.strftime('%A, %B %d, %Y at %I:%M %p %Z')}"
    )
    print(f"Fights found: {len(next_event_fights)}")
    print()

    for fight_number, fight_record in enumerate(next_event_fights, start=1):
        fighters = fight_record.get("fighters") or {}
        first_fighter = fighters.get("first") or {}
        second_fighter = fighters.get("second") or {}

        first_fighter_name = first_fighter.get("name", "Unknown fighter")
        second_fighter_name = second_fighter.get("name", "Unknown fighter")
        weight_class = fight_record.get("category") or "Unknown weight class"

        print(
            f"{fight_number}. {first_fighter_name} vs. "
            f"{second_fighter_name} — {weight_class}"
        )


if __name__ == "__main__":
    asyncio.run(main())
