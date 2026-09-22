from __future__ import annotations

import asyncio
import hashlib
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

import discord
from discord import app_commands

from ufc_pickem.database import (
    add_fight,
    create_database_connection,
    create_event,
    get_event,
    get_event_fights,
    get_fight,
    save_pick,
)
from ufc_pickem.mma_api import (
    get_fights_for_season,
    get_odds_for_date,
    get_odds_for_fight,
)
from ufc_pickem.scoring import calculate_pick_score


AUTOMATIC_SYNC_INTERVAL_SECONDS = 30 * 60
ODDS_DIRECT_RETRY_INTERVAL = timedelta(hours=6)
RECENT_RENDER_SKIP_INTERVAL = timedelta(minutes=5)
POSTING_WINDOW_DAYS = 7
FIGHTS_PER_MESSAGE = 5
FIGHTER_HISTORY_YEARS = 5
REMINDER_WINDOWS = {
    "24-hour": timedelta(hours=24),
    "2-hour": timedelta(hours=2),
}

background_sync_task: asyncio.Task | None = None
event_lock_tasks: dict[int, asyncio.Task] = {}
event_lock_timestamps: dict[int, int] = {}
registered_message_ids: set[int] = set()
ufc_synchronization_lock = asyncio.Lock()
recent_event_render_signatures: dict[
    tuple[int, int],
    tuple[str, datetime],
] = {}


@dataclass
class LeaderboardStanding:
    discord_user_id: int
    display_name: str
    points: int = 0
    correct_winners: int = 0
    scored_picks: int = 0
    current_streak: int = 0
    best_streak: int = 0
    event_wins: int = 0

    @property
    def accuracy(self) -> float:
        if self.scored_picks == 0:
            return 0.0

        return (self.correct_winners / self.scored_picks) * 100


@dataclass
class OddsSyncSummary:
    stored_fight_count: int = 0
    date_response_count: int = 0
    direct_response_count: int = 0
    matched_response_count: int = 0


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    parsed_datetime = datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )

    if parsed_datetime.tzinfo is None:
        parsed_datetime = parsed_datetime.replace(tzinfo=timezone.utc)

    return parsed_datetime.astimezone(timezone.utc)


def parse_api_datetime(fight_record: dict) -> datetime | None:
    return parse_iso_datetime(fight_record.get("date"))


def get_api_status(fight_record: dict) -> str:
    status = fight_record.get("status") or {}
    return str(status.get("short") or "").upper()


def get_fighter_data(
    fight_record: dict,
    fighter_position: str,
) -> dict:
    fighters = fight_record.get("fighters") or {}
    fighter_data = fighters.get(fighter_position) or {}
    return fighter_data if isinstance(fighter_data, dict) else {}


def get_fighter_name(
    fight_record: dict,
    fighter_position: str,
) -> str:
    fighter_data = get_fighter_data(fight_record, fighter_position)
    return str(
        fighter_data.get("name") or "Unknown fighter"
    ).strip()


def get_result_detail(
    fight_record: dict,
    possible_keys: tuple[str, ...],
) -> object | None:
    possible_sources = [
        fight_record,
        fight_record.get("result"),
        fight_record.get("results"),
        fight_record.get("status"),
    ]

    for possible_source in possible_sources:
        if not isinstance(possible_source, dict):
            continue

        for possible_key in possible_keys:
            value = possible_source.get(possible_key)

            if value not in (None, ""):
                return value

    return None


def normalize_result_method(value: object | None) -> str | None:
    if value is None:
        return None

    method_text = str(value).strip()
    lowered_method = method_text.casefold()

    if "decision" in lowered_method:
        return "Decision"

    if "submission" in lowered_method or "sub" == lowered_method:
        return "Submission"

    if "ko" in lowered_method or "tko" in lowered_method:
        return "KO/TKO"

    return method_text[:100] or None


def extract_result(
    fight_record: dict,
) -> tuple[str | None, str | None, int | None]:
    first_fighter = get_fighter_data(fight_record, "first")
    second_fighter = get_fighter_data(fight_record, "second")
    winner: str | None = None

    if first_fighter.get("winner") is True:
        winner = get_fighter_name(fight_record, "first")
    elif second_fighter.get("winner") is True:
        winner = get_fighter_name(fight_record, "second")

    result_method = normalize_result_method(
        get_result_detail(
            fight_record,
            ("method", "win_by", "result_method"),
        )
    )
    ending_round_value = get_result_detail(
        fight_record,
        ("round", "ending_round", "last_round"),
    )
    ending_round: int | None = None

    if ending_round_value is not None:
        try:
            possible_round = int(ending_round_value)

            if 1 <= possible_round <= 5:
                ending_round = possible_round
        except (TypeError, ValueError):
            ending_round = None

    return winner, result_method, ending_round


def add_column_if_missing(
    database_connection,
    table_name: str,
    column_name: str,
    column_definition: str,
) -> None:
    column_rows = database_connection.execute(
        f"PRAGMA table_info({table_name})"
    ).fetchall()
    existing_column_names = {
        str(column_row["name"])
        for column_row in column_rows
    }

    if column_name not in existing_column_names:
        database_connection.execute(
            f"ALTER TABLE {table_name} "
            f"ADD COLUMN {column_name} {column_definition}"
        )


def initialize_complete_tables() -> None:
    with create_database_connection() as database_connection:
        database_connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS automatic_events (
                api_event_key TEXT PRIMARY KEY,
                event_id INTEGER NOT NULL UNIQUE,
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS automatic_fights (
                api_fight_id INTEGER PRIMARY KEY,
                fight_id INTEGER NOT NULL UNIQUE,
                scheduled_time TEXT NOT NULL,
                weight_class TEXT,
                FOREIGN KEY (fight_id)
                    REFERENCES fights(fight_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                ufc_channel_id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS event_posts (
                guild_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                header_message_id INTEGER NOT NULL,
                posted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, event_id),
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS fight_posts (
                guild_id INTEGER NOT NULL,
                fight_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                section_number INTEGER NOT NULL,
                PRIMARY KEY (guild_id, fight_id),
                FOREIGN KEY (fight_id)
                    REFERENCES fights(fight_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS event_snapshots (
                event_id INTEGER PRIMARY KEY,
                snapshot_hash TEXT NOT NULL,
                snapshot_text TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS event_change_posts (
                guild_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                change_hash TEXT NOT NULL,
                posted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, event_id, change_hash),
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS event_reminders (
                guild_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                reminder_kind TEXT NOT NULL,
                sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, event_id, reminder_kind),
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS event_recaps (
                guild_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                posted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, event_id),
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS fight_outcomes (
                fight_id INTEGER PRIMARY KEY,
                outcome_type TEXT NOT NULL,
                outcome_text TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fight_id)
                    REFERENCES fights(fight_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS fight_odds (
                fight_id INTEGER PRIMARY KEY,
                fighter_one_american INTEGER NOT NULL,
                fighter_two_american INTEGER NOT NULL,
                fighter_one_probability REAL NOT NULL,
                fighter_two_probability REAL NOT NULL,
                bookmaker_count INTEGER NOT NULL,
                source_updated_at TEXT,
                fetched_at TEXT NOT NULL,
                FOREIGN KEY (fight_id)
                    REFERENCES fights(fight_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS fight_odds_history (
                odds_history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                fight_id INTEGER NOT NULL,
                fighter_one_american INTEGER NOT NULL,
                fighter_two_american INTEGER NOT NULL,
                fighter_one_probability REAL NOT NULL,
                fighter_two_probability REAL NOT NULL,
                bookmaker_count INTEGER NOT NULL,
                source_updated_at TEXT,
                recorded_at TEXT NOT NULL,
                FOREIGN KEY (fight_id)
                    REFERENCES fights(fight_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS pick_invalidations (
                invalidation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                fight_id INTEGER NOT NULL,
                discord_user_id INTEGER NOT NULL,
                old_pick TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                notified_at TEXT,
                UNIQUE(fight_id, discord_user_id, old_pick, reason),
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (fight_id)
                    REFERENCES fights(fight_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ufc_sync_status (
                status_key TEXT PRIMARY KEY,
                status_value TEXT NOT NULL
            );
            """
        )
        add_column_if_missing(
            database_connection,
            "automatic_fights",
            "fighter_one_api_id",
            "INTEGER",
        )
        add_column_if_missing(
            database_connection,
            "automatic_fights",
            "fighter_two_api_id",
            "INTEGER",
        )
        add_column_if_missing(
            database_connection,
            "automatic_fights",
            "last_api_status",
            "TEXT",
        )
        add_column_if_missing(
            database_connection,
            "automatic_fights",
            "is_main",
            "INTEGER NOT NULL DEFAULT 0",
        )
        add_column_if_missing(
            database_connection,
            "guild_settings",
            "reminders_enabled",
            "INTEGER NOT NULL DEFAULT 1",
        )


async def get_ufc_fights_for_seasons(
    seasons: list[int],
) -> list[dict]:
    season_results = await asyncio.gather(
        *(
            get_fights_for_season(season)
            for season in seasons
        ),
        return_exceptions=True,
    )
    all_fight_records: list[dict] = []
    failed_seasons: list[int] = []

    for season, season_result in zip(seasons, season_results):
        if isinstance(season_result, Exception):
            failed_seasons.append(season)
            print(
                f"Could not retrieve MMA season {season}: "
                f"{season_result}"
            )
            continue

        all_fight_records.extend(
            fight_record
            for fight_record in season_result
            if str(fight_record.get("slug") or "")
            .upper()
            .startswith("UFC")
        )

    if not all_fight_records:
        failed_season_text = ", ".join(
            str(season)
            for season in failed_seasons
        )
        raise RuntimeError(
            "The MMA API did not return UFC data"
            + (
                f" for seasons {failed_season_text}."
                if failed_season_text
                else "."
            )
        )

    return all_fight_records


def group_fights_by_event(
    fight_records: list[dict],
) -> dict[str, list[dict]]:
    fights_by_event: dict[str, list[dict]] = defaultdict(list)

    for fight_record in fight_records:
        event_name = str(fight_record.get("slug") or "").strip()

        if event_name:
            fights_by_event[event_name].append(fight_record)

    return dict(fights_by_event)


def sort_event_fights(fight_records: list[dict]) -> list[dict]:
    return sorted(
        fight_records,
        key=lambda fight_record: (
            0 if fight_record.get("is_main") else 1,
            -int(fight_record.get("timestamp") or 0),
            int(fight_record.get("id") or 0),
        ),
    )


def build_api_event(
    event_name: str,
    fight_records: list[dict],
) -> dict | None:
    dated_fight_records = [
        fight_record
        for fight_record in fight_records
        if parse_api_datetime(fight_record) is not None
    ]

    if not dated_fight_records:
        return None

    event_start_time = min(
        parse_api_datetime(fight_record)
        for fight_record in dated_fight_records
        if parse_api_datetime(fight_record) is not None
    )
    return {
        "event_name": event_name,
        "event_start_time": event_start_time,
        "fight_records": sort_event_fights(dated_fight_records),
    }


def find_next_api_event(
    fight_records: list[dict],
) -> dict | None:
    now = datetime.now(timezone.utc)
    upcoming_fight_records = [
        fight_record
        for fight_record in fight_records
        if parse_api_datetime(fight_record) is not None
        and parse_api_datetime(fight_record) > now
        and get_api_status(fight_record) not in {"CANC", "FT"}
    ]

    if not upcoming_fight_records:
        return None

    next_fight_record = min(
        upcoming_fight_records,
        key=lambda fight_record: (
            parse_api_datetime(fight_record)
            or datetime.max.replace(tzinfo=timezone.utc)
        ),
    )
    next_event_name = str(next_fight_record.get("slug") or "")
    event_fights = [
        fight_record
        for fight_record in fight_records
        if str(fight_record.get("slug") or "") == next_event_name
    ]
    return build_api_event(next_event_name, event_fights)


def get_known_automatic_events() -> dict[str, int]:
    with create_database_connection() as database_connection:
        event_rows = database_connection.execute(
            """
            SELECT api_event_key, event_id
            FROM automatic_events
            """
        ).fetchall()

    return {
        str(event_row["api_event_key"]): int(event_row["event_id"])
        for event_row in event_rows
    }


def build_event_snapshot(fight_records: list[dict]) -> str:
    snapshot_lines = []

    for fight_record in sort_event_fights(fight_records):
        scheduled_datetime = parse_api_datetime(fight_record)
        scheduled_timestamp = (
            int(scheduled_datetime.timestamp())
            if scheduled_datetime is not None
            else ""
        )
        snapshot_lines.append(
            "|".join(
                (
                    str(fight_record.get("id") or ""),
                    get_fighter_name(fight_record, "first"),
                    get_fighter_name(fight_record, "second"),
                    get_api_status(fight_record),
                    str(scheduled_timestamp),
                )
            )
        )

    return "\n".join(snapshot_lines)


def compare_event_snapshots(
    previous_snapshot: str,
    current_snapshot: str,
) -> list[str]:
    def parse_snapshot(snapshot_text: str) -> dict[str, list[str]]:
        parsed_snapshot: dict[str, list[str]] = {}

        for snapshot_line in snapshot_text.splitlines():
            columns = snapshot_line.split("|")

            if len(columns) >= 5:
                parsed_snapshot[columns[0]] = columns

        return parsed_snapshot

    previous_fights = parse_snapshot(previous_snapshot)
    current_fights = parse_snapshot(current_snapshot)
    changes: list[str] = []
    time_changes: list[tuple[str, str, str]] = []

    for api_fight_id, current_columns in current_fights.items():
        current_matchup = (
            f"{current_columns[1]} vs. {current_columns[2]}"
        )
        previous_columns = previous_fights.get(api_fight_id)

        if previous_columns is None:
            if current_columns[3] != "CANC":
                changes.append(f"Added: {current_matchup}")
            continue

        previous_matchup = (
            f"{previous_columns[1]} vs. {previous_columns[2]}"
        )

        if previous_matchup != current_matchup:
            changes.append(
                f"Changed: {previous_matchup} → {current_matchup}"
            )

        if (
            previous_columns[3] != "CANC"
            and current_columns[3] == "CANC"
        ):
            changes.append(f"Canceled: {current_matchup}")

        if (
            previous_columns[4]
            and current_columns[4]
            and previous_columns[4] != current_columns[4]
        ):
            time_changes.append(
                (
                    current_matchup,
                    previous_columns[4],
                    current_columns[4],
                )
            )

    for api_fight_id, previous_columns in previous_fights.items():
        if api_fight_id not in current_fights:
            changes.append(
                "Removed: "
                f"{previous_columns[1]} vs. {previous_columns[2]}"
            )

    unique_time_changes = {
        (previous_timestamp, current_timestamp)
        for _, previous_timestamp, current_timestamp in time_changes
    }

    if len(unique_time_changes) == 1 and len(time_changes) > 1:
        previous_timestamp, current_timestamp = next(
            iter(unique_time_changes)
        )
        changes.append(
            "Event time changed: "
            f"<t:{previous_timestamp}:F> → "
            f"<t:{current_timestamp}:F>"
        )
    else:
        for matchup, previous_timestamp, current_timestamp in time_changes:
            changes.append(
                f"Time changed: {matchup} — "
                f"<t:{previous_timestamp}:F> → "
                f"<t:{current_timestamp}:F>"
            )

    return changes


def save_and_compare_event_snapshot(
    event_id: int,
    fight_records: list[dict],
) -> list[str]:
    current_snapshot = build_event_snapshot(fight_records)
    current_hash = hashlib.sha256(
        current_snapshot.encode("utf-8")
    ).hexdigest()

    with create_database_connection() as database_connection:
        previous_row = database_connection.execute(
            """
            SELECT snapshot_hash, snapshot_text
            FROM event_snapshots
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        database_connection.execute(
            """
            INSERT INTO event_snapshots (
                event_id,
                snapshot_hash,
                snapshot_text
            )
            VALUES (?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                snapshot_hash = excluded.snapshot_hash,
                snapshot_text = excluded.snapshot_text,
                updated_at = CURRENT_TIMESTAMP
            """,
            (event_id, current_hash, current_snapshot),
        )

    if previous_row is None:
        return []

    if str(previous_row["snapshot_hash"]) == current_hash:
        return []

    return compare_event_snapshots(
        str(previous_row["snapshot_text"]),
        current_snapshot,
    )


def invalidate_picks_for_replaced_fighter(
    event_id: int,
    fight_id: int,
    replaced_fighter_name: str,
    replacement_fighter_name: str,
) -> None:
    with create_database_connection() as database_connection:
        affected_pick_rows = database_connection.execute(
            """
            SELECT discord_user_id, predicted_winner
            FROM picks
            WHERE fight_id = ?
                AND predicted_winner = ?
            """,
            (fight_id, replaced_fighter_name),
        ).fetchall()

        for affected_pick_row in affected_pick_rows:
            database_connection.execute(
                """
                INSERT OR IGNORE INTO pick_invalidations (
                    event_id,
                    fight_id,
                    discord_user_id,
                    old_pick,
                    reason
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    fight_id,
                    int(affected_pick_row["discord_user_id"]),
                    str(affected_pick_row["predicted_winner"]),
                    (
                        f"{replaced_fighter_name} was replaced by "
                        f"{replacement_fighter_name}"
                    ),
                ),
            )

        database_connection.execute(
            """
            DELETE FROM picks
            WHERE fight_id = ?
                AND predicted_winner = ?
            """,
            (fight_id, replaced_fighter_name),
        )


def invalidate_picks_for_canceled_fight(
    event_id: int,
    fight_id: int,
    reason: str,
) -> None:
    with create_database_connection() as database_connection:
        affected_pick_rows = database_connection.execute(
            """
            SELECT discord_user_id, predicted_winner
            FROM picks
            WHERE fight_id = ?
            """,
            (fight_id,),
        ).fetchall()

        for affected_pick_row in affected_pick_rows:
            database_connection.execute(
                """
                INSERT OR IGNORE INTO pick_invalidations (
                    event_id,
                    fight_id,
                    discord_user_id,
                    old_pick,
                    reason
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    fight_id,
                    int(affected_pick_row["discord_user_id"]),
                    str(affected_pick_row["predicted_winner"]),
                    reason,
                ),
            )


def save_automatic_result(
    fight_id: int,
    fight_record: dict,
) -> None:
    winner, method, ending_round = extract_result(fight_record)

    with create_database_connection() as database_connection:
        if winner is None:
            database_connection.execute(
                """
                UPDATE fights
                SET status = 'completed'
                WHERE fight_id = ?
                """,
                (fight_id,),
            )
            database_connection.execute(
                """
                INSERT INTO fight_outcomes (
                    fight_id,
                    outcome_type,
                    outcome_text
                )
                VALUES (?, 'draw_or_no_contest', ?)
                ON CONFLICT(fight_id) DO UPDATE SET
                    outcome_type = excluded.outcome_type,
                    outcome_text = excluded.outcome_text,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    fight_id,
                    "Draw, no contest, or no winner supplied by the API",
                ),
            )
            return

        database_connection.execute(
            """
            INSERT INTO results (
                fight_id,
                winner,
                method,
                ending_round
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(fight_id) DO UPDATE SET
                winner = excluded.winner,
                method = COALESCE(excluded.method, results.method),
                ending_round = COALESCE(
                    excluded.ending_round,
                    results.ending_round
                ),
                recorded_at = CURRENT_TIMESTAMP
            """,
            (fight_id, winner, method, ending_round),
        )
        database_connection.execute(
            """
            UPDATE fights
            SET status = 'completed'
            WHERE fight_id = ?
            """,
            (fight_id,),
        )
        database_connection.execute(
            """
            INSERT INTO fight_outcomes (
                fight_id,
                outcome_type,
                outcome_text
            )
            VALUES (?, 'winner', ?)
            ON CONFLICT(fight_id) DO UPDATE SET
                outcome_type = excluded.outcome_type,
                outcome_text = excluded.outcome_text,
                updated_at = CURRENT_TIMESTAMP
            """,
            (fight_id, winner),
        )


def update_event_status(event_id: int) -> str:
    now = datetime.now(timezone.utc)
    event_row = get_event(event_id)
    fight_rows = get_event_fights(event_id)

    if event_row is None:
        return "upcoming"

    active_fights = [
        fight_row
        for fight_row in fight_rows
        if fight_row["status"] != "canceled"
    ]
    event_start_time = parse_iso_datetime(event_row["start_time"])

    if active_fights and all(
        fight_row["status"] == "completed"
        for fight_row in active_fights
    ):
        event_status = "completed"
    elif event_start_time is not None and now >= event_start_time:
        event_status = "active"
    else:
        event_status = "upcoming"

    with create_database_connection() as database_connection:
        database_connection.execute(
            """
            UPDATE events
            SET status = ?
            WHERE event_id = ?
            """,
            (event_status, event_id),
        )

        if event_status == "active":
            database_connection.execute(
                """
                UPDATE fights
                SET status = 'active'
                WHERE event_id = ?
                    AND status = 'upcoming'
                """,
                (event_id,),
            )

    return event_status


def synchronize_event_with_database(
    api_event: dict,
) -> tuple[int, list[str]]:
    event_name = str(api_event["event_name"])
    event_start_time: datetime = api_event["event_start_time"]
    fight_records: list[dict] = api_event["fight_records"]
    incoming_api_fight_ids = {
        int(fight_record["id"])
        for fight_record in fight_records
        if fight_record.get("id") is not None
    }

    with create_database_connection() as database_connection:
        automatic_event_row = database_connection.execute(
            """
            SELECT event_id
            FROM automatic_events
            WHERE api_event_key = ?
            """,
            (event_name,),
        ).fetchone()

        if automatic_event_row is None:
            possible_event_rows = database_connection.execute(
                """
                SELECT
                    automatic_events.api_event_key,
                    automatic_events.event_id,
                    events.start_time
                FROM automatic_events
                JOIN events
                    ON events.event_id = automatic_events.event_id
                WHERE events.status = 'upcoming'
                    AND EXISTS (
                        SELECT 1
                        FROM event_posts
                        WHERE event_posts.event_id = events.event_id
                    )
                """
            ).fetchall()

            best_overlap_row = None
            best_overlap_count = 0

            for possible_event_row in possible_event_rows:
                possible_fight_rows = database_connection.execute(
                    """
                    SELECT automatic_fights.api_fight_id
                    FROM automatic_fights
                    JOIN fights
                        ON fights.fight_id = automatic_fights.fight_id
                    WHERE fights.event_id = ?
                    """,
                    (int(possible_event_row["event_id"]),),
                ).fetchall()
                possible_api_fight_ids = {
                    int(possible_fight_row["api_fight_id"])
                    for possible_fight_row in possible_fight_rows
                }
                overlap_count = len(
                    incoming_api_fight_ids & possible_api_fight_ids
                )

                if overlap_count > best_overlap_count:
                    best_overlap_count = overlap_count
                    best_overlap_row = possible_event_row

            minimum_overlap = max(
                2,
                min(4, len(incoming_api_fight_ids) // 3),
            )

            if (
                best_overlap_row is not None
                and best_overlap_count >= minimum_overlap
            ):
                database_connection.execute(
                    """
                    UPDATE automatic_events
                    SET api_event_key = ?
                    WHERE event_id = ?
                    """,
                    (
                        event_name,
                        int(best_overlap_row["event_id"]),
                    ),
                )
                automatic_event_row = best_overlap_row

            for possible_event_row in possible_event_rows:
                if automatic_event_row is not None:
                    break

                possible_start_time = parse_iso_datetime(
                    possible_event_row["start_time"]
                )

                if possible_start_time is None:
                    continue

                start_time_difference = abs(
                    (
                        possible_start_time - event_start_time
                    ).total_seconds()
                )

                if start_time_difference <= 18 * 60 * 60:
                    database_connection.execute(
                        """
                        UPDATE automatic_events
                        SET api_event_key = ?
                        WHERE event_id = ?
                        """,
                        (
                            event_name,
                            int(possible_event_row["event_id"]),
                        ),
                    )
                    automatic_event_row = possible_event_row
                    break

    if automatic_event_row is None:
        event_id = create_event(
            event_name=event_name,
            start_time=event_start_time.isoformat(),
        )

        with create_database_connection() as database_connection:
            database_connection.execute(
                """
                INSERT INTO automatic_events (api_event_key, event_id)
                VALUES (?, ?)
                """,
                (event_name, event_id),
            )
    else:
        event_id = int(automatic_event_row["event_id"])

        with create_database_connection() as database_connection:
            database_connection.execute(
                """
                UPDATE events
                SET
                    event_name = ?,
                    start_time = ?
                WHERE event_id = ?
                """,
                (
                    event_name,
                    event_start_time.isoformat(),
                    event_id,
                ),
            )

    changes = save_and_compare_event_snapshot(
        event_id,
        fight_records,
    )
    active_api_fight_ids: list[int] = []

    for fight_order, fight_record in enumerate(
        sort_event_fights(fight_records),
        start=1,
    ):
        api_fight_id = int(fight_record["id"])
        active_api_fight_ids.append(api_fight_id)
        fighter_one_data = get_fighter_data(fight_record, "first")
        fighter_two_data = get_fighter_data(fight_record, "second")
        fighter_one = get_fighter_name(fight_record, "first")
        fighter_two = get_fighter_name(fight_record, "second")
        scheduled_datetime = parse_api_datetime(fight_record)
        weight_class = str(
            fight_record.get("category") or "Unlisted weight class"
        ).strip()
        api_status = get_api_status(fight_record)

        if scheduled_datetime is None:
            continue

        with create_database_connection() as database_connection:
            automatic_fight_row = database_connection.execute(
                """
                SELECT
                    fight_id,
                    fighter_one_api_id,
                    fighter_two_api_id
                FROM automatic_fights
                WHERE api_fight_id = ?
                """,
                (api_fight_id,),
            ).fetchone()

        if automatic_fight_row is None:
            fight_id = add_fight(
                event_id=event_id,
                fighter_one=fighter_one,
                fighter_two=fighter_two,
            )

            with create_database_connection() as database_connection:
                database_connection.execute(
                    """
                    INSERT INTO automatic_fights (
                        api_fight_id,
                        fight_id,
                        scheduled_time,
                        weight_class,
                        fighter_one_api_id,
                        fighter_two_api_id,
                        last_api_status,
                        is_main
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        api_fight_id,
                        fight_id,
                        scheduled_datetime.isoformat(),
                        weight_class,
                        fighter_one_data.get("id"),
                        fighter_two_data.get("id"),
                        api_status,
                        1 if fight_record.get("is_main") else 0,
                    ),
                )
        else:
            fight_id = int(automatic_fight_row["fight_id"])

        with create_database_connection() as database_connection:
            existing_fight_row = database_connection.execute(
                """
                SELECT fighter_one, fighter_two
                FROM fights
                WHERE fight_id = ?
                """,
                (fight_id,),
            ).fetchone()

            if existing_fight_row is not None:
                previous_fighter_one = str(
                    existing_fight_row["fighter_one"]
                )
                previous_fighter_two = str(
                    existing_fight_row["fighter_two"]
                )
                previous_fighter_one_api_id = (
                    automatic_fight_row["fighter_one_api_id"]
                    if automatic_fight_row is not None
                    else None
                )
                previous_fighter_two_api_id = (
                    automatic_fight_row["fighter_two_api_id"]
                    if automatic_fight_row is not None
                    else None
                )
                current_fighter_one_api_id = fighter_one_data.get("id")
                current_fighter_two_api_id = fighter_two_data.get("id")

                if previous_fighter_one != fighter_one:
                    if (
                        previous_fighter_one_api_id is not None
                        and previous_fighter_one_api_id
                        == current_fighter_one_api_id
                    ):
                        database_connection.execute(
                            """
                            UPDATE picks
                            SET predicted_winner = ?
                            WHERE fight_id = ?
                                AND predicted_winner = ?
                            """,
                            (
                                fighter_one,
                                fight_id,
                                previous_fighter_one,
                            ),
                        )
                    else:
                        invalidate_picks_for_replaced_fighter(
                            event_id,
                            fight_id,
                            previous_fighter_one,
                            fighter_one,
                        )

                if previous_fighter_two != fighter_two:
                    if (
                        previous_fighter_two_api_id is not None
                        and previous_fighter_two_api_id
                        == current_fighter_two_api_id
                    ):
                        database_connection.execute(
                            """
                            UPDATE picks
                            SET predicted_winner = ?
                            WHERE fight_id = ?
                                AND predicted_winner = ?
                            """,
                            (
                                fighter_two,
                                fight_id,
                                previous_fighter_two,
                            ),
                        )
                    else:
                        invalidate_picks_for_replaced_fighter(
                            event_id,
                            fight_id,
                            previous_fighter_two,
                            fighter_two,
                        )

            fight_status = (
                "canceled"
                if api_status == "CANC"
                else "completed"
                if api_status == "FT"
                else "upcoming"
            )
            database_connection.execute(
                """
                UPDATE fights
                SET
                    fighter_one = ?,
                    fighter_two = ?,
                    fight_order = ?,
                    status = ?
                WHERE fight_id = ?
                """,
                (
                    fighter_one,
                    fighter_two,
                    fight_order,
                    fight_status,
                    fight_id,
                ),
            )
            database_connection.execute(
                """
                UPDATE automatic_fights
                SET
                    scheduled_time = ?,
                    weight_class = ?,
                    fighter_one_api_id = ?,
                    fighter_two_api_id = ?,
                    last_api_status = ?,
                    is_main = ?
                WHERE api_fight_id = ?
                """,
                (
                    scheduled_datetime.isoformat(),
                    weight_class,
                    fighter_one_data.get("id"),
                    fighter_two_data.get("id"),
                    api_status,
                    1 if fight_record.get("is_main") else 0,
                    api_fight_id,
                ),
            )

        if api_status == "CANC":
            invalidate_picks_for_canceled_fight(
                event_id,
                fight_id,
                f"{fighter_one} vs. {fighter_two} was canceled",
            )

        if api_status == "FT":
            save_automatic_result(fight_id, fight_record)

    removed_fight_ids: list[int] = []

    with create_database_connection() as database_connection:
        existing_automatic_fights = database_connection.execute(
            """
            SELECT
                automatic_fights.api_fight_id,
                automatic_fights.fight_id
            FROM automatic_fights
            JOIN fights
                ON fights.fight_id = automatic_fights.fight_id
            WHERE fights.event_id = ?
            """,
            (event_id,),
        ).fetchall()

        for existing_fight in existing_automatic_fights:
            if int(existing_fight["api_fight_id"]) not in active_api_fight_ids:
                missing_fight_id = int(existing_fight["fight_id"])
                database_connection.execute(
                    """
                    UPDATE fights
                    SET status = 'canceled'
                    WHERE fight_id = ?
                    """,
                    (missing_fight_id,),
                )
                removed_fight_ids.append(missing_fight_id)

    for removed_fight_id in removed_fight_ids:
        invalidate_picks_for_canceled_fight(
            event_id,
            removed_fight_id,
            "The fight was removed from the UFC card",
        )

    update_event_status(event_id)
    return event_id, changes


def save_guild_channel(guild_id: int, channel_id: int) -> None:
    with create_database_connection() as database_connection:
        database_connection.execute(
            """
            INSERT INTO guild_settings (guild_id, ufc_channel_id)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                ufc_channel_id = excluded.ufc_channel_id
            """,
            (guild_id, channel_id),
        )


def set_ufc_sync_status(status_key: str, status_value: str) -> None:
    with create_database_connection() as database_connection:
        database_connection.execute(
            """
            INSERT INTO ufc_sync_status (status_key, status_value)
            VALUES (?, ?)
            ON CONFLICT(status_key) DO UPDATE SET
                status_value = excluded.status_value
            """,
            (status_key, status_value),
        )


def get_ufc_sync_status() -> dict[str, str]:
    with create_database_connection() as database_connection:
        status_rows = database_connection.execute(
            """
            SELECT status_key, status_value
            FROM ufc_sync_status
            """
        ).fetchall()

    return {
        str(status_row["status_key"]): str(status_row["status_value"])
        for status_row in status_rows
    }


def set_reminders_enabled(guild_id: int, enabled: bool) -> None:
    with create_database_connection() as database_connection:
        database_connection.execute(
            """
            UPDATE guild_settings
            SET reminders_enabled = ?
            WHERE guild_id = ?
            """,
            (1 if enabled else 0, guild_id),
        )


def get_guild_settings() -> list:
    with create_database_connection() as database_connection:
        return database_connection.execute(
            """
            SELECT
                guild_id,
                ufc_channel_id,
                reminders_enabled
            FROM guild_settings
            """
        ).fetchall()


def event_has_been_posted(guild_id: int, event_id: int) -> bool:
    with create_database_connection() as database_connection:
        event_post = database_connection.execute(
            """
            SELECT 1
            FROM event_posts
            WHERE guild_id = ?
                AND event_id = ?
            """,
            (guild_id, event_id),
        ).fetchone()

    return event_post is not None


def save_event_post(
    guild_id: int,
    event_id: int,
    channel_id: int,
    header_message_id: int,
) -> None:
    with create_database_connection() as database_connection:
        database_connection.execute(
            """
            INSERT OR REPLACE INTO event_posts (
                guild_id,
                event_id,
                channel_id,
                header_message_id
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                guild_id,
                event_id,
                channel_id,
                header_message_id,
            ),
        )


def replace_fight_post_mapping(
    guild_id: int,
    event_id: int,
    channel_id: int,
    section_messages: list[tuple[int, int, list[int]]],
) -> None:
    with create_database_connection() as database_connection:
        database_connection.execute(
            """
            DELETE FROM fight_posts
            WHERE guild_id = ?
                AND fight_id IN (
                    SELECT fight_id
                    FROM fights
                    WHERE event_id = ?
                )
            """,
            (guild_id, event_id),
        )

        for message_id, section_number, fight_ids in section_messages:
            database_connection.executemany(
                """
                INSERT OR REPLACE INTO fight_posts (
                    guild_id,
                    fight_id,
                    channel_id,
                    message_id,
                    section_number
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        guild_id,
                        fight_id,
                        channel_id,
                        message_id,
                        section_number,
                    )
                    for fight_id in fight_ids
                ],
            )


def get_pick_counts(fight_id: int) -> dict[str, int]:
    with create_database_connection() as database_connection:
        pick_rows = database_connection.execute(
            """
            SELECT predicted_winner, COUNT(*) AS vote_count
            FROM picks
            WHERE fight_id = ?
            GROUP BY predicted_winner
            """,
            (fight_id,),
        ).fetchall()

    return {
        str(pick_row["predicted_winner"]): int(pick_row["vote_count"])
        for pick_row in pick_rows
    }


def get_weight_class(fight_id: int) -> str:
    with create_database_connection() as database_connection:
        automatic_fight_row = database_connection.execute(
            """
            SELECT weight_class
            FROM automatic_fights
            WHERE fight_id = ?
            """,
            (fight_id,),
        ).fetchone()

    if automatic_fight_row is None:
        return "Unlisted weight class"

    return str(
        automatic_fight_row["weight_class"]
        or "Unlisted weight class"
    )


def decimal_odds_from_value(value: object) -> float | None:
    try:
        numeric_value = float(str(value).strip())
    except (TypeError, ValueError):
        return None

    if numeric_value > 1.0 and numeric_value < 50.0:
        return numeric_value

    if numeric_value >= 100.0:
        return 1.0 + (numeric_value / 100.0)

    if numeric_value <= -100.0:
        return 1.0 + (100.0 / abs(numeric_value))

    return None


def american_odds_from_decimal(decimal_odds: float) -> int:
    if decimal_odds >= 2.0:
        return int(round((decimal_odds - 1.0) * 100.0))

    return -int(round(100.0 / (decimal_odds - 1.0)))


def format_american_odds(american_odds: int | None) -> str:
    if american_odds is None:
        return ""

    return (
        f"+{american_odds}"
        if american_odds > 0
        else str(american_odds)
    )


def get_odds_row_for_fight(fight_id: int):
    with create_database_connection() as database_connection:
        return database_connection.execute(
            """
            SELECT
                fighter_one_american,
                fighter_two_american,
                fighter_one_probability,
                fighter_two_probability,
                bookmaker_count,
                source_updated_at,
                fetched_at
            FROM fight_odds
            WHERE fight_id = ?
            """,
            (fight_id,),
        ).fetchone()


def get_api_fight_id_from_odds_row(odds_row: dict) -> int | None:
    possible_containers = (
        odds_row.get("fight"),
        odds_row.get("fixture"),
        odds_row.get("game"),
    )

    for possible_container in possible_containers:
        possible_id = (
            possible_container.get("id")
            if isinstance(possible_container, dict)
            else possible_container
        )

        if possible_id is not None:
            try:
                return int(possible_id)
            except (TypeError, ValueError):
                pass

    for possible_key in ("fight_id", "fixture_id"):
        possible_id = odds_row.get(possible_key)

        if possible_id is not None:
            try:
                return int(possible_id)
            except (TypeError, ValueError):
                pass

    return None


def select_winner_bet(bookmaker_row: dict) -> dict | None:
    bet_rows = bookmaker_row.get("bets") or []

    if not isinstance(bet_rows, list):
        return None

    for bet_row in bet_rows:
        if not isinstance(bet_row, dict):
            continue

        try:
            bet_id = int(bet_row.get("id"))
        except (TypeError, ValueError):
            bet_id = None

        if bet_id == 2:
            return bet_row

    preferred_names = (
        "winner",
        "fight winner",
        "match winner",
        "moneyline",
        "money line",
        "home/away",
        "home away",
    )

    for bet_row in bet_rows:
        if not isinstance(bet_row, dict):
            continue

        bet_name = str(bet_row.get("name") or "").strip().casefold()

        if bet_name in preferred_names:
            return bet_row

    for bet_row in bet_rows:
        if not isinstance(bet_row, dict):
            continue

        bet_name = str(bet_row.get("name") or "").strip().casefold()

        if "winner" in bet_name and "method" not in bet_name:
            return bet_row

    return None


def match_odds_values_to_fighters(
    value_rows: list[dict],
    fighter_one: str,
    fighter_two: str,
) -> tuple[float, float] | None:
    matched_decimal_odds: dict[int, float] = {}

    for value_row in value_rows:
        value_name = str(
            value_row.get("value")
            or value_row.get("name")
            or ""
        ).strip()
        decimal_odds = decimal_odds_from_value(
            value_row.get("odd")
            or value_row.get("odds")
            or value_row.get("price")
        )

        if decimal_odds is None:
            continue

        lowered_value_name = value_name.casefold()

        if lowered_value_name == fighter_one.casefold():
            matched_decimal_odds[1] = decimal_odds
        elif lowered_value_name == fighter_two.casefold():
            matched_decimal_odds[2] = decimal_odds
        elif lowered_value_name in {"1", "home", "first"}:
            matched_decimal_odds[1] = decimal_odds
        elif lowered_value_name in {"2", "away", "second"}:
            matched_decimal_odds[2] = decimal_odds

    if 1 in matched_decimal_odds and 2 in matched_decimal_odds:
        return matched_decimal_odds[1], matched_decimal_odds[2]

    valid_value_rows = [
        value_row
        for value_row in value_rows
        if decimal_odds_from_value(
            value_row.get("odd")
            or value_row.get("odds")
            or value_row.get("price")
        )
        is not None
    ]

    if len(valid_value_rows) != 2:
        return None

    first_decimal_odds = decimal_odds_from_value(
        valid_value_rows[0].get("odd")
        or valid_value_rows[0].get("odds")
        or valid_value_rows[0].get("price")
    )
    second_decimal_odds = decimal_odds_from_value(
        valid_value_rows[1].get("odd")
        or valid_value_rows[1].get("odds")
        or valid_value_rows[1].get("price")
    )

    if first_decimal_odds is None or second_decimal_odds is None:
        return None

    return first_decimal_odds, second_decimal_odds


def calculate_consensus_odds(
    odds_row: dict,
    fighter_one: str,
    fighter_two: str,
) -> dict | None:
    bookmaker_rows = odds_row.get("bookmakers") or []

    if not isinstance(bookmaker_rows, list):
        return None

    fighter_one_decimal_values: list[float] = []
    fighter_two_decimal_values: list[float] = []

    for bookmaker_row in bookmaker_rows:
        if not isinstance(bookmaker_row, dict):
            continue

        winner_bet = select_winner_bet(bookmaker_row)

        if winner_bet is None:
            continue

        value_rows = winner_bet.get("values") or []

        if not isinstance(value_rows, list):
            continue

        matched_odds = match_odds_values_to_fighters(
            [
                value_row
                for value_row in value_rows
                if isinstance(value_row, dict)
            ],
            fighter_one,
            fighter_two,
        )

        if matched_odds is None:
            continue

        fighter_one_decimal_values.append(matched_odds[0])
        fighter_two_decimal_values.append(matched_odds[1])

    if not fighter_one_decimal_values or not fighter_two_decimal_values:
        return None

    fighter_one_decimal = statistics.median(
        fighter_one_decimal_values
    )
    fighter_two_decimal = statistics.median(
        fighter_two_decimal_values
    )
    fighter_one_raw_probability = 1.0 / fighter_one_decimal
    fighter_two_raw_probability = 1.0 / fighter_two_decimal
    probability_total = (
        fighter_one_raw_probability + fighter_two_raw_probability
    )

    return {
        "fighter_one_american": american_odds_from_decimal(
            fighter_one_decimal
        ),
        "fighter_two_american": american_odds_from_decimal(
            fighter_two_decimal
        ),
        "fighter_one_probability": (
            fighter_one_raw_probability / probability_total
        ),
        "fighter_two_probability": (
            fighter_two_raw_probability / probability_total
        ),
        "bookmaker_count": min(
            len(fighter_one_decimal_values),
            len(fighter_two_decimal_values),
        ),
        "source_updated_at": (
            odds_row.get("update")
            or odds_row.get("updated")
            or odds_row.get("updated_at")
        ),
    }


def save_consensus_odds(
    fight_id: int,
    consensus_odds: dict,
) -> None:
    recorded_at = datetime.now(timezone.utc).isoformat()

    with create_database_connection() as database_connection:
        previous_odds_row = database_connection.execute(
            """
            SELECT
                fighter_one_american,
                fighter_two_american,
                bookmaker_count
            FROM fight_odds
            WHERE fight_id = ?
            """,
            (fight_id,),
        ).fetchone()
        odds_changed = (
            previous_odds_row is None
            or int(previous_odds_row["fighter_one_american"])
            != int(consensus_odds["fighter_one_american"])
            or int(previous_odds_row["fighter_two_american"])
            != int(consensus_odds["fighter_two_american"])
            or int(previous_odds_row["bookmaker_count"])
            != int(consensus_odds["bookmaker_count"])
        )

        database_connection.execute(
            """
            INSERT INTO fight_odds (
                fight_id,
                fighter_one_american,
                fighter_two_american,
                fighter_one_probability,
                fighter_two_probability,
                bookmaker_count,
                source_updated_at,
                fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fight_id) DO UPDATE SET
                fighter_one_american =
                    excluded.fighter_one_american,
                fighter_two_american =
                    excluded.fighter_two_american,
                fighter_one_probability =
                    excluded.fighter_one_probability,
                fighter_two_probability =
                    excluded.fighter_two_probability,
                bookmaker_count = excluded.bookmaker_count,
                source_updated_at = excluded.source_updated_at,
                fetched_at = excluded.fetched_at
            """,
            (
                fight_id,
                consensus_odds["fighter_one_american"],
                consensus_odds["fighter_two_american"],
                consensus_odds["fighter_one_probability"],
                consensus_odds["fighter_two_probability"],
                consensus_odds["bookmaker_count"],
                consensus_odds["source_updated_at"],
                recorded_at,
            ),
        )

        if odds_changed:
            database_connection.execute(
                """
                INSERT INTO fight_odds_history (
                    fight_id,
                    fighter_one_american,
                    fighter_two_american,
                    fighter_one_probability,
                    fighter_two_probability,
                    bookmaker_count,
                    source_updated_at,
                    recorded_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fight_id,
                    consensus_odds["fighter_one_american"],
                    consensus_odds["fighter_two_american"],
                    consensus_odds["fighter_one_probability"],
                    consensus_odds["fighter_two_probability"],
                    consensus_odds["bookmaker_count"],
                    consensus_odds["source_updated_at"],
                    recorded_at,
                ),
            )


async def synchronize_event_odds(
    event_id: int,
    api_event: dict,
) -> OddsSyncSummary:
    event_start_time: datetime = api_event["event_start_time"]
    now = datetime.now(timezone.utc)
    odds_sync_summary = OddsSyncSummary()

    if event_start_time < now or event_start_time > (
        now + timedelta(days=POSTING_WINDOW_DAYS)
    ):
        return odds_sync_summary

    odds_rows: list[dict] = []
    fight_dates = {
        parsed_datetime.date()
        for fight_record in api_event["fight_records"]
        if (
            parsed_datetime := parse_api_datetime(fight_record)
        )
        is not None
    }

    for fight_date in sorted(fight_dates):
        odds_rows.extend(await get_odds_for_date(fight_date))

    odds_sync_summary.date_response_count = len(odds_rows)

    with create_database_connection() as database_connection:
        fight_rows = database_connection.execute(
            """
            SELECT
                automatic_fights.api_fight_id,
                automatic_fights.fight_id,
                fights.fighter_one,
                fights.fighter_two
            FROM automatic_fights
            JOIN fights
                ON fights.fight_id = automatic_fights.fight_id
            WHERE fights.event_id = ?
                AND fights.status = 'upcoming'
            """,
            (event_id,),
        ).fetchall()

    odds_rows_by_api_fight_id: dict[int, dict] = {}

    for odds_row in odds_rows:
        if not isinstance(odds_row, dict):
            continue

        api_fight_id = get_api_fight_id_from_odds_row(odds_row)

        if api_fight_id is not None:
            odds_rows_by_api_fight_id[api_fight_id] = odds_row

    status_values = get_ufc_sync_status()
    last_direct_attempt = parse_iso_datetime(
        status_values.get("last_direct_odds_attempt_at")
    )
    direct_retry_is_due = (
        last_direct_attempt is None
        or now - last_direct_attempt >= ODDS_DIRECT_RETRY_INTERVAL
    )

    if direct_retry_is_due:
        missing_api_fight_ids = [
            int(fight_row["api_fight_id"])
            for fight_row in fight_rows
            if int(fight_row["api_fight_id"])
            not in odds_rows_by_api_fight_id
        ]

        if missing_api_fight_ids:
            set_ufc_sync_status(
                "last_direct_odds_attempt_at",
                now.isoformat(),
            )

        for api_fight_id in missing_api_fight_ids:
            direct_odds_rows = await get_odds_for_fight(
                api_fight_id
            )
            odds_sync_summary.direct_response_count += len(
                direct_odds_rows
            )

            for direct_odds_row in direct_odds_rows:
                if not isinstance(direct_odds_row, dict):
                    continue

                returned_api_fight_id = (
                    get_api_fight_id_from_odds_row(
                        direct_odds_row
                    )
                    or api_fight_id
                )
                odds_rows_by_api_fight_id[
                    returned_api_fight_id
                ] = direct_odds_row

    for fight_row in fight_rows:
        odds_row = odds_rows_by_api_fight_id.get(
            int(fight_row["api_fight_id"])
        )

        if odds_row is None:
            continue

        odds_sync_summary.matched_response_count += 1
        consensus_odds = calculate_consensus_odds(
            odds_row,
            str(fight_row["fighter_one"]),
            str(fight_row["fighter_two"]),
        )

        if consensus_odds is None:
            continue

        save_consensus_odds(
            int(fight_row["fight_id"]),
            consensus_odds,
        )
        odds_sync_summary.stored_fight_count += 1

    return odds_sync_summary


def fight_picks_are_open(fight_id: int) -> bool:
    with create_database_connection() as database_connection:
        fight_row = database_connection.execute(
            """
            SELECT
                fights.status,
                events.start_time
            FROM fights
            JOIN events
                ON events.event_id = fights.event_id
            WHERE fights.fight_id = ?
            """,
            (fight_id,),
        ).fetchone()

    if fight_row is None or fight_row["status"] != "upcoming":
        return False

    event_start_time = parse_iso_datetime(fight_row["start_time"])
    return (
        event_start_time is None
        or datetime.now(timezone.utc) < event_start_time
    )


def get_result_for_fight(fight_id: int):
    with create_database_connection() as database_connection:
        return database_connection.execute(
            """
            SELECT winner, method, ending_round
            FROM results
            WHERE fight_id = ?
            """,
            (fight_id,),
        ).fetchone()


def build_event_header_embed(
    event_id: int,
    fight_rows: list,
) -> discord.Embed:
    event_row = get_event(event_id)

    if event_row is None:
        raise ValueError(f"Event {event_id} does not exist.")

    start_time = parse_iso_datetime(event_row["start_time"])
    active_fights = [
        fight_row
        for fight_row in fight_rows
        if fight_row["status"] != "canceled"
    ]
    card_lines = [
        (
            f"**{fight_number}.** "
            f"{fight_row['fighter_one']} vs. "
            f"{fight_row['fighter_two']}"
        )
        for fight_number, fight_row in enumerate(active_fights, start=1)
    ]
    event_status = str(event_row["status"])
    color = (
        discord.Color.green()
        if event_status == "completed"
        else discord.Color.orange()
        if event_status == "active"
        else discord.Color.red()
    )
    embed = discord.Embed(
        title=str(event_row["event_name"]),
        description="\n".join(card_lines),
        color=color,
    )

    if start_time is not None:
        unix_timestamp = int(start_time.timestamp())
        embed.add_field(
            name=(
                "Event started"
                if event_status in {"active", "completed"}
                else "Event starts"
            ),
            value=f"<t:{unix_timestamp}:F>\n<t:{unix_timestamp}:R>",
            inline=False,
        )

    embed.add_field(
        name="Status",
        value=event_status.title(),
        inline=True,
    )
    embed.set_footer(
        text=(
            "Choose one fighter in each row. "
            "Picks lock when the event begins."
            if event_status == "upcoming"
            else "Picks are closed."
        )
    )
    return embed


def build_fight_section_embed(
    fight_ids: list[int],
    section_number: int,
) -> discord.Embed:
    embed = discord.Embed(
        title=f"Fight Picks · Section {section_number}",
        color=discord.Color.dark_red(),
    )

    for fight_id in fight_ids:
        fight_row = get_fight(fight_id)

        if fight_row is None:
            continue

        pick_counts = get_pick_counts(fight_id)
        fighter_one = str(fight_row["fighter_one"])
        fighter_two = str(fight_row["fighter_two"])
        fighter_one_votes = pick_counts.get(fighter_one, 0)
        fighter_two_votes = pick_counts.get(fighter_two, 0)
        odds_row = get_odds_row_for_fight(fight_id)
        result_row = get_result_for_fight(fight_id)
        status_lines: list[str] = []
        fighter_one_odds_text = ""
        fighter_two_odds_text = ""

        if odds_row is not None:
            fighter_one_odds_text = (
                " · "
                f"{format_american_odds(int(odds_row['fighter_one_american']))}"
                " · "
                f"{float(odds_row['fighter_one_probability']) * 100:.0f}%"
            )
            fighter_two_odds_text = (
                " · "
                f"{format_american_odds(int(odds_row['fighter_two_american']))}"
                " · "
                f"{float(odds_row['fighter_two_probability']) * 100:.0f}%"
            )

        if result_row is not None:
            result_text = f"**Winner: {result_row['winner']}**"

            if result_row["method"]:
                result_text += f" · {result_row['method']}"

            if result_row["ending_round"]:
                result_text += f" · Round {result_row['ending_round']}"

            status_lines.append(result_text)
        elif fight_row["status"] == "canceled":
            status_lines.append("**Canceled**")
        elif not fight_picks_are_open(fight_id):
            status_lines.append("**Picks closed**")

        embed.add_field(
            name=(
                f"{fight_row['fight_order']}. "
                f"{fighter_one} vs. {fighter_two}"
            ),
            value=(
                f"{get_weight_class(fight_id)}\n"
                f"🔵 {fighter_one}{fighter_one_odds_text}: "
                f"**{fighter_one_votes} pick(s)**\n"
                f"🔴 {fighter_two}{fighter_two_odds_text}: "
                f"**{fighter_two_votes} pick(s)**"
                + (
                    "\n" + "\n".join(status_lines)
                    if status_lines
                    else ""
                )
            ),
            inline=False,
        )

    embed.set_footer(
        text=(
            "When available, odds are consensus American moneylines. "
            "Percentages are normalized market probabilities."
        )
    )
    return embed


def build_pick_button_label(
    fight_id: int,
    fighter_number: int,
    fighter_name: str,
) -> str:
    odds_row = get_odds_row_for_fight(fight_id)

    if odds_row is None:
        return fighter_name[:80]

    column_name = (
        "fighter_one_american"
        if fighter_number == 1
        else "fighter_two_american"
    )
    odds_text = format_american_odds(int(odds_row[column_name]))
    available_name_length = max(1, 80 - len(odds_text) - 3)
    shortened_name = fighter_name[:available_name_length]
    return f"{shortened_name} ({odds_text})"


class FightPickButton(discord.ui.Button):
    def __init__(
        self,
        fight_id: int,
        fighter_number: int,
        fighter_name: str,
        row_number: int,
        disabled: bool,
    ) -> None:
        button_style = (
            discord.ButtonStyle.primary
            if fighter_number == 1
            else discord.ButtonStyle.danger
        )
        super().__init__(
            label=build_pick_button_label(
                fight_id,
                fighter_number,
                fighter_name,
            ),
            style=button_style,
            custom_id=f"ufc_pick:{fight_id}:{fighter_number}",
            row=row_number,
            disabled=disabled,
        )
        self.fight_id = fight_id
        self.fighter_name = fighter_name

    async def callback(
        self,
        interaction: discord.Interaction,
    ) -> None:
        if not fight_picks_are_open(self.fight_id):
            await interaction.response.send_message(
                "Picks are closed for this fight.",
                ephemeral=True,
            )
            return

        try:
            save_pick(
                discord_user_id=interaction.user.id,
                display_name=interaction.user.display_name,
                fight_id=self.fight_id,
                predicted_winner=self.fighter_name,
                predicted_method=None,
                predicted_round=None,
            )
        except ValueError as error:
            await interaction.response.send_message(
                str(error),
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"Your pick is **{self.fighter_name}**.",
            ephemeral=True,
        )

        if (
            interaction.message is not None
            and isinstance(self.view, FightCardView)
        ):
            try:
                await interaction.message.edit(
                    embed=build_fight_section_embed(
                        self.view.fight_ids,
                        self.view.section_number,
                    ),
                    view=FightCardView(
                        self.view.fight_ids,
                        self.view.section_number,
                    ),
                )
            except discord.HTTPException as error:
                print(f"Could not refresh UFC vote totals: {error}")


class FightCardView(discord.ui.View):
    def __init__(
        self,
        fight_ids: list[int],
        section_number: int,
    ) -> None:
        super().__init__(timeout=None)
        self.fight_ids = fight_ids
        self.section_number = section_number

        for row_number, fight_id in enumerate(fight_ids):
            fight_row = get_fight(fight_id)

            if fight_row is None:
                continue

            picks_are_open = fight_picks_are_open(fight_id)
            self.add_item(
                FightPickButton(
                    fight_id=fight_id,
                    fighter_number=1,
                    fighter_name=str(fight_row["fighter_one"]),
                    row_number=row_number,
                    disabled=not picks_are_open,
                )
            )
            self.add_item(
                FightPickButton(
                    fight_id=fight_id,
                    fighter_number=2,
                    fighter_name=str(fight_row["fighter_two"]),
                    row_number=row_number,
                    disabled=not picks_are_open,
                )
            )


def chunk_fight_ids(fight_rows: list) -> list[list[int]]:
    active_fight_ids = [
        int(fight_row["fight_id"])
        for fight_row in fight_rows
        if fight_row["status"] != "canceled"
    ]
    return [
        active_fight_ids[
            start_index:start_index + FIGHTS_PER_MESSAGE
        ]
        for start_index in range(
            0,
            len(active_fight_ids),
            FIGHTS_PER_MESSAGE,
        )
    ]


async def post_event_card(
    channel: discord.TextChannel,
    event_id: int,
) -> None:
    fight_rows = get_event_fights(event_id)
    fight_id_sections = chunk_fight_ids(fight_rows)

    if not fight_id_sections:
        raise ValueError("The upcoming UFC event has no active fights.")

    header_message = await channel.send(
        embed=build_event_header_embed(event_id, fight_rows)
    )
    save_event_post(
        guild_id=channel.guild.id,
        event_id=event_id,
        channel_id=channel.id,
        header_message_id=header_message.id,
    )
    section_messages: list[tuple[int, int, list[int]]] = []

    for section_number, fight_ids in enumerate(
        fight_id_sections,
        start=1,
    ):
        fight_view = FightCardView(
            fight_ids=fight_ids,
            section_number=section_number,
        )
        section_message = await channel.send(
            embed=build_fight_section_embed(
                fight_ids,
                section_number,
            ),
            view=fight_view,
        )
        section_messages.append(
            (section_message.id, section_number, fight_ids)
        )

    replace_fight_post_mapping(
        guild_id=channel.guild.id,
        event_id=event_id,
        channel_id=channel.id,
        section_messages=section_messages,
    )


def build_event_render_signature(event_id: int) -> str:
    fight_rows = get_event_fights(event_id)
    fight_id_sections = chunk_fight_ids(fight_rows)
    rendered_sections: list[object] = []

    for section_number, fight_ids in enumerate(
        fight_id_sections,
        start=1,
    ):
        fight_view = FightCardView(fight_ids, section_number)
        button_data = [
            (
                getattr(view_item, "custom_id", None),
                getattr(view_item, "label", None),
                bool(getattr(view_item, "disabled", False)),
                int(getattr(view_item, "style", 0)),
            )
            for view_item in fight_view.children
        ]
        rendered_sections.append(
            (
                build_fight_section_embed(
                    fight_ids,
                    section_number,
                ).to_dict(),
                button_data,
            )
        )

    render_data = (
        build_event_header_embed(event_id, fight_rows).to_dict(),
        rendered_sections,
    )
    return hashlib.sha256(
        repr(render_data).encode("utf-8")
    ).hexdigest()


async def refresh_posted_event_card(
    client: discord.Client,
    guild_id: int,
    event_id: int,
) -> None:
    with create_database_connection() as database_connection:
        event_post_row = database_connection.execute(
            """
            SELECT channel_id, header_message_id
            FROM event_posts
            WHERE guild_id = ?
                AND event_id = ?
            """,
            (guild_id, event_id),
        ).fetchone()
        existing_section_rows = database_connection.execute(
            """
            SELECT DISTINCT message_id, section_number
            FROM fight_posts
            JOIN fights
                ON fights.fight_id = fight_posts.fight_id
            WHERE fight_posts.guild_id = ?
                AND fights.event_id = ?
            ORDER BY section_number
            """,
            (guild_id, event_id),
        ).fetchall()

    if event_post_row is None:
        return

    channel = client.get_channel(int(event_post_row["channel_id"]))

    if not isinstance(channel, discord.TextChannel):
        return

    fight_rows = get_event_fights(event_id)
    fight_id_sections = chunk_fight_ids(fight_rows)
    render_signature = build_event_render_signature(event_id)
    render_key = (guild_id, event_id)
    recent_render = recent_event_render_signatures.get(render_key)
    now = datetime.now(timezone.utc)

    if (
        recent_render is not None
        and recent_render[0] == render_signature
        and now - recent_render[1] < RECENT_RENDER_SKIP_INTERVAL
    ):
        return

    refresh_succeeded = True

    try:
        header_message = await channel.fetch_message(
            int(event_post_row["header_message_id"])
        )
        await header_message.edit(
            embed=build_event_header_embed(event_id, fight_rows)
        )
    except discord.NotFound:
        refresh_succeeded = False
        print(
            f"Could not refresh the UFC event header for event {event_id}: "
            "the message was deleted."
        )
    except discord.HTTPException as error:
        refresh_succeeded = False
        print(
            f"Could not refresh the UFC event header for event {event_id}: "
            f"{error}"
        )

    section_messages: list[tuple[int, int, list[int]]] = []

    for section_index, fight_ids in enumerate(fight_id_sections):
        section_number = section_index + 1
        fight_view = FightCardView(fight_ids, section_number)

        if section_index < len(existing_section_rows):
            message_id = int(
                existing_section_rows[section_index]["message_id"]
            )

            try:
                section_message = await channel.fetch_message(message_id)
                await section_message.edit(
                    embed=build_fight_section_embed(
                        fight_ids,
                        section_number,
                    ),
                    view=fight_view,
                )
            except discord.NotFound:
                section_message = await channel.send(
                    embed=build_fight_section_embed(
                        fight_ids,
                        section_number,
                    ),
                    view=fight_view,
                )
                message_id = section_message.id
            except discord.HTTPException as error:
                refresh_succeeded = False
                print(
                    "Could not refresh a UFC fight section for event "
                    f"{event_id}: {error}"
                )
                section_messages.append(
                    (message_id, section_number, fight_ids)
                )
                continue
        else:
            section_message = await channel.send(
                embed=build_fight_section_embed(
                    fight_ids,
                    section_number,
                ),
                view=fight_view,
            )
            message_id = section_message.id

        section_messages.append(
            (message_id, section_number, fight_ids)
        )

    for unused_section_row in existing_section_rows[
        len(fight_id_sections):
    ]:
        try:
            unused_message = await channel.fetch_message(
                int(unused_section_row["message_id"])
            )
            await unused_message.delete()
        except discord.NotFound:
            pass
        except discord.HTTPException as error:
            print(f"Could not remove an obsolete UFC section: {error}")

    replace_fight_post_mapping(
        guild_id=guild_id,
        event_id=event_id,
        channel_id=channel.id,
        section_messages=section_messages,
    )

    if refresh_succeeded:
        recent_event_render_signatures[render_key] = (
            render_signature,
            now,
        )


def register_saved_views(client: discord.Client) -> None:
    with create_database_connection() as database_connection:
        posted_message_rows = database_connection.execute(
            """
            SELECT DISTINCT message_id, section_number
            FROM fight_posts
            ORDER BY message_id
            """
        ).fetchall()

        for message_row in posted_message_rows:
            message_id = int(message_row["message_id"])

            if message_id in registered_message_ids:
                continue

            fight_rows = database_connection.execute(
                """
                SELECT fight_posts.fight_id
                FROM fight_posts
                JOIN fights
                    ON fights.fight_id = fight_posts.fight_id
                WHERE message_id = ?
                ORDER BY fights.fight_order
                """,
                (message_id,),
            ).fetchall()
            fight_ids = [
                int(fight_row["fight_id"])
                for fight_row in fight_rows
            ]

            if not fight_ids:
                continue

            client.add_view(
                FightCardView(
                    fight_ids=fight_ids,
                    section_number=int(
                        message_row["section_number"]
                    ),
                ),
                message_id=message_id,
            )
            registered_message_ids.add(message_id)


def get_event_participation(event_id: int) -> list:
    with create_database_connection() as database_connection:
        return database_connection.execute(
            """
            SELECT
                users.discord_user_id,
                users.display_name,
                COUNT(DISTINCT picks.fight_id) AS picks_made,
                (
                    SELECT COUNT(*)
                    FROM fights
                    WHERE fights.event_id = ?
                        AND fights.status != 'canceled'
                ) AS total_fights
            FROM picks
            JOIN users
                ON users.discord_user_id = picks.discord_user_id
            JOIN fights
                ON fights.fight_id = picks.fight_id
            WHERE fights.event_id = ?
                AND fights.status != 'canceled'
            GROUP BY
                users.discord_user_id,
                users.display_name
            ORDER BY users.display_name
            """,
            (event_id, event_id),
        ).fetchall()


async def send_event_reminders(client: discord.Client) -> None:
    now = datetime.now(timezone.utc)

    with create_database_connection() as database_connection:
        reminder_event_rows = database_connection.execute(
            """
            SELECT
                event_posts.guild_id,
                event_posts.event_id,
                event_posts.channel_id,
                events.event_name,
                events.start_time
            FROM event_posts
            JOIN events
                ON events.event_id = event_posts.event_id
            JOIN guild_settings
                ON guild_settings.guild_id = event_posts.guild_id
            WHERE events.status = 'upcoming'
                AND guild_settings.reminders_enabled = 1
            """
        ).fetchall()

    for event_row in reminder_event_rows:
        event_start_time = parse_iso_datetime(event_row["start_time"])

        if event_start_time is None or event_start_time <= now:
            continue

        time_until_event = event_start_time - now

        for reminder_kind, reminder_window in REMINDER_WINDOWS.items():
            if time_until_event > reminder_window:
                continue

            if (
                reminder_kind == "24-hour"
                and time_until_event <= REMINDER_WINDOWS["2-hour"]
            ):
                continue

            guild_id = int(event_row["guild_id"])
            event_id = int(event_row["event_id"])

            with create_database_connection() as database_connection:
                existing_reminder = database_connection.execute(
                    """
                    SELECT 1
                    FROM event_reminders
                    WHERE guild_id = ?
                        AND event_id = ?
                        AND reminder_kind = ?
                    """,
                    (guild_id, event_id, reminder_kind),
                ).fetchone()

            if existing_reminder is not None:
                continue

            channel = client.get_channel(int(event_row["channel_id"]))

            if not isinstance(channel, discord.TextChannel):
                continue

            participation_rows = get_event_participation(event_id)
            incomplete_mentions = []

            for participation_row in participation_rows:
                missing_picks = (
                    int(participation_row["total_fights"])
                    - int(participation_row["picks_made"])
                )

                if missing_picks > 0:
                    incomplete_mentions.append(
                        f"<@{participation_row['discord_user_id']}> "
                        f"({missing_picks} left)"
                    )

            unix_timestamp = int(event_start_time.timestamp())
            reminder_message = (
                f"⏰ **{event_row['event_name']} pick reminder**\n"
                f"Picks lock <t:{unix_timestamp}:R>."
            )

            if incomplete_mentions:
                reminder_message += (
                    "\nStill incomplete: "
                    + ", ".join(incomplete_mentions[:20])
                )
            elif participation_rows:
                reminder_message += "\nEveryone who started has finished."
            else:
                reminder_message += "\nNo picks have been submitted yet."

            await channel.send(
                reminder_message,
                allowed_mentions=discord.AllowedMentions(
                    users=True,
                    roles=False,
                    everyone=False,
                ),
            )

            with create_database_connection() as database_connection:
                database_connection.execute(
                    """
                    INSERT OR IGNORE INTO event_reminders (
                        guild_id,
                        event_id,
                        reminder_kind
                    )
                    VALUES (?, ?, ?)
                    """,
                    (guild_id, event_id, reminder_kind),
                )


def get_scored_pick_rows(
    season: int | None = None,
    event_id: int | None = None,
) -> list:
    conditions = []
    parameters: list[object] = []

    if season is not None:
        conditions.append(
            "substr(events.start_time, 1, 4) = ?"
        )
        parameters.append(str(season))

    if event_id is not None:
        conditions.append("events.event_id = ?")
        parameters.append(event_id)

    where_clause = (
        "WHERE " + " AND ".join(conditions)
        if conditions
        else ""
    )

    with create_database_connection() as database_connection:
        return database_connection.execute(
            f"""
            SELECT
                users.discord_user_id,
                users.display_name,
                events.event_id,
                events.event_name,
                events.start_time,
                events.status AS event_status,
                fights.fight_order,
                picks.predicted_winner,
                picks.predicted_method,
                picks.predicted_round,
                results.winner AS official_winner,
                results.method AS official_method,
                results.ending_round AS official_round
            FROM picks
            JOIN users
                ON users.discord_user_id = picks.discord_user_id
            JOIN fights
                ON fights.fight_id = picks.fight_id
            JOIN events
                ON events.event_id = fights.event_id
            JOIN results
                ON results.fight_id = picks.fight_id
            {where_clause}
            ORDER BY
                events.start_time,
                fights.fight_order,
                users.display_name
            """,
            parameters,
        ).fetchall()


def build_leaderboard(
    season: int | None = None,
    event_id: int | None = None,
) -> list[LeaderboardStanding]:
    scored_pick_rows = get_scored_pick_rows(
        season=season,
        event_id=event_id,
    )
    standings_by_user: dict[int, LeaderboardStanding] = {}
    results_by_user: dict[int, list[bool]] = defaultdict(list)
    event_points: dict[int, dict[int, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    for scored_pick_row in scored_pick_rows:
        discord_user_id = int(
            scored_pick_row["discord_user_id"]
        )
        standing = standings_by_user.setdefault(
            discord_user_id,
            LeaderboardStanding(
                discord_user_id=discord_user_id,
                display_name=str(scored_pick_row["display_name"]),
            ),
        )
        pick_score = calculate_pick_score(
            predicted_winner=scored_pick_row["predicted_winner"],
            predicted_method=scored_pick_row["predicted_method"],
            predicted_round=scored_pick_row["predicted_round"],
            official_winner=scored_pick_row["official_winner"],
            official_method=scored_pick_row["official_method"],
            official_round=scored_pick_row["official_round"],
        )
        winner_is_correct = pick_score.winner_points > 0
        standing.points += pick_score.total_points
        standing.correct_winners += 1 if winner_is_correct else 0
        standing.scored_picks += 1
        results_by_user[discord_user_id].append(winner_is_correct)
        if scored_pick_row["event_status"] == "completed":
            event_points[int(scored_pick_row["event_id"])][
                discord_user_id
            ] += pick_score.total_points

    for discord_user_id, result_sequence in results_by_user.items():
        best_streak = 0
        running_streak = 0

        for winner_is_correct in result_sequence:
            if winner_is_correct:
                running_streak += 1
                best_streak = max(best_streak, running_streak)
            else:
                running_streak = 0

        standings_by_user[discord_user_id].current_streak = (
            running_streak
        )
        standings_by_user[discord_user_id].best_streak = best_streak

    for points_by_user in event_points.values():
        if not points_by_user:
            continue

        winning_score = max(points_by_user.values())

        for discord_user_id, points in points_by_user.items():
            if points == winning_score:
                standings_by_user[
                    discord_user_id
                ].event_wins += 1

    return sorted(
        standings_by_user.values(),
        key=lambda standing: (
            -standing.points,
            -standing.accuracy,
            -standing.correct_winners,
            standing.display_name.casefold(),
        ),
    )


def build_leaderboard_embed(
    season: int | None = None,
    event_id: int | None = None,
) -> discord.Embed:
    standings = build_leaderboard(
        season=season,
        event_id=event_id,
    )
    title = (
        f"{season} UFC Pick'em Leaderboard"
        if season is not None
        else "All-Time UFC Pick'em Leaderboard"
    )

    if event_id is not None:
        event_row = get_event(event_id)
        title = (
            f"{event_row['event_name']} Standings"
            if event_row is not None
            else f"Event {event_id} Standings"
        )

    embed = discord.Embed(
        title=title,
        color=discord.Color.gold(),
    )

    if not standings:
        embed.description = "No scored picks are available yet."
        return embed

    standing_lines = []

    for position, standing in enumerate(standings[:20], start=1):
        medal = (
            "🥇"
            if position == 1
            else "🥈"
            if position == 2
            else "🥉"
            if position == 3
            else f"**{position}.**"
        )
        standing_lines.append(
            f"{medal} **{standing.display_name}** — "
            f"{standing.points} pts · "
            f"{standing.correct_winners}/"
            f"{standing.scored_picks} "
            f"({standing.accuracy:.0f}%) · "
            f"streak {standing.current_streak} · "
            f"{standing.event_wins} event win"
            f"{'' if standing.event_wins == 1 else 's'}"
        )

    embed.description = "\n".join(standing_lines)
    embed.set_footer(
        text=(
            "1 point for the winner; method and round add "
            "1 point each when those picks and results are available."
        )
    )
    return embed


def get_latest_event_id(
    completed_only: bool = False,
) -> int | None:
    where_clause = (
        "WHERE status = 'completed'"
        if completed_only
        else ""
    )

    with create_database_connection() as database_connection:
        event_row = database_connection.execute(
            f"""
            SELECT event_id
            FROM events
            {where_clause}
            ORDER BY
                CASE WHEN start_time IS NULL THEN 1 ELSE 0 END,
                start_time DESC,
                event_id DESC
            LIMIT 1
            """
        ).fetchone()

    return int(event_row["event_id"]) if event_row else None


def build_recap_embed(event_id: int) -> discord.Embed:
    event_row = get_event(event_id)

    if event_row is None:
        raise ValueError(f"Event {event_id} does not exist.")

    with create_database_connection() as database_connection:
        result_rows = database_connection.execute(
            """
            SELECT
                fights.fight_order,
                fights.fighter_one,
                fights.fighter_two,
                results.winner,
                results.method,
                results.ending_round
            FROM fights
            JOIN results
                ON results.fight_id = fights.fight_id
            WHERE fights.event_id = ?
            ORDER BY fights.fight_order
            """,
            (event_id,),
        ).fetchall()
        upset_rows = database_connection.execute(
            """
            SELECT
                fights.fighter_one,
                fights.fighter_two,
                results.winner,
                SUM(
                    CASE
                        WHEN picks.predicted_winner = results.winner
                        THEN 1 ELSE 0
                    END
                ) AS winner_votes,
                SUM(
                    CASE
                        WHEN picks.predicted_winner != results.winner
                        THEN 1 ELSE 0
                    END
                ) AS other_votes
            FROM fights
            JOIN results
                ON results.fight_id = fights.fight_id
            LEFT JOIN picks
                ON picks.fight_id = fights.fight_id
            WHERE fights.event_id = ?
            GROUP BY
                fights.fight_id,
                fights.fighter_one,
                fights.fighter_two,
                results.winner
            """,
            (event_id,),
        ).fetchall()

    result_lines = []

    for result_row in result_rows:
        result_text = (
            f"**{result_row['fight_order']}. "
            f"{result_row['winner']}**"
        )

        if result_row["method"]:
            result_text += f" · {result_row['method']}"

        if result_row["ending_round"]:
            result_text += f" · R{result_row['ending_round']}"

        result_lines.append(result_text)

    embed = discord.Embed(
        title=f"{event_row['event_name']} Recap",
        description=(
            "\n".join(result_lines)
            if result_lines
            else "No official winners were supplied."
        ),
        color=discord.Color.green(),
    )
    standings = build_leaderboard(event_id=event_id)

    if standings:
        podium_lines = [
            (
                f"{position}. **{standing.display_name}** — "
                f"{standing.points} pts, "
                f"{standing.correct_winners}/"
                f"{standing.scored_picks} correct"
            )
            for position, standing in enumerate(
                standings[:3],
                start=1,
            )
        ]
        embed.add_field(
            name="Event leaders",
            value="\n".join(podium_lines),
            inline=False,
        )

    upset_candidates = [
        upset_row
        for upset_row in upset_rows
        if int(upset_row["other_votes"] or 0)
        > int(upset_row["winner_votes"] or 0)
    ]

    if upset_candidates:
        biggest_upset = max(
            upset_candidates,
            key=lambda upset_row: (
                int(upset_row["other_votes"] or 0)
                - int(upset_row["winner_votes"] or 0)
            ),
        )
        embed.add_field(
            name="Biggest pick'em upset",
            value=(
                f"**{biggest_upset['winner']}** won after receiving "
                f"{int(biggest_upset['winner_votes'] or 0)} pick(s); "
                f"the opponent received "
                f"{int(biggest_upset['other_votes'] or 0)}."
            ),
            inline=False,
        )

    return embed


async def post_completed_event_recaps(
    client: discord.Client,
) -> None:
    with create_database_connection() as database_connection:
        recap_candidate_rows = database_connection.execute(
            """
            SELECT
                event_posts.guild_id,
                event_posts.event_id,
                event_posts.channel_id
            FROM event_posts
            JOIN events
                ON events.event_id = event_posts.event_id
            LEFT JOIN event_recaps
                ON event_recaps.guild_id = event_posts.guild_id
                AND event_recaps.event_id = event_posts.event_id
            WHERE events.status = 'completed'
                AND event_recaps.event_id IS NULL
            """
        ).fetchall()

    for candidate_row in recap_candidate_rows:
        channel = client.get_channel(
            int(candidate_row["channel_id"])
        )

        if not isinstance(channel, discord.TextChannel):
            continue

        event_id = int(candidate_row["event_id"])
        recap_message = await channel.send(
            embed=build_recap_embed(event_id)
        )

        with create_database_connection() as database_connection:
            database_connection.execute(
                """
                INSERT OR IGNORE INTO event_recaps (
                    guild_id,
                    event_id,
                    channel_id,
                    message_id
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    int(candidate_row["guild_id"]),
                    event_id,
                    channel.id,
                    recap_message.id,
                ),
            )


async def post_event_changes(
    client: discord.Client,
    event_id: int,
    changes: list[str],
) -> None:
    if not changes:
        return

    change_text = "\n".join(
        f"• {change}"
        for change in changes
    )
    with create_database_connection() as database_connection:
        post_rows = database_connection.execute(
            """
            SELECT guild_id, channel_id
            FROM event_posts
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchall()

    event_row = get_event(event_id)
    event_name = (
        str(event_row["event_name"])
        if event_row is not None
        else f"Event {event_id}"
    )
    with create_database_connection() as database_connection:
        invalidation_rows = database_connection.execute(
            """
            SELECT
                invalidation_id,
                discord_user_id,
                old_pick,
                reason
            FROM pick_invalidations
            WHERE event_id = ?
                AND notified_at IS NULL
            ORDER BY invalidation_id
            """,
            (event_id,),
        ).fetchall()

    invalidation_lines = [
        (
            f"<@{invalidation_row['discord_user_id']}>: "
            f"your **{invalidation_row['old_pick']}** pick was cleared "
            f"because {invalidation_row['reason']}. Please repick."
        )
        for invalidation_row in invalidation_rows
    ]
    change_hash = hashlib.sha256(
        (
            change_text
            + "\n"
            + "\n".join(
                str(invalidation_row["invalidation_id"])
                for invalidation_row in invalidation_rows
            )
        ).encode("utf-8")
    ).hexdigest()
    sent_change_post = False

    for post_row in post_rows:
        guild_id = int(post_row["guild_id"])

        with create_database_connection() as database_connection:
            existing_post = database_connection.execute(
                """
                SELECT 1
                FROM event_change_posts
                WHERE guild_id = ?
                    AND event_id = ?
                    AND change_hash = ?
                """,
                (guild_id, event_id, change_hash),
            ).fetchone()

        if existing_post is not None:
            continue

        channel = client.get_channel(int(post_row["channel_id"]))

        if not isinstance(channel, discord.TextChannel):
            continue

        await channel.send(
            (
                f"⚠️ **{event_name} card update**\n{change_text}"
                + (
                    "\n\n" + "\n".join(invalidation_lines[:20])
                    if invalidation_lines
                    else ""
                )
            ),
            allowed_mentions=discord.AllowedMentions(
                users=True,
                roles=False,
                everyone=False,
            ),
        )
        sent_change_post = True

        with create_database_connection() as database_connection:
            database_connection.execute(
                """
                INSERT OR IGNORE INTO event_change_posts (
                    guild_id,
                    event_id,
                    change_hash
                )
                VALUES (?, ?, ?)
                """,
                (guild_id, event_id, change_hash),
            )

    if sent_change_post and invalidation_rows:
        with create_database_connection() as database_connection:
            database_connection.executemany(
                """
                UPDATE pick_invalidations
                SET notified_at = CURRENT_TIMESTAMP
                WHERE invalidation_id = ?
                """,
                [
                    (int(invalidation_row["invalidation_id"]),)
                    for invalidation_row in invalidation_rows
                ],
            )


async def lock_event_at_start(
    client: discord.Client,
    event_id: int,
    event_start_time: datetime,
) -> None:
    wait_seconds = max(
        0,
        (event_start_time - datetime.now(timezone.utc)).total_seconds(),
    )

    try:
        await asyncio.sleep(wait_seconds)
        update_event_status(event_id)

        with create_database_connection() as database_connection:
            post_rows = database_connection.execute(
                """
                SELECT guild_id
                FROM event_posts
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchall()

        for post_row in post_rows:
            await refresh_posted_event_card(
                client,
                int(post_row["guild_id"]),
                event_id,
            )
    except asyncio.CancelledError:
        raise
    except Exception as error:
        print(f"Could not lock UFC event {event_id}: {error}")
    finally:
        if event_lock_tasks.get(event_id) is asyncio.current_task():
            event_lock_tasks.pop(event_id, None)
            event_lock_timestamps.pop(event_id, None)


def schedule_event_lock(
    client: discord.Client,
    event_id: int,
) -> None:
    event_row = get_event(event_id)

    if event_row is None or event_row["status"] != "upcoming":
        existing_task = event_lock_tasks.pop(event_id, None)
        event_lock_timestamps.pop(event_id, None)

        if existing_task is not None and not existing_task.done():
            existing_task.cancel()

        return

    event_start_time = parse_iso_datetime(event_row["start_time"])

    if (
        event_start_time is None
        or event_start_time <= datetime.now(timezone.utc)
    ):
        existing_task = event_lock_tasks.pop(event_id, None)
        event_lock_timestamps.pop(event_id, None)

        if existing_task is not None and not existing_task.done():
            existing_task.cancel()

        update_event_status(event_id)
        return

    existing_task = event_lock_tasks.get(event_id)
    event_start_timestamp = int(event_start_time.timestamp())

    if existing_task is not None and not existing_task.done():
        if (
            event_lock_timestamps.get(event_id)
            == event_start_timestamp
        ):
            return

        existing_task.cancel()

    event_lock_tasks[event_id] = asyncio.create_task(
        lock_event_at_start(client, event_id, event_start_time)
    )
    event_lock_timestamps[event_id] = event_start_timestamp


async def _synchronize_all_ufc_data(
    client: discord.Client,
    force_post: bool = False,
    only_guild_id: int | None = None,
) -> tuple[int, str]:
    set_ufc_sync_status(
        "last_attempt_at",
        datetime.now(timezone.utc).isoformat(),
    )
    current_year = datetime.now(timezone.utc).year
    seasons = [current_year - 1, current_year, current_year + 1]
    all_fight_records = await get_ufc_fights_for_seasons(seasons)
    fights_by_event = group_fights_by_event(all_fight_records)
    known_events = get_known_automatic_events()
    synchronized_event_ids: set[int] = set()

    for event_name in known_events:
        event_fights = fights_by_event.get(event_name)

        if not event_fights:
            continue

        api_event = build_api_event(event_name, event_fights)

        if api_event is None:
            continue

        event_id, changes = synchronize_event_with_database(api_event)
        synchronized_event_ids.add(event_id)
        schedule_event_lock(client, event_id)
        await post_event_changes(client, event_id, changes)

    next_api_event = find_next_api_event(all_fight_records)

    if next_api_event is None:
        raise RuntimeError("No upcoming UFC event was found.")

    next_event_id, changes = synchronize_event_with_database(
        next_api_event
    )
    synchronized_event_ids.add(next_event_id)
    next_event_name = str(next_api_event["event_name"])
    next_event_start_time: datetime = next_api_event[
        "event_start_time"
    ]
    schedule_event_lock(client, next_event_id)
    await post_event_changes(client, next_event_id, changes)
    try:
        odds_sync_summary = await synchronize_event_odds(
            next_event_id,
            next_api_event,
        )
        set_ufc_sync_status(
            "last_odds_result",
            (
                "Date response: "
                f"{odds_sync_summary.date_response_count}; "
                "direct response: "
                f"{odds_sync_summary.direct_response_count}; "
                "matched: "
                f"{odds_sync_summary.matched_response_count}; "
                "stored: "
                f"{odds_sync_summary.stored_fight_count} fight(s) at "
                f"{datetime.now(timezone.utc).isoformat()}"
            ),
        )
        set_ufc_sync_status(
            "last_odds_empty",
            (
                "API-Sports returned no odds for this event."
                if (
                    odds_sync_summary.date_response_count == 0
                    and odds_sync_summary.direct_response_count == 0
                )
                else ""
            ),
        )
        set_ufc_sync_status("last_odds_error", "")
    except Exception as error:
        set_ufc_sync_status("last_odds_error", str(error))
        print(f"Automatic UFC odds sync failed: {error}")

    posting_deadline = datetime.now(timezone.utc) + timedelta(
        days=POSTING_WINDOW_DAYS
    )

    for setting_row in get_guild_settings():
        guild_id = int(setting_row["guild_id"])

        if only_guild_id is not None and guild_id != only_guild_id:
            continue

        if event_has_been_posted(guild_id, next_event_id):
            continue

        if (
            not force_post
            and next_event_start_time > posting_deadline
        ):
            continue

        channel = client.get_channel(
            int(setting_row["ufc_channel_id"])
        )

        if not isinstance(channel, discord.TextChannel):
            print(
                f"Could not find UFC channel for server {guild_id}."
            )
            continue

        await post_event_card(channel, next_event_id)
        print(f"Posted automatic UFC card: {next_event_name}")

    with create_database_connection() as database_connection:
        posted_event_rows = database_connection.execute(
            """
            SELECT DISTINCT guild_id, event_id
            FROM event_posts
            """
        ).fetchall()

    for posted_event_row in posted_event_rows:
        event_id = int(posted_event_row["event_id"])

        if event_id not in synchronized_event_ids:
            update_event_status(event_id)

        await refresh_posted_event_card(
            client,
            int(posted_event_row["guild_id"]),
            event_id,
        )

    await send_event_reminders(client)
    await post_completed_event_recaps(client)
    set_ufc_sync_status(
        "last_success_at",
        datetime.now(timezone.utc).isoformat(),
    )
    set_ufc_sync_status("last_sync_error", "")
    return next_event_id, next_event_name


async def synchronize_all_ufc_data(
    client: discord.Client,
    force_post: bool = False,
    only_guild_id: int | None = None,
) -> tuple[int, str]:
    async with ufc_synchronization_lock:
        return await _synchronize_all_ufc_data(
            client=client,
            force_post=force_post,
            only_guild_id=only_guild_id,
        )


async def automatic_sync_loop(client: discord.Client) -> None:
    await client.wait_until_ready()

    while not client.is_closed():
        try:
            await synchronize_all_ufc_data(client)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            set_ufc_sync_status("last_sync_error", str(error))
            print(f"Automatic UFC sync failed: {error}")

        await asyncio.sleep(AUTOMATIC_SYNC_INTERVAL_SECONDS)


def start_ufc_background_tasks(client: discord.Client) -> None:
    global background_sync_task

    initialize_complete_tables()
    register_saved_views(client)

    with create_database_connection() as database_connection:
        upcoming_event_rows = database_connection.execute(
            """
            SELECT event_id
            FROM events
            WHERE status = 'upcoming'
            """
        ).fetchall()

    for event_row in upcoming_event_rows:
        schedule_event_lock(client, int(event_row["event_id"]))

    if background_sync_task is None or background_sync_task.done():
        background_sync_task = asyncio.create_task(
            automatic_sync_loop(client)
        )
        print(
            "Complete UFC automation is running: cards, locks, "
            "results, reminders, and recaps."
        )


def find_fighter_matches(
    fight_records: list[dict],
    fighter_name: str,
) -> tuple[str | None, list[dict]]:
    cleaned_search = fighter_name.strip().casefold()
    exact_names: set[str] = set()
    partial_names: set[str] = set()

    for fight_record in fight_records:
        for fighter_position in ("first", "second"):
            possible_name = get_fighter_name(
                fight_record,
                fighter_position,
            )
            lowered_name = possible_name.casefold()

            if lowered_name == cleaned_search:
                exact_names.add(possible_name)
            elif cleaned_search in lowered_name:
                partial_names.add(possible_name)

    matching_names = exact_names or partial_names

    if len(matching_names) != 1:
        return None, []

    canonical_name = next(iter(matching_names))
    matching_fights = [
        fight_record
        for fight_record in fight_records
        if canonical_name.casefold()
        in {
            get_fighter_name(fight_record, "first").casefold(),
            get_fighter_name(fight_record, "second").casefold(),
        }
    ]
    return canonical_name, matching_fights


def describe_fighter_history(
    canonical_name: str,
    fight_records: list[dict],
) -> discord.Embed:
    now = datetime.now(timezone.utc)
    wins = 0
    losses = 0
    draws_or_no_contests = 0
    completed_fights: list[tuple[datetime, str]] = []
    upcoming_fights: list[tuple[datetime, str]] = []
    weight_classes: set[str] = set()

    for fight_record in fight_records:
        first_name = get_fighter_name(fight_record, "first")
        second_name = get_fighter_name(fight_record, "second")
        selected_is_first = (
            first_name.casefold() == canonical_name.casefold()
        )
        selected_data = get_fighter_data(
            fight_record,
            "first" if selected_is_first else "second",
        )
        opponent_data = get_fighter_data(
            fight_record,
            "second" if selected_is_first else "first",
        )
        opponent_name = (
            second_name if selected_is_first else first_name
        )
        fight_datetime = parse_api_datetime(fight_record)
        status = get_api_status(fight_record)
        weight_class = str(
            fight_record.get("category") or ""
        ).strip()

        if weight_class:
            weight_classes.add(weight_class)

        if fight_datetime is None:
            continue

        if status == "FT":
            if selected_data.get("winner") is True:
                wins += 1
                outcome = "W"
            elif opponent_data.get("winner") is True:
                losses += 1
                outcome = "L"
            else:
                draws_or_no_contests += 1
                outcome = "D/NC"

            completed_fights.append(
                (
                    fight_datetime,
                    f"**{outcome}** vs. {opponent_name} "
                    f"— {fight_record.get('slug')}",
                )
            )
        elif (
            status != "CANC"
            and fight_datetime > now
        ):
            upcoming_fights.append(
                (
                    fight_datetime,
                    f"vs. **{opponent_name}** — "
                    f"{fight_record.get('slug')}",
                )
            )

    completed_fights.sort(key=lambda fight: fight[0], reverse=True)
    upcoming_fights.sort(key=lambda fight: fight[0])
    embed = discord.Embed(
        title=canonical_name,
        description=(
            f"API history record: **{wins}-{losses}-"
            f"{draws_or_no_contests}**\n"
            f"Weight class history: "
            f"{', '.join(sorted(weight_classes)) or 'Unknown'}"
        ),
        color=discord.Color.blue(),
    )

    if upcoming_fights:
        next_datetime, next_description = upcoming_fights[0]
        embed.add_field(
            name="Next fight",
            value=(
                f"{next_description}\n"
                f"<t:{int(next_datetime.timestamp())}:F>"
            ),
            inline=False,
        )

    if completed_fights:
        embed.add_field(
            name="Recent UFC fights",
            value="\n".join(
                fight_description
                for _, fight_description
                in completed_fights[:5]
            ),
            inline=False,
        )

    embed.set_footer(
        text=(
            "Record is calculated from the MMA API history available "
            "to MusicDude; official UFC rankings are not supplied."
        )
    )
    return embed


def describe_matchup(
    first_name: str,
    first_fights: list[dict],
    second_name: str,
    second_fights: list[dict],
) -> discord.Embed:
    first_embed = describe_fighter_history(first_name, first_fights)
    second_embed = describe_fighter_history(second_name, second_fights)

    def record_line(embed: discord.Embed) -> str:
        return str(embed.description or "").splitlines()[0]

    shared_fight_ids = {
        int(fight_record["id"])
        for fight_record in first_fights
    } & {
        int(fight_record["id"])
        for fight_record in second_fights
    }
    head_to_head_lines = []

    for fight_record in first_fights:
        if int(fight_record["id"]) not in shared_fight_ids:
            continue

        fight_datetime = parse_api_datetime(fight_record)
        winner, _, _ = extract_result(fight_record)
        head_to_head_lines.append(
            (
                fight_datetime
                or datetime.min.replace(tzinfo=timezone.utc),
                (
                    f"{fight_record.get('slug')} — "
                    f"{winner or 'No winner'}"
                ),
            )
        )

    head_to_head_lines.sort(key=lambda item: item[0], reverse=True)
    embed = discord.Embed(
        title=f"{first_name} vs. {second_name}",
        color=discord.Color.purple(),
    )
    embed.add_field(
        name=first_name,
        value=record_line(first_embed),
        inline=True,
    )
    embed.add_field(
        name=second_name,
        value=record_line(second_embed),
        inline=True,
    )
    embed.add_field(
        name="Head-to-head",
        value=(
            "\n".join(
                description
                for _, description in head_to_head_lines
            )
            if head_to_head_lines
            else "No UFC meeting found in the available API history."
        ),
        inline=False,
    )
    return embed


async def get_fighter_history_records() -> list[dict]:
    current_year = datetime.now(timezone.utc).year
    first_year = current_year - FIGHTER_HISTORY_YEARS + 1
    seasons = list(range(first_year, current_year + 2))
    return await get_ufc_fights_for_seasons(seasons)


def user_can_manage_server(
    interaction: discord.Interaction,
) -> bool:
    return interaction.permissions.manage_guild


async def send_admin_permission_error(
    interaction: discord.Interaction,
) -> None:
    await interaction.response.send_message(
        "You need the Manage Server permission to use this command.",
        ephemeral=True,
    )


def register_automatic_ufc_commands(
    command_tree: app_commands.CommandTree,
) -> None:
    initialize_complete_tables()
    ufc_group = command_tree.get_command("ufc")

    if not isinstance(ufc_group, app_commands.Group):
        raise RuntimeError(
            "The UFC command group must be registered first."
        )

    @ufc_group.command(
        name="setup",
        description="Choose the channel for automatic UFC cards.",
    )
    @app_commands.describe(
        channel="The text channel where UFC cards should be posted",
    )
    async def setup_command(
        interaction: discord.Interaction,
        channel: discord.TextChannel,
    ) -> None:
        if not user_can_manage_server(interaction):
            await send_admin_permission_error(interaction)
            return

        if interaction.guild_id is None:
            await interaction.response.send_message(
                "This command must be used inside a server.",
                ephemeral=True,
            )
            return

        save_guild_channel(interaction.guild_id, channel.id)
        await interaction.response.defer(ephemeral=True)

        try:
            event_id, event_name = await synchronize_all_ufc_data(
                client=interaction.client,
                force_post=True,
                only_guild_id=interaction.guild_id,
            )
        except Exception as error:
            await interaction.followup.send(
                (
                    f"Saved {channel.mention}, but the first UFC "
                    f"sync failed: {error}"
                ),
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            (
                f"Automatic UFC cards will post in {channel.mention}.\n"
                f"Synced **{event_name}** as event `{event_id}`."
            ),
            ephemeral=True,
        )

    @ufc_group.command(
        name="sync",
        description="Run cards, results, reminders, and recaps now.",
    )
    async def sync_command(
        interaction: discord.Interaction,
    ) -> None:
        if not user_can_manage_server(interaction):
            await send_admin_permission_error(interaction)
            return

        if interaction.guild_id is None:
            await interaction.response.send_message(
                "This command must be used inside a server.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            event_id, event_name = await synchronize_all_ufc_data(
                client=interaction.client,
                force_post=True,
                only_guild_id=interaction.guild_id,
            )
        except Exception as error:
            await interaction.followup.send(
                f"UFC sync failed: {error}",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            (
                f"Synced **{event_name}** as event `{event_id}`. "
                "Cards, results, locks, reminders, and recaps are current."
            ),
                ephemeral=True,
            )

    @ufc_group.command(
        name="status",
        description="Show UFC sync health, event time, and odds coverage.",
    )
    async def status_command(
        interaction: discord.Interaction,
    ) -> None:
        status_values = get_ufc_sync_status()
        next_event_id = get_latest_event_id(completed_only=False)
        event_row = (
            get_event(next_event_id)
            if next_event_id is not None
            else None
        )
        status_lines = [
            "Automatic checks: **every 30 minutes**",
        ]

        last_success = parse_iso_datetime(
            status_values.get("last_success_at")
        )

        if last_success is not None:
            status_lines.append(
                "Last successful sync: "
                f"<t:{int(last_success.timestamp())}:R>"
            )
        else:
            status_lines.append(
                "Last successful sync: not recorded yet"
            )

        if event_row is not None:
            event_start_time = parse_iso_datetime(
                event_row["start_time"]
            )
            status_lines.append(
                f"Tracked event: **{event_row['event_name']}**"
            )

            if event_start_time is not None:
                status_lines.append(
                    "Verified event time: "
                    f"<t:{int(event_start_time.timestamp())}:F>"
                )

            with create_database_connection() as database_connection:
                odds_count_row = database_connection.execute(
                    """
                    SELECT COUNT(*) AS odds_count
                    FROM fight_odds
                    JOIN fights
                        ON fights.fight_id = fight_odds.fight_id
                    WHERE fights.event_id = ?
                    """,
                    (int(event_row["event_id"]),),
                ).fetchone()
            status_lines.append(
                "Odds coverage: "
                f"**{int(odds_count_row['odds_count'])} fight(s)**"
            )

        if status_values.get("last_sync_error"):
            status_lines.append(
                "Last data error: "
                f"`{status_values['last_sync_error'][:250]}`"
            )

        if status_values.get("last_odds_error"):
            status_lines.append(
                "Last odds error: "
                f"`{status_values['last_odds_error'][:250]}`"
            )
        elif status_values.get("last_odds_result"):
            status_lines.append(
                f"Odds: {status_values['last_odds_result']}"
            )

        if status_values.get("last_odds_empty"):
            status_lines.append(
                "Odds note: "
                f"**{status_values['last_odds_empty']}** "
                "MusicDude will keep retrying automatically."
            )

        await interaction.response.send_message(
            "\n".join(status_lines),
            ephemeral=True,
        )

    @ufc_group.command(
        name="leaderboard",
        description="Show season or all-time UFC Pick'em standings.",
    )
    @app_commands.describe(
        season="Calendar year, such as 2026; omit for all-time",
    )
    async def leaderboard_command(
        interaction: discord.Interaction,
        season: int | None = None,
    ) -> None:
        if season is not None and not 2020 <= season <= 2100:
            await interaction.response.send_message(
                "Enter a season from 2020 through 2100.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            embed=build_leaderboard_embed(season=season)
        )

    @ufc_group.command(
        name="recap",
        description="Show the recap for a completed UFC event.",
    )
    @app_commands.describe(
        event_id="Event ID; omit for the latest completed event",
    )
    async def recap_command(
        interaction: discord.Interaction,
        event_id: int | None = None,
    ) -> None:
        selected_event_id = (
            event_id
            if event_id is not None
            else get_latest_event_id(completed_only=True)
        )

        if selected_event_id is None:
            await interaction.response.send_message(
                "There is no completed UFC event to recap yet.",
                ephemeral=True,
            )
            return

        try:
            recap_embed = build_recap_embed(selected_event_id)
        except ValueError as error:
            await interaction.response.send_message(
                str(error),
                ephemeral=True,
            )
            return

        await interaction.response.send_message(embed=recap_embed)

    @ufc_group.command(
        name="fighter",
        description="Show a fighter's record and recent UFC history.",
    )
    @app_commands.describe(
        name="Full fighter name or a unique part of it",
    )
    async def fighter_command(
        interaction: discord.Interaction,
        name: str,
    ) -> None:
        await interaction.response.defer()

        try:
            fight_records = await get_fighter_history_records()
            canonical_name, matching_fights = find_fighter_matches(
                fight_records,
                name,
            )
        except Exception as error:
            await interaction.followup.send(
                f"Fighter lookup failed: {error}",
                ephemeral=True,
            )
            return

        if canonical_name is None:
            await interaction.followup.send(
                (
                    "That name was not unique. Try the fighter's "
                    "full name."
                ),
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            embed=describe_fighter_history(
                canonical_name,
                matching_fights,
            )
        )

    @ufc_group.command(
        name="matchup",
        description="Compare two fighters and their UFC history.",
    )
    @app_commands.describe(
        fighter_one="First fighter's full name",
        fighter_two="Second fighter's full name",
    )
    async def matchup_command(
        interaction: discord.Interaction,
        fighter_one: str,
        fighter_two: str,
    ) -> None:
        await interaction.response.defer()

        try:
            fight_records = await get_fighter_history_records()
            first_name, first_fights = find_fighter_matches(
                fight_records,
                fighter_one,
            )
            second_name, second_fights = find_fighter_matches(
                fight_records,
                fighter_two,
            )
        except Exception as error:
            await interaction.followup.send(
                f"Matchup lookup failed: {error}",
                ephemeral=True,
            )
            return

        if first_name is None or second_name is None:
            await interaction.followup.send(
                (
                    "One or both names were not unique. "
                    "Use both fighters' full names."
                ),
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            embed=describe_matchup(
                first_name,
                first_fights,
                second_name,
                second_fights,
            )
        )

    @ufc_group.command(
        name="reminders",
        description="Turn UFC pick reminders on or off.",
    )
    @app_commands.describe(
        setting="Enable or disable 24-hour and 2-hour reminders",
    )
    async def reminders_command(
        interaction: discord.Interaction,
        setting: Literal["on", "off"],
    ) -> None:
        if not user_can_manage_server(interaction):
            await send_admin_permission_error(interaction)
            return

        if interaction.guild_id is None:
            await interaction.response.send_message(
                "This command must be used inside a server.",
                ephemeral=True,
            )
            return

        enabled = setting == "on"
        set_reminders_enabled(interaction.guild_id, enabled)
        await interaction.response.send_message(
            f"UFC pick reminders are now **{setting}**.",
            ephemeral=True,
        )

    @ufc_group.command(
        name="season-champions",
        description="Show the winner from each completed season.",
    )
    async def season_champions_command(
        interaction: discord.Interaction,
    ) -> None:
        current_year = datetime.now(timezone.utc).year

        with create_database_connection() as database_connection:
            season_rows = database_connection.execute(
                """
                SELECT DISTINCT substr(start_time, 1, 4) AS season
                FROM events
                WHERE status = 'completed'
                    AND start_time IS NOT NULL
                ORDER BY season DESC
                """
            ).fetchall()

        champion_lines = []

        for season_row in season_rows:
            season_text = str(season_row["season"])

            if not season_text.isdigit():
                continue

            season_standings = build_leaderboard(
                season=int(season_text)
            )

            if season_standings:
                champion = season_standings[0]
                season_label = (
                    "leader"
                    if int(season_text) == current_year
                    else "champion"
                )
                season_icon = (
                    "👑"
                    if int(season_text) == current_year
                    else "🏆"
                )
                champion_lines.append(
                    f"{season_icon} **{season_text} {season_label}: "
                    f"{champion.display_name}** — "
                    f"{champion.points} points, "
                    f"{champion.accuracy:.0f}% accuracy"
                )

        await interaction.response.send_message(
            embed=discord.Embed(
                title="UFC Pick'em Season Leaders & Champions",
                description=(
                    "\n".join(champion_lines)
                    if champion_lines
                    else "No completed season has a champion yet."
                ),
                color=discord.Color.gold(),
            )
        )
