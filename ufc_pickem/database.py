import sqlite3
from pathlib import Path


UFC_FOLDER_PATH = Path(__file__).resolve().parent
DATABASE_PATH = UFC_FOLDER_PATH / "ufc_pickem.db"


def create_database_connection() -> sqlite3.Connection:
    database_connection = sqlite3.connect(DATABASE_PATH)
    database_connection.row_factory = sqlite3.Row
    database_connection.execute("PRAGMA foreign_keys = ON")
    return database_connection


def initialize_database() -> None:
    with create_database_connection() as database_connection:
        database_connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                discord_user_id INTEGER PRIMARY KEY,
                display_name TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_name TEXT NOT NULL,
                start_time TEXT,
                status TEXT NOT NULL DEFAULT 'upcoming'
                    CHECK (status IN ('upcoming', 'active', 'completed'))
            );

            CREATE TABLE IF NOT EXISTS fights (
                fight_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                fighter_one TEXT NOT NULL,
                fighter_two TEXT NOT NULL,
                fight_order INTEGER,
                status TEXT NOT NULL DEFAULT 'upcoming'
                    CHECK (
                        status IN (
                            'upcoming',
                            'active',
                            'completed',
                            'canceled'
                        )
                    ),
                FOREIGN KEY (event_id)
                    REFERENCES events(event_id)
                    ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS picks (
                pick_id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_user_id INTEGER NOT NULL,
                fight_id INTEGER NOT NULL,
                predicted_winner TEXT NOT NULL,
                predicted_method TEXT,
                predicted_round INTEGER,
                submitted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (discord_user_id)
                    REFERENCES users(discord_user_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (fight_id)
                    REFERENCES fights(fight_id)
                    ON DELETE CASCADE,
                UNIQUE (discord_user_id, fight_id)
            );

            CREATE TABLE IF NOT EXISTS results (
                fight_id INTEGER PRIMARY KEY,
                winner TEXT NOT NULL,
                method TEXT,
                ending_round INTEGER,
                recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fight_id)
                    REFERENCES fights(fight_id)
                    ON DELETE CASCADE
            );
            """
        )

    print(f"UFC Pick'em database ready: {DATABASE_PATH}")


def create_event(
    event_name: str,
    start_time: str | None = None,
) -> int:
    cleaned_event_name = event_name.strip()

    if not cleaned_event_name:
        raise ValueError("The event name cannot be empty.")

    with create_database_connection() as database_connection:
        database_cursor = database_connection.execute(
            """
            INSERT INTO events (event_name, start_time)
            VALUES (?, ?)
            """,
            (cleaned_event_name, start_time),
        )

        return int(database_cursor.lastrowid)


def get_events() -> list[sqlite3.Row]:
    with create_database_connection() as database_connection:
        event_rows = database_connection.execute(
            """
            SELECT
                event_id,
                event_name,
                start_time,
                status
            FROM events
            ORDER BY event_id DESC
            """
        ).fetchall()

    return event_rows


def get_event(event_id: int) -> sqlite3.Row | None:
    with create_database_connection() as database_connection:
        event_row = database_connection.execute(
            """
            SELECT
                event_id,
                event_name,
                start_time,
                status
            FROM events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()

    return event_row


def add_fight(
    event_id: int,
    fighter_one: str,
    fighter_two: str,
) -> int:
    cleaned_fighter_one = fighter_one.strip()
    cleaned_fighter_two = fighter_two.strip()

    if not cleaned_fighter_one or not cleaned_fighter_two:
        raise ValueError("Both fighter names are required.")

    if cleaned_fighter_one.casefold() == cleaned_fighter_two.casefold():
        raise ValueError("A fighter cannot compete against themselves.")

    with create_database_connection() as database_connection:
        event_row = database_connection.execute(
            """
            SELECT status
            FROM events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()

        if event_row is None:
            raise ValueError(f"Event {event_id} does not exist.")

        if event_row["status"] == "completed":
            raise ValueError("Fights cannot be added to a completed event.")

        fight_order_row = database_connection.execute(
            """
            SELECT COALESCE(MAX(fight_order), 0) + 1 AS next_fight_order
            FROM fights
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()

        fight_order = int(fight_order_row["next_fight_order"])

        database_cursor = database_connection.execute(
            """
            INSERT INTO fights (
                event_id,
                fighter_one,
                fighter_two,
                fight_order
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                event_id,
                cleaned_fighter_one,
                cleaned_fighter_two,
                fight_order,
            ),
        )

        return int(database_cursor.lastrowid)


def get_fight(fight_id: int) -> sqlite3.Row | None:
    with create_database_connection() as database_connection:
        fight_row = database_connection.execute(
            """
            SELECT
                fights.fight_id,
                fights.event_id,
                fights.fighter_one,
                fights.fighter_two,
                fights.fight_order,
                fights.status,
                events.event_name
            FROM fights
            JOIN events
                ON events.event_id = fights.event_id
            WHERE fights.fight_id = ?
            """,
            (fight_id,),
        ).fetchone()

    return fight_row


def get_event_fights(event_id: int) -> list[sqlite3.Row]:
    with create_database_connection() as database_connection:
        fight_rows = database_connection.execute(
            """
            SELECT
                fight_id,
                event_id,
                fighter_one,
                fighter_two,
                fight_order,
                status
            FROM fights
            WHERE event_id = ?
            ORDER BY fight_order
            """,
            (event_id,),
        ).fetchall()

    return fight_rows


def save_pick(
    discord_user_id: int,
    display_name: str,
    fight_id: int,
    predicted_winner: str,
    predicted_method: str | None,
    predicted_round: int | None,
) -> None:
    if predicted_round is not None and not 1 <= predicted_round <= 5:
        raise ValueError("The predicted round must be between 1 and 5.")

    with create_database_connection() as database_connection:
        fight_row = database_connection.execute(
            """
            SELECT
                fighter_one,
                fighter_two,
                status
            FROM fights
            WHERE fight_id = ?
            """,
            (fight_id,),
        ).fetchone()

        if fight_row is None:
            raise ValueError(f"Fight {fight_id} does not exist.")

        if fight_row["status"] != "upcoming":
            raise ValueError("Picks are closed for this fight.")

        valid_fighter_names = (
            fight_row["fighter_one"],
            fight_row["fighter_two"],
        )

        matching_fighter_name = next(
            (
                fighter_name
                for fighter_name in valid_fighter_names
                if fighter_name.casefold()
                == predicted_winner.strip().casefold()
            ),
            None,
        )

        if matching_fighter_name is None:
            raise ValueError(
                "The winner must match one of the two fighters exactly."
            )

        database_connection.execute(
            """
            INSERT INTO users (
                discord_user_id,
                display_name
            )
            VALUES (?, ?)
            ON CONFLICT(discord_user_id) DO UPDATE SET
                display_name = excluded.display_name,
                updated_at = CURRENT_TIMESTAMP
            """,
            (discord_user_id, display_name),
        )

        database_connection.execute(
            """
            INSERT INTO picks (
                discord_user_id,
                fight_id,
                predicted_winner,
                predicted_method,
                predicted_round
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(discord_user_id, fight_id) DO UPDATE SET
                predicted_winner = excluded.predicted_winner,
                predicted_method = excluded.predicted_method,
                predicted_round = excluded.predicted_round,
                submitted_at = CURRENT_TIMESTAMP
            """,
            (
                discord_user_id,
                fight_id,
                matching_fighter_name,
                predicted_method,
                predicted_round,
            ),
        )


def get_user_picks(
    discord_user_id: int,
    event_id: int,
) -> list[sqlite3.Row]:
    with create_database_connection() as database_connection:
        pick_rows = database_connection.execute(
            """
            SELECT
                picks.pick_id,
                picks.fight_id,
                picks.predicted_winner,
                picks.predicted_method,
                picks.predicted_round,
                fights.fighter_one,
                fights.fighter_two,
                fights.fight_order
            FROM picks
            JOIN fights
                ON fights.fight_id = picks.fight_id
            WHERE picks.discord_user_id = ?
                AND fights.event_id = ?
            ORDER BY fights.fight_order
            """,
            (discord_user_id, event_id),
        ).fetchall()

    return pick_rows


def record_result(
    fight_id: int,
    winner: str,
    method: str | None,
    ending_round: int | None,
) -> None:
    if ending_round is not None and not 1 <= ending_round <= 5:
        raise ValueError("The ending round must be between 1 and 5.")

    with create_database_connection() as database_connection:
        fight_row = database_connection.execute(
            """
            SELECT
                event_id,
                fighter_one,
                fighter_two,
                status
            FROM fights
            WHERE fight_id = ?
            """,
            (fight_id,),
        ).fetchone()

        if fight_row is None:
            raise ValueError(f"Fight {fight_id} does not exist.")

        valid_fighter_names = (
            fight_row["fighter_one"],
            fight_row["fighter_two"],
        )

        matching_fighter_name = next(
            (
                fighter_name
                for fighter_name in valid_fighter_names
                if fighter_name.casefold() == winner.strip().casefold()
            ),
            None,
        )

        if matching_fighter_name is None:
            raise ValueError(
                "The winner must match one of the two fighters exactly."
            )

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
                method = excluded.method,
                ending_round = excluded.ending_round,
                recorded_at = CURRENT_TIMESTAMP
            """,
            (
                fight_id,
                matching_fighter_name,
                method,
                ending_round,
            ),
        )

        database_connection.execute(
            """
            UPDATE fights
            SET status = 'completed'
            WHERE fight_id = ?
            """,
            (fight_id,),
        )


def get_scored_picks(event_id: int) -> list[sqlite3.Row]:
    with create_database_connection() as database_connection:
        scored_pick_rows = database_connection.execute(
            """
            SELECT
                users.discord_user_id,
                users.display_name,
                picks.fight_id,
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
            JOIN results
                ON results.fight_id = picks.fight_id
            WHERE fights.event_id = ?
            ORDER BY users.display_name, fights.fight_order
            """,
            (event_id,),
        ).fetchall()

    return scored_pick_rows


if __name__ == "__main__":
    initialize_database()