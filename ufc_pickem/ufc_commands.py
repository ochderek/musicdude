from typing import Literal

import discord
from discord import app_commands

from ufc_pickem.database import (
    add_fight,
    create_event,
    get_event,
    get_event_fights,
    get_events,
    get_scored_picks,
    get_user_picks,
    initialize_database,
    record_result,
    save_pick,
)
from ufc_pickem.scoring import calculate_pick_score


FightMethod = Literal["KO/TKO", "Submission", "Decision"]


def user_can_manage_server(interaction: discord.Interaction) -> bool:
    return interaction.permissions.manage_guild


async def send_admin_permission_error(
    interaction: discord.Interaction,
) -> None:
    await interaction.response.send_message(
        "You need the Manage Server permission to use this command.",
        ephemeral=True,
    )


def register_ufc_commands(
    command_tree: app_commands.CommandTree,
) -> None:
    initialize_database()

    ufc_group = app_commands.Group(
        name="ufc",
        description="UFC Pick'em commands",
    )

    @ufc_group.command(
        name="create-event",
        description="Create a new UFC event.",
    )
    @app_commands.describe(
        event_name="The UFC event name",
        start_time="Optional event date and start time",
    )
    async def create_event_command(
        interaction: discord.Interaction,
        event_name: str,
        start_time: str | None = None,
    ) -> None:
        if not user_can_manage_server(interaction):
            await send_admin_permission_error(interaction)
            return

        try:
            event_id = create_event(
                event_name=event_name,
                start_time=start_time,
            )
        except ValueError as error:
            await interaction.response.send_message(
                str(error),
                ephemeral=True,
            )
            return

        message = (
            f"Created **{event_name.strip()}**.\n"
            f"Event ID: `{event_id}`"
        )

        if start_time:
            message += f"\nStart time: {start_time}"

        await interaction.response.send_message(
            message,
            ephemeral=True,
        )

    @ufc_group.command(
        name="add-fight",
        description="Add a fight to a UFC event.",
    )
    @app_commands.describe(
        event_id="The event ID",
        fighter_one="The first fighter's name",
        fighter_two="The second fighter's name",
    )
    async def add_fight_command(
        interaction: discord.Interaction,
        event_id: int,
        fighter_one: str,
        fighter_two: str,
    ) -> None:
        if not user_can_manage_server(interaction):
            await send_admin_permission_error(interaction)
            return

        try:
            fight_id = add_fight(
                event_id=event_id,
                fighter_one=fighter_one,
                fighter_two=fighter_two,
            )
        except ValueError as error:
            await interaction.response.send_message(
                str(error),
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            (
                f"Added **{fighter_one.strip()} vs. "
                f"{fighter_two.strip()}**.\n"
                f"Fight ID: `{fight_id}`"
            ),
            ephemeral=True,
        )

    @ufc_group.command(
        name="events",
        description="Show saved UFC events.",
    )
    async def events_command(
        interaction: discord.Interaction,
    ) -> None:
        event_rows = get_events()

        if not event_rows:
            await interaction.response.send_message(
                "There are no saved UFC events yet.",
                ephemeral=True,
            )
            return

        event_lines: list[str] = []

        for event_row in event_rows:
            event_line = (
                f"`{event_row['event_id']}` — "
                f"**{event_row['event_name']}** "
                f"({event_row['status']})"
            )

            if event_row["start_time"]:
                event_line += f"\nStart: {event_row['start_time']}"

            event_lines.append(event_line)

        await interaction.response.send_message(
            "\n\n".join(event_lines),
            ephemeral=True,
        )

    @ufc_group.command(
        name="card",
        description="Show every fight saved for an event.",
    )
    @app_commands.describe(
        event_id="The event ID",
    )
    async def card_command(
        interaction: discord.Interaction,
        event_id: int,
    ) -> None:
        event_row = get_event(event_id)

        if event_row is None:
            await interaction.response.send_message(
                f"Event {event_id} does not exist.",
                ephemeral=True,
            )
            return

        fight_rows = get_event_fights(event_id)

        if not fight_rows:
            await interaction.response.send_message(
                f"**{event_row['event_name']}** has no fights yet.",
                ephemeral=True,
            )
            return

        fight_lines = [
            (
                f"`{fight_row['fight_id']}` — "
                f"**{fight_row['fighter_one']}** vs. "
                f"**{fight_row['fighter_two']}**"
            )
            for fight_row in fight_rows
        ]

        await interaction.response.send_message(
            (
                f"# {event_row['event_name']}\n"
                + "\n".join(fight_lines)
            )
        )

    @ufc_group.command(
        name="pick",
        description="Submit or change your pick for a fight.",
    )
    @app_commands.describe(
        fight_id="The fight ID shown on the event card",
        winner="The fighter you predict will win",
        method="How you predict the fighter will win",
        predicted_round="The predicted ending round",
    )
    @app_commands.rename(predicted_round="round")
    async def pick_command(
        interaction: discord.Interaction,
        fight_id: int,
        winner: str,
        method: FightMethod | None = None,
        predicted_round: int | None = None,
    ) -> None:
        try:
            save_pick(
                discord_user_id=interaction.user.id,
                display_name=interaction.user.display_name,
                fight_id=fight_id,
                predicted_winner=winner,
                predicted_method=method,
                predicted_round=predicted_round,
            )
        except ValueError as error:
            await interaction.response.send_message(
                str(error),
                ephemeral=True,
            )
            return

        pick_details = [f"Winner: **{winner.strip()}**"]

        if method:
            pick_details.append(f"Method: **{method}**")

        if predicted_round:
            pick_details.append(f"Round: **{predicted_round}**")

        await interaction.response.send_message(
            "Your pick was saved.\n" + "\n".join(pick_details),
            ephemeral=True,
        )

    @ufc_group.command(
        name="my-picks",
        description="Show your picks for a UFC event.",
    )
    @app_commands.describe(
        event_id="The event ID",
    )
    async def my_picks_command(
        interaction: discord.Interaction,
        event_id: int,
    ) -> None:
        event_row = get_event(event_id)

        if event_row is None:
            await interaction.response.send_message(
                f"Event {event_id} does not exist.",
                ephemeral=True,
            )
            return

        pick_rows = get_user_picks(
            discord_user_id=interaction.user.id,
            event_id=event_id,
        )

        if not pick_rows:
            await interaction.response.send_message(
                f"You have no picks saved for **{event_row['event_name']}**.",
                ephemeral=True,
            )
            return

        pick_lines: list[str] = []

        for pick_row in pick_rows:
            pick_line = (
                f"**{pick_row['fighter_one']} vs. "
                f"{pick_row['fighter_two']}**\n"
                f"Winner: {pick_row['predicted_winner']}"
            )

            if pick_row["predicted_method"]:
                pick_line += (
                    f" | Method: {pick_row['predicted_method']}"
                )

            if pick_row["predicted_round"]:
                pick_line += (
                    f" | Round: {pick_row['predicted_round']}"
                )

            pick_lines.append(pick_line)

        await interaction.response.send_message(
            (
                f"# Your picks: {event_row['event_name']}\n"
                + "\n\n".join(pick_lines)
            ),
            ephemeral=True,
        )

    @ufc_group.command(
        name="record-result",
        description="Record the official result of a fight.",
    )
    @app_commands.describe(
        fight_id="The fight ID",
        winner="The fighter who officially won",
        method="The official winning method",
        ending_round="The round in which the fight ended",
    )
    @app_commands.rename(ending_round="round")
    async def record_result_command(
        interaction: discord.Interaction,
        fight_id: int,
        winner: str,
        method: FightMethod,
        ending_round: int,
    ) -> None:
        if not user_can_manage_server(interaction):
            await send_admin_permission_error(interaction)
            return

        try:
            record_result(
                fight_id=fight_id,
                winner=winner,
                method=method,
                ending_round=ending_round,
            )
        except ValueError as error:
            await interaction.response.send_message(
                str(error),
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            (
                "Official result saved.\n"
                f"Winner: **{winner.strip()}**\n"
                f"Method: **{method}**\n"
                f"Round: **{ending_round}**"
            ),
            ephemeral=True,
        )

    @ufc_group.command(
        name="standings",
        description="Show the standings for a UFC event.",
    )
    @app_commands.describe(
        event_id="The event ID",
    )
    async def standings_command(
        interaction: discord.Interaction,
        event_id: int,
    ) -> None:
        event_row = get_event(event_id)

        if event_row is None:
            await interaction.response.send_message(
                f"Event {event_id} does not exist.",
                ephemeral=True,
            )
            return

        scored_pick_rows = get_scored_picks(event_id)

        if not scored_pick_rows:
            await interaction.response.send_message(
                (
                    f"There are no scored picks for "
                    f"**{event_row['event_name']}** yet."
                )
            )
            return

        standings_by_user: dict[int, tuple[str, int, int]] = {}

        for scored_pick_row in scored_pick_rows:
            pick_score = calculate_pick_score(
                predicted_winner=scored_pick_row["predicted_winner"],
                predicted_method=scored_pick_row["predicted_method"],
                predicted_round=scored_pick_row["predicted_round"],
                official_winner=scored_pick_row["official_winner"],
                official_method=scored_pick_row["official_method"],
                official_round=scored_pick_row["official_round"],
            )

            discord_user_id = scored_pick_row["discord_user_id"]
            display_name = scored_pick_row["display_name"]

            previous_standing = standings_by_user.get(
                discord_user_id,
                (display_name, 0, 0),
            )

            standings_by_user[discord_user_id] = (
                display_name,
                previous_standing[1] + pick_score.total_points,
                previous_standing[2] + 1,
            )

        sorted_standings = sorted(
            standings_by_user.values(),
            key=lambda standing: (
                -standing[1],
                standing[0].casefold(),
            ),
        )

        standing_lines = [
            (
                f"**{position}. {display_name}** — "
                f"{total_points} points "
                f"({scored_fights} scored picks)"
            )
            for position, (
                display_name,
                total_points,
                scored_fights,
            ) in enumerate(sorted_standings, start=1)
        ]

        await interaction.response.send_message(
            (
                f"# {event_row['event_name']} Standings\n"
                + "\n".join(standing_lines)
            )
        )

    command_tree.add_command(ufc_group)