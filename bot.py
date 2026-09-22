import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path

import discord
from dotenv import load_dotenv

from discord import app_commands
from ufc_pickem.ufc_commands import register_ufc_commands
from ufc_pickem.ufc_automatic import (
    register_automatic_ufc_commands,
    start_ufc_background_tasks,
)


PROJECT_FOLDER_PATH = Path(__file__).resolve().parent
AUDIO_FOLDER_PATH = PROJECT_FOLDER_PATH / "audio"
INTRO_ASSIGNMENTS_PATH = PROJECT_FOLDER_PATH / "intros.json"


@dataclass
class IntroRequest:
    member: discord.Member
    audio_file_path: Path


load_dotenv(PROJECT_FOLDER_PATH / ".env")
discord_bot_token = os.getenv("DISCORD_BOT_TOKEN")

intents = discord.Intents.none()
intents.guilds = True
intents.voice_states = True

bot = discord.Client(intents=intents)
command_tree = app_commands.CommandTree(bot)
register_ufc_commands(command_tree)
register_automatic_ufc_commands(command_tree)

commands_have_synced = False

intro_queue: asyncio.Queue[IntroRequest] = asyncio.Queue()
intro_worker_task: asyncio.Task | None = None


def load_intro_assignments() -> dict[str, str]:
    try:
        with INTRO_ASSIGNMENTS_PATH.open("r", encoding="utf-8") as intros_file:
            intro_assignments = json.load(intros_file)
    except FileNotFoundError:
        print("intros.json could not be found.")
        return {}
    except json.JSONDecodeError as error:
        print(f"intros.json contains invalid JSON: {error}")
        return {}

    if not isinstance(intro_assignments, dict):
        print("intros.json must contain a JSON object.")
        return {}

    return {
        str(discord_user_id): str(audio_file_name)
        for discord_user_id, audio_file_name in intro_assignments.items()
    }


async def disconnect_all_voice_clients() -> None:
    for voice_client in list(bot.voice_clients):
        if voice_client.is_connected():
            try:
                await voice_client.disconnect(force=True)
            except discord.DiscordException as error:
                print(f"Could not disconnect from voice: {error}")


async def play_intro(intro_request: IntroRequest) -> None:
    member = intro_request.member

    if member.voice is None or member.voice.channel is None:
        print(f"Skipping {member.display_name}: they disconnected before their intro.")
        return

    if not intro_request.audio_file_path.is_file():
        print(f"Audio file not found: {intro_request.audio_file_path}")
        return

    target_voice_channel = member.voice.channel

    voice_client = discord.utils.get(
        bot.voice_clients,
        guild=member.guild,
    )

    if voice_client is None or not voice_client.is_connected():
        voice_client = await target_voice_channel.connect()
    elif voice_client.channel != target_voice_channel:
        await voice_client.move_to(target_voice_channel)

    playback_finished_event = asyncio.Event()
    playback_error: Exception | None = None
    event_loop = asyncio.get_running_loop()

    def finish_playback(error: Exception | None) -> None:
        nonlocal playback_error
        playback_error = error
        event_loop.call_soon_threadsafe(playback_finished_event.set)

    ffmpeg_audio_source = discord.FFmpegPCMAudio(
        str(intro_request.audio_file_path),
        options="-vn",
    )

    audio_source = discord.PCMVolumeTransformer(
        ffmpeg_audio_source,
        volume=0.40,
    )

    print(
        f"Playing {intro_request.audio_file_path.name} "
        f"for {member.display_name} in {target_voice_channel.name}."
    )

    try:
        voice_client.play(audio_source, after=finish_playback)
        await playback_finished_event.wait()
    except Exception:
        audio_source.cleanup()
        raise

    if playback_error is not None:
        print(f"Audio playback error: {playback_error}")


async def process_intro_queue() -> None:
    while not bot.is_closed():
        intro_request = await intro_queue.get()

        try:
            await play_intro(intro_request)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            print(f"Could not play an intro: {error}")
        finally:
            intro_queue.task_done()

        if intro_queue.empty():
            await disconnect_all_voice_clients()


@bot.event
async def on_ready() -> None:
    global intro_worker_task
    global commands_have_synced

    start_ufc_background_tasks(bot)

    if intro_worker_task is None or intro_worker_task.done():
        intro_worker_task = asyncio.create_task(process_intro_queue())

    if not commands_have_synced:
        try:
            synced_commands = await command_tree.sync()
            commands_have_synced = True
            print(
                f"Synced {len(synced_commands)} Discord command group(s)."
            )
        except discord.HTTPException as error:
            print(f"Could not sync Discord commands: {error}")

    print(f"{bot.user} is online and ready.")
    print(f"Connected to {len(bot.guilds)} server(s).")


@bot.event
async def on_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
) -> None:
    if member.bot:
        return

    joined_from_disconnected = (
        before.channel is None and after.channel is not None
    )

    if not joined_from_disconnected:
        return

    intro_assignments = load_intro_assignments()
    audio_file_name = intro_assignments.get(str(member.id))

    if audio_file_name is None:
        print(f"No intro assigned to {member.display_name}.")
        return

    audio_file_path = (AUDIO_FOLDER_PATH / audio_file_name).resolve()
    resolved_audio_folder_path = AUDIO_FOLDER_PATH.resolve()

    if resolved_audio_folder_path not in audio_file_path.parents:
        print(f"Invalid audio filename assigned to {member.display_name}.")
        return

    if not audio_file_path.is_file():
        print(f"Assigned audio file does not exist: {audio_file_path}")
        return

    await intro_queue.put(
        IntroRequest(
            member=member,
            audio_file_path=audio_file_path,
        )
    )

    print(f"Queued intro for {member.display_name}.")


if not discord_bot_token:
    raise SystemExit(
        "DISCORD_BOT_TOKEN is missing. Add it to the .env file."
    )

bot.run(discord_bot_token)
