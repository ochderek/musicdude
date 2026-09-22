# MusicDude

A Python Discord bot I built for a server with friends. It plays personalized audio introductions when someone joins a voice channel and includes a UFC pick'em system for making fight predictions and tracking scores.

This project gave me a way to build something people could actually use while getting more experience with Python, APIs, asynchronous programming, and databases.

## Features

### Voice introductions
- Plays an assigned audio clip when a member joins a voice channel.
- Queues introductions so clips play one at a time.
- Checks that the member is still in a voice channel before playing.
- Ignores moves between voice channels.
- Disconnects once the queue is empty.
- Uses a default playback volume of 40%.

### UFC pick'em
- Uses Discord slash commands for submitting picks and viewing standings.
- Integrates with the API-Sports MMA API for fight information.
- Stores events, predictions, and results in SQLite.
- Includes scoring logic and background tasks for event automation.

### Process restart
- Includes a Windows batch script that restarts the bot after its process exits.
- Waits 10 seconds between restart attempts.

## Built With

- **Python**
- **discord.py** for Discord events, slash commands, and voice connections
- **asyncio** for background tasks and the audio queue
- **SQLite** for persistent pick'em data
- **aiohttp** for asynchronous API requests
- **python-dotenv** for loading local configuration
- **FFmpeg** for audio playback

## Project Structure

| File | Purpose |
|---|---|
| `bot.py` | Bot startup, Discord events, and voice introduction queue |
| `ufc_pickem/database.py` | SQLite database operations |
| `ufc_pickem/mma_api.py` | Requests to the MMA API |
| `ufc_pickem/scoring.py` | Prediction scoring logic |
| `ufc_pickem/ufc_commands.py` | UFC slash commands |
| `ufc_pickem/ufc_automatic.py` | Exposes the UFC automation setup |
| `ufc_pickem/ufc_complete.py` | Additional UFC features and background automation |
| `ufc_pickem/inspect_next_event.py` | Utility for inspecting upcoming event data |
| `requirements.txt` | Python dependencies |
| `start_bot.bat` | Windows launcher with automatic process restart |

## Getting Started

### 1. Clone the repository

```powershell
git clone https://github.com/ochderek/musicdude.git
cd musicdude
```

### 2. Create a virtual environment and install dependencies

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Install FFmpeg separately and make sure it is available on your system's PATH.

### 3. Configure your credentials

Create a `.env` file in the project root:

```dotenv
DISCORD_BOT_TOKEN=your_discord_bot_token
MMA_API_KEY=your_api_sports_mma_key
```

The Discord token connects your bot to Discord. The MMA API key is used by the UFC features.

Keep this file private. It is excluded from version control.

### 4. Invite your Discord bot

Create a bot application in the Discord Developer Portal and invite it to your server with the `bot` and `applications.commands` scopes.

Give it the permissions needed to view its channels, send messages, and connect and speak in voice channels. Additional permissions may be needed for specific UFC features.

### 5. Configure voice introductions

Create an `audio` folder in the project root and add your audio clips.

Create an `intros.json` file that maps Discord member IDs to filenames inside that folder:

```json
{
  "123456789012345678": "example_intro.mp3"
}
```

Replace the example ID with a real member's Discord ID. You can enable Developer Mode in Discord to copy member IDs.

Audio files and personal introduction mappings are kept out of the repository. To run without personalized introductions, use an empty mapping:

```json
{}
```

### 6. Start the bot

```powershell
.\.venv\Scripts\python.exe bot.py
```

Once the bot connects and its commands synchronize, use Discord's slash command menu to explore the UFC commands.

For automatic process restarts on Windows, use `start_bot.bat`. The current script contains my original local project path, so update that path to your own project folder before running it.

## Challenges and What I Learned

One challenge was handling multiple people joining voice channels close together. An asynchronous queue allows the bot to process introductions in order without playing overlapping clips.

Another focus was recovery after unexpected errors. The audio worker catches playback errors so it can continue processing requests, while the Windows launcher restarts the bot if the process exits. These handle different situations: recovering from a failed action inside the application and restarting the application itself.

The UFC features gave me experience connecting an external API to Discord commands and storing information in a relational database. Keeping API access, database operations, and scoring in separate modules also made it easier to follow how data moves through the application.

## Notes

- This is a personal project built for a Discord server with friends.
- Voice playback requires FFmpeg and appropriate Discord permissions.
- UFC features depend on the availability and coverage of the external MMA API.
- The restart script does not recover a frozen process or automatically start the bot after a computer reboot.
- Credentials, local databases, audio files, and member configuration are excluded from this repository.
