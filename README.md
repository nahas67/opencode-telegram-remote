# OpenCode Telegram Remote

Control your local OpenCode sessions from your phone via Telegram. A native
OpenCode V2 plugin (single file) plus an optional second send-only bot for
notifications.

> **Disclaimer:** Not affiliated with, endorsed by, or sponsored by OpenCode,
> SST, or any of their affiliates.

## What it does

- **Remote prompts** — send free text to the bound session (`/mode on`)
- **Session control** — list, bind, create, fork, interrupt, rename, delete
- **Approvals** — permission requests arrive with Allow/Deny buttons
- **Live follow** — heartbeats, completion alerts, `/live` status card
- **Files** — browse the project (`/ls`), attach files to prompts, send
  documents/photos from Telegram
- **Worktrees, models, agents, skills, commands, MCP servers**
- **Scheduled tasks** (`/task`, `/tasklist`), prompt queue with withdraw,
  message history with revert (`/messages`)
- **Dual-bot split** — commands on the control bot, alerts on a separate
  send-only notify bot

See `plugin/REMOTE-CONTROL-DESIGN.md` for the full design and
`plugin/src/telegram-v2.ts` for the implementation.

## Setup

1. Create two bots with [@BotFather](https://t.me/BotFather) (control + notify).
2. Copy `plugin/src/telegram-v2.ts` to your OpenCode plugins directory, e.g.
   `~/.config/opencode/plugins/telegram-notify/index.ts`.
3. Next to it, create `.env`:

```ini
TELEGRAM_BOT_TOKEN=<control bot token>
TELEGRAM_NOTIFY_TOKEN=<notify bot token>
TELEGRAM_ALLOWED_USER_IDS=<your Telegram user id>
TELEGRAM_CHAT_ID=<your Telegram user id>
# Optional: Whisper-compatible endpoint for voice transcription
#TELEGRAM_WHISPER_URL=https://api.openai.com/v1
#TELEGRAM_WHISPER_KEY=
```

4. `/start` both bots in Telegram (a bot cannot message you first).
5. `opencode reload` (run from inside a project directory).

## Use

In the control bot:

```
/tasks → /use <number> → /mode on → just type
```

`/help` lists all 40+ commands. Completion alerts arrive in both bots;
command replies only in the control bot.

## Notes

- Single-owner design: one Telegram user, one chat binding per user.
- Private chats only; `allowExec` stays `false` (exec approvals auto-deny).
- No voice transcription without a Whisper endpoint (honest message instead).
- `CODE_GENERATION` in the source must be bumped on every change, including
  new environment variables — OpenCode does not tear down the old instance.

## License

MIT — see [LICENSE](LICENSE).
