# OpenCode Telegram Integration

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Bun](https://img.shields.io/badge/Bun-%23000000.svg?logo=bun&logoColor=white)](https://bun.sh)
[![TypeScript](https://img.shields.io/badge/TypeScript-007ACC?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![grammY](https://img.shields.io/badge/grammY-Bot%20Framework-blue)](https://grammy.dev/)

A Telegram bot that orchestrates multiple [OpenCode](https://opencode.ai) instances through forum topics. Each forum topic in a Telegram supergroup gets its own dedicated OpenCode instance, enabling multi-user/multi-project AI assistance.

> **Developer?** Architecture, configuration reference, API details, and
> contributing internals live in [DEV.md](DEV.md).

## Features

- **Forum Topic to OpenCode Instance**: Each topic gets a dedicated OpenCode session
- **Real-time Streaming**: Responses stream into Telegram as editable messages
- **Session Discovery**: Connect to any running OpenCode instance on your machine
- **Instance Lifecycle Management**: Auto-start, health checks, crash recovery, idle timeout
- **Persistent State**: Topics and instances survive bot restarts
- **Permission Handling**: Approve/deny dangerous operations via inline buttons
- **Telegram-Aware Answers**: Replies stay short, use Telegram-safe formatting, and lead with the conclusion
- **Connection Resilience**: Dropped event streams reconnect automatically; if live updates can't be restored, the bot says so in chat instead of going silent

## Table of Contents

- [Quick Start](#quick-start)
- [Install as a User Service](#install-as-a-user-service)
- [Running with Docker](#running-with-docker)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Quick Start

### Prerequisites

- [Bun](https://bun.sh) runtime (v1.0+)
- [OpenCode](https://opencode.ai) CLI installed
- Telegram account

### 1. Create a Telegram Bot

1. Open Telegram and message [@BotFather](https://t.me/BotFather)
2. Send `/newbot` and follow the prompts
3. Copy the bot token (looks like `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`)

### 2. Create a Supergroup with Topics

1. Create a new Telegram group
2. Convert it to a supergroup (Settings → Group Type → Supergroup)
3. Enable Topics (Settings → Topics → Enable)
4. Add your bot as an **admin** with permissions to manage topics

### 3. Get Your Chat ID

The chat ID for supergroups starts with `-100`. You can find it by:

1. Adding [@RawDataBot](https://t.me/RawDataBot) to your group temporarily
2. It will show the chat ID in its message
3. Remove the bot after getting the ID

### 4. Install and Configure

```bash
# Clone the repository
git clone https://github.com/huynle/opencode-telegram.git
cd opencode-telegram

# Guided setup: walks through each setting, validates input, writes .env
bash scripts/setup-env.sh
```

Re-running the setup script is safe: it backs up the existing `.env` first
and keeps current values as defaults.

Alternatively, configure manually:

```bash
cp .env.example .env
# Edit .env with your bot token and chat ID
```

### 5. Run the Bot

```bash
# Development with hot reload
bun run dev

# Production
bun run start
```

## Install as a User Service

For a bot that survives logouts and restarts, install it as a per-user
systemd service. The installer is user-space-only: no root, no sudo, no
system-wide unit, nothing under `/opt`. It copies the repo to
`~/.local/share/opencode-telegram` by default and runs the service as your
own account via `systemctl --user`.

```bash
# Preview what the installer will do (changes nothing)
bash scripts/install.sh --dry-run

# Install
bash scripts/install.sh
```

Run `bash scripts/install.sh --help` for all flags (`--prefix`,
`--env-from`, `--non-interactive`, `--uninstall`/`--yes`, `--dry-run`,
`--allow-root`). The old `--system` and `--user NAME` flags were removed
and now abort with an error — just re-run without them.

If the installer reports that lingering is off, enable it so the service
starts on boot without a login (the installer offers this step, or prints
the command under `--non-interactive`/`--dry-run`):

```bash
sudo loginctl enable-linger $USER
```

Check status, logs, and health:

```bash
systemctl --user is-active opencode-telegram.service
journalctl --user -u opencode-telegram.service -f
curl -s http://localhost:4200/api/health
```

Uninstall:

```bash
bash scripts/install.sh --uninstall          # removes the unit, keeps installed files (incl. data/)
bash scripts/install.sh --uninstall --yes    # also deletes the install prefix after typed DELETE confirmation
```

### Migrating from an old system-wide install

Old system-mode installs leave root-owned leftovers the new installer will
not touch. Keep exactly ONE service per bot token — two bot copies polling
Telegram cause `409 Conflict` errors and flapping. Migrate with a fresh
user-space install above, then remove the old pieces:

```bash
sudo systemctl disable --now opencode-telegram.service
sudo rm -f /etc/systemd/system/opencode-telegram.service && sudo systemctl daemon-reload
sudo rm -rf /opt/opencode-telegram   # deletes its data/ (chat/session DBs) — back up first if history matters
```

`--uninstall` prints these exact remediation lines automatically when it
detects the leftovers.

## Running with Docker

> **Recommendation:** running natively with Bun gives you everything,
> including session discovery. Docker is fine if you only need instances
> created via `/new`, or if you register external instances via the API.

Short version:

```bash
docker build -t opencode-telegram .
docker run -d --name opencode-telegram \
  --network=host \
  -v $(pwd)/data:/app/data \
  -v ~/oc-bot:/root/oc-bot \
  --env-file .env \
  opencode-telegram
```

Things to know:

- **Discovery doesn't work in plain Docker.** The bot finds host OpenCode
  sessions via `ps`/`lsof`, which can't see host processes from inside a
  container. On Linux you can add `--pid=host` to enable it.
- On macOS/Windows, Docker Desktop runs in a VM, so discovery won't work
  there at all — run natively or register instances via the API instead.
- Useful commands: `docker logs -f opencode-telegram`,
  `docker stop opencode-telegram`, `docker compose up -d`.

Full Docker details (compose file, all three run modes, volume table) are in
[DEV.md](DEV.md#docker-details).

## Usage

### General Topic Commands (Control Plane)

These commands work in the General topic of your supergroup:

| Command | Description |
|---------|-------------|
| `/new <name>` | Create folder + topic + start OpenCode instance |
| `/sessions` | List all OpenCode sessions (managed + discovered) |
| `/connect <name>` | Connect to an existing session by name or ID |
| `/clear` | Clean up stale topic mappings |
| `/status` | Show orchestrator status |
| `/help` | Show context-aware help |

### Topic Commands (Inside a Session)

These commands work inside individual topic threads:

| Command | Description |
|---------|-------------|
| `/session` | Show current topic's OpenCode session info |
| `/link <path>` | Link topic to existing project directory |
| `/stream` | Toggle real-time streaming on/off |
| `/disconnect` | Disconnect session and delete topic |
| `/help` | Show context-aware help |

### Session Discovery

The bot can discover any running OpenCode instance on your machine:

```
/sessions              # Lists all sessions including discovered ones
/connect myproject     # Connect to a discovered session by name
/connect ses_abc123    # Connect by session ID prefix
```

Discovered sessions show with a magnifying glass icon in `/sessions` output.

> **Note**: Discovery requires the bot to run natively (not in Docker) or
> with `--pid=host` on Linux.

### Topic Naming Convention

Topics follow the `<project>-<session title>` naming convention:

1. **On `/new <project>`**: Topic is created with just `<project>` name initially
2. **After first message**: Once OpenCode generates a session title, the topic is automatically renamed to `<project>-<session title>`
3. **On `/connect`**: If the session already has a title, the topic is created with `<project>-<session title>` immediately

## Troubleshooting

### Instance Won't Start (Port Conflict)

**Symptom**: Instance crashes with "Failed to start server on port 4100"

**Solution**: The bot cleans up stale ports automatically on start. For manual cleanup:

```bash
lsof -ti:4100 | xargs kill
```

### No Response After Sending a Message

**Symptom**: SSE events received but nothing arrives in Telegram

**Solution**: Check that the topic is properly linked with the `/session` command.
If the bot recently restarted, send your message again — in-flight prompts
from before a restart are not replayed.

### Duplicate Messages

**Symptom**: Multiple "Thinking..." or response messages

**Solution**: Fixed in the current version (subscriptions are cleaned up on
restart, and harmless "message is not modified" edits are ignored). If you
still see duplicates, restart the bot and report it.

### Discovery Not Working in Docker

**Symptom**: `/sessions` only shows managed instances, not host OpenCode sessions

**Cause**: Docker containers have isolated process namespaces.

**Solutions** (in order of recommendation):
1. **Run natively**: `bun run start` (recommended)
2. **Linux with `--pid=host`**: Shares host process namespace
3. **External API**: Manually register instances via `/api/register` (see [DEV.md](DEV.md#external-instance-api))

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [OpenCode](https://opencode.ai) - The AI coding assistant this bot integrates with
- [grammY](https://grammy.dev/) - The Telegram Bot framework
- [Bun](https://bun.sh) - The JavaScript runtime
