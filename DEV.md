# Developer Guide

Technical reference for contributors and operators of the OpenCode Telegram
Integration. End-user setup and bot commands live in [README.md](README.md).

## Table of Contents

- [Architecture](#architecture)
- [Directory Structure](#directory-structure)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Development](#development)
- [Streaming and SSE Internals](#streaming-and-sse-internals)
- [Testing Notes](#testing-notes)
- [Docker Details](#docker-details)
- [Contributing](#contributing)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Telegram Supergroup (Forum)                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                          │
│  │ Topic #1 │  │ Topic #2 │  │ Topic #3 │  ...                     │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                          │
└───────┼─────────────┼─────────────┼─────────────────────────────────┘
        │             │             │
        ▼             ▼             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Integration Layer                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │ grammY Bot  │  │TopicManager │  │StreamHandler│                 │
│  └─────────────┘  └─────────────┘  └─────────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
        │             │             │
        ▼             ▼             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Instance Manager (Orchestrator)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ Instance #1  │  │ Instance #2  │  │ Instance #3  │  ...         │
│  │ Port 4100    │  │ Port 4101    │  │ Port 4102    │              │
│  │ opencode     │  │ opencode     │  │ opencode     │              │
│  │ serve        │  │ serve        │  │ serve        │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
└─────────────────────────────────────────────────────────────────────┘
```

Key components:

- **Integration layer** (`src/integration.ts`): wires everything together —
  creates the grammY bot, handles orchestrator events, owns OpenCode clients
  and SSE subscriptions, routes messages between Telegram and OpenCode.
- **Instance manager** (`src/orchestrator/manager.ts`): lifecycle of OpenCode
  instances — on-demand creation, health checks, crash recovery, idle
  timeout, SQLite-backed restart recovery.
- **Stream handler** (`src/opencode/stream-handler.ts`): bridges SSE events to
  Telegram — "Thinking..." progress messages, throttled text streaming, tool
  status, in-place message edits.
- **Topic manager** (`src/forum/topic-manager.ts`): topic → session mapping,
  automatic session creation for new topics, message routing.

### Directory Structure

```
src/
├── index.ts              # Entry point
├── config.ts             # Configuration from environment
├── integration.ts        # Wires all components together
├── api-server.ts         # External instance registration API
├── bot/
│   └── handlers/
│       └── forum.ts      # Telegram message/command handlers
├── forum/
│   ├── topic-manager.ts  # Topic → Session mapping logic
│   └── topic-store.ts    # SQLite persistence for topic mappings
├── opencode/
│   ├── client.ts         # OpenCode REST API client
│   ├── discovery.ts      # Discover running OpenCode instances
│   ├── stream-handler.ts # SSE → Telegram message bridging
│   └── telegram-markdown.ts # Markdown conversion for Telegram
├── orchestrator/
│   ├── manager.ts        # Manages multiple instances
│   ├── instance.ts       # Single OpenCode instance lifecycle
│   ├── port-pool.ts      # Port allocation
│   └── state-store.ts    # SQLite persistence for instance state
└── types/
    ├── forum.ts          # Forum/topic types
    └── orchestrator.ts   # Orchestrator types
```

Runtime state (gitignored) lives in `data/`:

- `data/orchestrator.db` — instance state
- `data/topics.db` — topic mappings

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TELEGRAM_BOT_TOKEN` | Yes | - | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Yes | - | Supergroup ID (starts with -100) |
| `TELEGRAM_ALLOWED_USERS` | No | _(empty = all)_ | Comma-separated allowed user IDs |
| `HANDLE_GENERAL_TOPIC` | No | `true` | Whether to handle General topic messages |
| `PROJECT_BASE_PATH` | No | `~/oc-bot` | Where topic directories are created |
| `AUTO_CREATE_PROJECT_DIRS` | No | `true` | Auto-create project directories |
| `OPENCODE_PATH` | No | `opencode` | Path to opencode binary |
| `OPENCODE_MAX_INSTANCES` | No | `10` | Max concurrent instances |
| `OPENCODE_PORT_START` | No | `4100` | Starting port for instances |
| `OPENCODE_PORT_POOL_SIZE` | No | `100` | Number of ports in the pool |
| `OPENCODE_IDLE_TIMEOUT_MS` | No | `1800000` | Idle timeout (30 min) |
| `OPENCODE_HEALTH_CHECK_INTERVAL_MS` | No | `30000` | Health check interval (30 s) |
| `OPENCODE_STARTUP_TIMEOUT_MS` | No | `60000` | Instance startup timeout (60 s) |
| `ORCHESTRATOR_DB_PATH` | No | `./data/orchestrator.db` | Instance state database |
| `TOPIC_DB_PATH` | No | `./data/topics.db` | Topic mapping database |
| `API_PORT` | No | `4200` | External API server port |
| `API_KEY` | No | - | Optional API key (recommended for production) |

See [.env.example](.env.example) for all available options, including optional
webhook settings. Guided setup: `bash scripts/setup-env.sh`.

## API Reference

### External Instance API

The bot exposes an API (default port 4200) for external OpenCode instances to
register. Useful when running in Docker without process discovery, or for
attaching long-lived instances started elsewhere.

```bash
# Register an external instance
curl -X POST http://localhost:4200/api/register \
  -H "Content-Type: application/json" \
  -d '{
    "projectPath": "/path/to/project",
    "projectName": "my-project",
    "opencodePort": 4096,
    "sessionId": "ses_abc123"
  }'

# Unregister
curl -X POST http://localhost:4200/api/unregister \
  -H "Content-Type: application/json" \
  -d '{"projectPath": "/path/to/project"}'

# Check status
curl http://localhost:4200/api/status/$(echo -n "/path/to/project" | base64)

# List all instances
curl http://localhost:4200/api/instances

# Health check
curl http://localhost:4200/api/health
```

Registering creates a Telegram forum topic for the project, subscribes to the
session's SSE events, forwards responses to Telegram in real time, and routes
Telegram replies back into the session.

### OpenCode REST API (per instance)

Each OpenCode instance exposes:

```
GET  /global/health           # Health check
GET  /session                 # List sessions
POST /session                 # Create session
GET  /session/:id/message     # Get messages
POST /session/:id/message     # Send message (sync)
POST /session/:id/prompt_async # Send message (async)
GET  /event                   # SSE event stream
```

## Development

```bash
# Install dependencies
bun install

# Start with hot reload
bun run dev

# Type check
bun run typecheck

# Format code (if prettier configured)
bun run format
```

### Service Install (user-space only)

Production installs use `scripts/install.sh`, which is user-space-only: a
per-user systemd unit (`systemctl --user`), default prefix
`~/.local/share/opencode-telegram`, no root/sudo/`/opt`. The operator flow
(flags, lingering, uninstall gating, migration off old system-wide
installs) is documented in
[README.md](README.md#install-as-a-user-service); `bash
scripts/install.sh --help` is authoritative for flags. Keep exactly one
service per bot token to avoid Telegram `409 Conflict` errors.

### Key Patterns

- **Event-driven**: Orchestrator emits events, integration layer handles them
- **State recovery**: Both orchestrator and topic manager recover state on restart
- **Graceful degradation**: Errors are logged but don't crash the bot

### Adding New Features

1. **New bot commands**: Add to `src/bot/handlers/forum.ts` in `createForumCommands()`
2. **New SSE event handling**: Modify `src/opencode/stream-handler.ts`
3. **New instance lifecycle events**: Modify `src/orchestrator/instance.ts`

## Streaming and SSE Internals

Summary of how a prompt becomes streamed Telegram messages (see
`.opencode/skills/silent-stop-debug/SKILL.md` for the failure-mode map):

1. `routeMessageToInstance` (`src/integration.ts`) sends the prompt via
   `POST /session/:id/prompt_async` (fire-and-forget) and records the prompt
   per topic for restart recovery.
2. Each instance has one SSE subscription (`OpenCodeClient.subscribe` /
   `startSSE` in `src/opencode/client.ts`) over `GET /event`, with bounded
   reconnect (5 retries, 1s→15s backoff, budget reset on every received
   event). If the stream is lost for good, the affected topic gets an honest
   notice instead of silence.
3. `StreamHandler.handleEvent` (`src/opencode/stream-handler.ts`) routes
   events by session ID: `message.part.updated` drives the "Thinking..." /
   streaming progress message, `session.idle` renders the final answer,
   `session.error` deletes progress and posts the error, and
   `permission.updated` posts an approve/deny card that blocks the session
   until answered.
4. Session IDs are extracted from several envelope shapes
   (`props.sessionID`, `props.info`, `props.part`, `session.updated` info);
   events for unregistered sessions are dropped with a log line.
5. On crash/restart, `instance:ready` re-subscribes and may create a new
   session; if the previous prompt had produced activity, the topic gets a
   "resend your message" nudge — accepted prompts are never silently dropped.

## Testing Notes

- Send messages in Telegram topics to exercise the full flow.
- Watch the terminal running `bun run dev` for request, SSE, and error logs.
- Inspect `data/orchestrator.db` and `data/topics.db` for persisted state.
- After a suspected silent failure, check `/status` in General (SSE
  subscription count, instance state), then send one more message in the
  stuck topic and note whether it recovers or errors loudly.

## Docker Details

> Native Bun is recommended for full functionality (especially session
> discovery). This section covers Docker for operators who need it.

### Feature Matrix

| Feature | Native (Bun) | Docker | Docker + `--pid=host` |
|---------|--------------|--------|----------------------|
| `/new` - create managed instances | Works | Works | Works |
| `/sessions` - discover host sessions | Works | **No** | Linux only |
| `/connect` - attach to discovered sessions | Works | **No** | Linux only |
| External API registration | Works | Works | Works |
| Stream responses to Telegram | Works | Works | Works |

### When Docker Makes Sense

- You only need **managed instances** (created via `/new` command)
- You're on Linux and can use `--pid=host`
- You want to use the **External API** to manually register instances

### Build the Image

```bash
docker build -t opencode-telegram .
```

### Option 1: Managed Instances Only

```bash
docker run -d --name opencode-telegram \
  --network=host \
  -v $(pwd)/data:/app/data \
  -v ~/oc-bot:/root/oc-bot \
  --env-file .env \
  opencode-telegram
```

**Volume Mounts:**
| Mount | Purpose |
|-------|---------|
| `./data:/app/data` | SQLite databases for persistent state |
| `~/oc-bot:/root/oc-bot` | Project directories created by `/new` command |

### Option 2: With Discovery (Linux Only)

```bash
docker run -d --name opencode-telegram \
  --network=host \
  --pid=host \
  -v $(pwd)/data:/app/data \
  -v ~/oc-bot:/root/oc-bot \
  --env-file .env \
  opencode-telegram
```

> **Warning**: `--pid=host` shares the host's process namespace with the container. The container can see all host processes.

### Option 3: External API Registration

Start the bot as in Option 1, then register host instances (see
[External Instance API](#external-instance-api)).

### macOS/Windows Note

On macOS and Windows, Docker Desktop runs containers in a Linux VM:
- `--network=host` doesn't provide true host networking
- `--pid=host` is not available
- **Discovery will not work** - use native Bun or the External API

### Docker Compose

```yaml
version: '3.8'

services:
  opencode-telegram:
    build: .
    container_name: opencode-telegram
    network_mode: host
    # Uncomment for discovery (Linux only):
    # pid: host
    volumes:
      - ./data:/app/data
      - ~/oc-bot:/root/oc-bot
    env_file:
      - .env
    restart: unless-stopped
```

```bash
docker compose up -d
docker compose logs -f
```

### Useful Docker Commands

```bash
# View logs
docker logs -f opencode-telegram

# Check container status
docker ps -a --filter name=opencode-telegram

# Stop the bot
docker stop opencode-telegram

# Remove container
docker rm opencode-telegram

# Rebuild after code changes
docker build -t opencode-telegram . && docker compose up -d
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.
