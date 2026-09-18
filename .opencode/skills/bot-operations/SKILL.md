---
name: bot-operations
description: Operate the opencode-telegram bot service. Start, stop, and restart the bot (bun run dev, systemd unit, Docker); read logs (data/bot.log, journalctl, docker logs, tmux pane); health-check the external API on port 4200; manage environment via scripts/setup-env.sh; install or uninstall the systemd service via scripts/install.sh; detect stale or duplicate bot processes (Telegram 409 conflicts) and OpenCode port usage from 4100 up. Use for any run-state, deploy, restart, or "is the bot alive" task in this repo.
---

# Bot Operations

Run-state operations for the opencode-telegram bot in this repo. Paths below
are relative to the repo root. For user-facing setup, see `README.md`; for
internals, see `DEV.md`.

## Start / stop / restart

Dev (hot reload) and production:

```bash
bun run dev      # hot reload, logs to terminal
bun run start    # production
```

systemd service (installed via `scripts/install.sh`, unit
`opencode-telegram.service`):

```bash
systemctl --user status opencode-telegram.service
systemctl --user restart opencode-telegram.service
systemctl --user stop opencode-telegram.service
# User-space only: no --system mode, no sudo; the unit lives under ~/.config/systemd/user/.
```

Docker:

```bash
docker stop opencode-telegram && docker start opencode-telegram
docker compose up -d   # recreate after code changes + rebuild
```

tmux dev setup (window `telegram-exp`, bot pane `%368` — see `AGENTS.md`):

```bash
tmux capture-pane -t %368 -p -S -100   # read recent bot logs
tmux send-keys -t %368 C-c             # stop
tmux send-keys -t %368 'bun run dev' Enter   # start
```

## Read logs

Pick the source that matches how the bot runs:

- File mode: `data/bot.log` (present only when file logging is enabled).
- systemd: `journalctl --user -u opencode-telegram.service -f`.
- Docker: `docker logs -f opencode-telegram` or `docker compose logs -f`.
- Dev/tmux: terminal output, or `tmux capture-pane -t %368 -p -S -200`.

## Health check

```bash
curl -s http://localhost:4200/api/health   # 4200 unless API_PORT is set
systemctl --user is-active opencode-telegram.service
```

A healthy bot answers the API health endpoint and shows active instances via
`/status` in the Telegram General topic.

## Environment and service install

- `bash scripts/setup-env.sh` — guided env setup; validates input, writes
  `.env` with restricted permissions; safe to re-run (backs up existing `.env`).
- `bash scripts/install.sh --dry-run` — preview installer actions first.
- `bash scripts/install.sh` — user-space-only install (default prefix
  `~/.local/share/opencode-telegram`; no root, no sudo, no `/opt`).
- `bash scripts/install.sh --help` — all flags (`--prefix`, `--env-from`,
  `--non-interactive`, `--uninstall`/`--yes`, `--dry-run`, `--allow-root`).
  Old `--system` / `--user NAME` flags abort — re-run without them.
- `bash scripts/install.sh --uninstall` — remove unit (keeps installed files
  unless `--yes` plus typed `DELETE` confirmation).
- Migrating off an old system-wide install: keep exactly ONE service per
  token (duplicates cause `409 Conflict`s). Fresh user-space install first,
  then `sudo systemctl disable --now opencode-telegram.service`,
  `sudo rm -f /etc/systemd/system/opencode-telegram.service && sudo systemctl daemon-reload`,
  `sudo rm -rf /opt/opencode-telegram` (deletes its data/ — back up first).

## Stale / duplicate processes

Two bot copies polling Telegram cause `409 Conflict` errors and flapping
behavior. Exactly one bot owner must exist per token:

```bash
ps aux | grep -E "bun|opencode-telegram" | grep -v grep
lsof -ti:4200   # API port holder should be the live bot
```

OpenCode instances occupy ports from `4100` up (`OPENCODE_PORT_START`,
pool size `OPENCODE_PORT_POOL_SIZE`). A stale holder blocks restarts:

```bash
lsof -ti:4100 | xargs kill   # substitute the conflicting port
```

The bot auto-cleans stale instance ports on start; manual cleanup is the
fallback. Never kill a port without checking `ps` first — it may belong to
an unrelated OpenCode session you can instead attach via `/connect`.
