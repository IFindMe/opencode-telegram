#!/usr/bin/env bash
#
# install.sh — install the opencode-telegram bot as a systemd service so it
# runs on boot without keeping a terminal (or this repo checkout) open.
#
# What it does:
#   1. Copies the runtime files (src/, package.json, bun.lock*, scripts/,
#      .env.example, README.md, LICENSE) from this repo into an install
#      prefix OUTSIDE the checkout, so the installed copy survives
#      independent of the source repo.
#   2. Runs `bun install` in the installed copy and creates its data/ dir.
#   3. Provides the installed copy's .env (copies --env-from, defaulting to
#      this repo's .env when present; otherwise runs the installed copy's
#      scripts/setup-env.sh interactively).
#   4. Writes a systemd unit (user service by default) and enables+starts it.
#
# Modes:
#   user   (default) — `systemctl --user` unit in
#                       ~/.config/systemd/user/, no root needed.
#   system (--system) — unit in /etc/systemd/system/, requires root/sudo and
#                       --user NAME (the account the service runs as).
#
# Examples:
#   bash scripts/install.sh
#   bash scripts/install.sh --dry-run
#   bash scripts/install.sh --prefix /tmp/oc-tg-test --dry-run
#   bash scripts/install.sh --system --user alice
#   bash scripts/install.sh --env-from /path/to/.env --non-interactive
#   bash scripts/install.sh --uninstall
#   bash scripts/install.sh --system --uninstall --yes
#
# Re-running is safe (idempotent): the existing unit and .env are backed up
# with a timestamp suffix before being replaced. Secrets are never printed
# (a token is shown only as its last 4 characters, like setup-env.sh).

set -euo pipefail

START_DIR="$PWD"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SERVICE_NAME="opencode-telegram.service"
INSTALL_SCRIPT_ABS="$REPO_ROOT/scripts/install.sh"

MODE="user"
PREFIX=""
ENV_FROM=""
ENV_FROM_GIVEN="false"
SYSTEM_USER=""
NON_INTERACTIVE="false"
UNINSTALL="false"
DRY_RUN="false"
ASSUME_YES="false"
SHOW_HELP="false"
ORIG_ARGS=("$@")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() { printf '%s\n' "$*"; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# run CMD... — execute the command, or only print it under --dry-run.
run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[dry-run] would run:'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

# run_soft CMD... — like run(), but a real-run failure only warns.
run_soft() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[dry-run] would run (failure tolerated):'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@" || warn "command failed, continuing: $*"
  fi
}

# mask_secret VALUE — render only the last 4 chars (mirrors setup-env.sh).
mask_secret() {
  local secret="${1:-}"
  if [[ -z "$secret" ]]; then
    printf '(empty)'
  else
    printf '...%s' "${secret: -4}"
  fi
}

# env_get FILE KEY — print KEY's raw value from an env-style file without
# executing it. Prints empty string when key/file is missing; always exits 0.
env_get() {
  local file="$1" key="$2" line=""
  line=$(grep -E "^${key}=" "$file" 2>/dev/null | tail -n 1 || true)
  line=${line#"${key}="}
  printf '%s' "$line"
}

expand_tilde() {
  local p="$1"
  # Intentional literal "~" comparisons below; expansion is manual.
  # shellcheck disable=SC2088
  if [[ "$p" == "~" ]]; then
    printf '%s' "$HOME"
  elif [[ "$p" == "~/"* ]]; then
    printf '%s' "${HOME}/${p#"~/"}"
  else
    printf '%s' "$p"
  fi
}

usage() {
  cat <<'EOF'
Usage: scripts/install.sh [OPTIONS]

Install the opencode-telegram bot as a systemd service (user service by
default), from an independent copy outside this repo checkout.

Options:
  --system            Install a system-wide unit in /etc/systemd/system
                      (requires root/sudo and --user NAME). Default is a
                      per-user unit (systemctl --user), no root needed.
  --user NAME         Account the system-wide service runs as (User=/Group=
                      in the unit). Required with --system; ignored otherwise.
  --prefix PATH       Install prefix (must be absolute). Defaults:
                      user mode:   $HOME/.local/share/opencode-telegram
                      system mode: /opt/opencode-telegram
  --env-from PATH     Copy this .env file to <prefix>/.env (mode 600).
                      Default: this repo's .env, when present. Otherwise the
                      installed copy's scripts/setup-env.sh runs
                      interactively (unless --non-interactive, which aborts).
  --non-interactive   Never prompt. Aborts instead of running setup-env.sh
                      when no .env source exists, and skips the lingering
                      offer (prints the manual command instead).
  --uninstall         Stop+disable the service and remove its unit file.
                      Installed files (<prefix>, incl. data/) are kept unless
                      --yes is ALSO given AND deletion is confirmed.
  --yes, -y           With --uninstall: allow deleting <prefix> (still asks
                      for typed DELETE confirmation unless --non-interactive
                      or --dry-run). Without --yes nothing is ever deleted.
  --dry-run           Print every mutation that would happen; change nothing.
  --help, -h          Show this help and exit.

Examples:
  bash scripts/install.sh
  bash scripts/install.sh --dry-run
  bash scripts/install.sh --system --user alice
  bash scripts/install.sh --env-from /path/to/.env --non-interactive
  bash scripts/install.sh --uninstall
  bash scripts/install.sh --system --uninstall --yes
EOF
}

# resolve_bun — print an absolute path to the bun binary, or fail.
# Tries PATH first, then common install locations, then (under sudo) the
# invoking user's bun home. Read-only: safe under --dry-run.
resolve_bun() {
  local found="" cand="" home_dir=""
  if found="$(command -v bun 2>/dev/null || true)"; then
    if [[ -n "$found" && "$found" == */* ]]; then
      printf '%s' "$found"
      return 0
    fi
  fi
  for cand in "$HOME/.bun/bin/bun" "/usr/local/bin/bun" "/opt/bun/bin/bun"; do
    if [[ -x "$cand" ]]; then
      printf '%s' "$cand"
      return 0
    fi
  done
  if [[ -n "${SUDO_USER:-}" && "${SUDO_USER:-}" != "root" ]]; then
    home_dir="$(getent passwd "$SUDO_USER" 2>/dev/null | cut -d: -f6 || true)"
    if [[ -n "$home_dir" && -x "$home_dir/.bun/bin/bun" ]]; then
      printf '%s' "$home_dir/.bun/bin/bun"
      return 0
    fi
  fi
  return 1
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case "$1" in
    --system) MODE="system"; shift ;;
    --prefix)
      [[ $# -ge 2 ]] || die "--prefix needs a PATH argument (see --help)."
      [[ -n "${2:-}" ]] || die "--prefix needs a non-empty PATH argument."
      PREFIX="$(expand_tilde "$2")"; shift 2 ;;
    --prefix=*)
      [[ -n "${1#--prefix=}" ]] || die "--prefix needs a non-empty PATH argument."
      PREFIX="$(expand_tilde "${1#--prefix=}")"; shift ;;
    --env-from)
      [[ $# -ge 2 ]] || die "--env-from needs a PATH argument (see --help)."
      [[ -n "${2:-}" ]] || die "--env-from needs a non-empty PATH argument."
      ENV_FROM="$2"; ENV_FROM_GIVEN="true"; shift 2 ;;
    --env-from=*)
      [[ -n "${1#--env-from=}" ]] || die "--env-from needs a non-empty PATH argument."
      ENV_FROM="${1#--env-from=}"; ENV_FROM_GIVEN="true"; shift ;;
    --user)
      [[ $# -ge 2 ]] || die "--user needs a NAME argument (see --help)."
      SYSTEM_USER="$2"; shift 2 ;;
    --user=*)
      SYSTEM_USER="${1#--user=}"; shift ;;
    --non-interactive) NON_INTERACTIVE="true"; shift ;;
    --uninstall) UNINSTALL="true"; shift ;;
    --dry-run) DRY_RUN="true"; shift ;;
    --yes|-y) ASSUME_YES="true"; shift ;;
    --help|-h) SHOW_HELP="true"; shift ;;
    --) shift; break ;;
    -*) die "Unknown flag: $1 (see --help)." ;;
    *) die "Unexpected argument: $1 (see --help)." ;;
  esac
done

if [[ "$SHOW_HELP" == "true" ]]; then
  usage
  exit 0
fi

if [[ -n "$PREFIX" && "$PREFIX" != /* ]]; then
  die "--prefix must be an absolute path (got: $PREFIX)."
fi

# Resolve a relative --env-from against the invoking directory (we cd'd to
# the repo root above).
if [[ -n "$ENV_FROM" && "$ENV_FROM" != /* ]]; then
  ENV_FROM="$START_DIR/$ENV_FROM"
fi

# ---------------------------------------------------------------------------
# Mode setup
# ---------------------------------------------------------------------------

UNIT_PATH=""
WANTED_BY=""
SYSCTL=()
JCTL=()

if [[ "$MODE" == "system" ]]; then
  [[ -n "$PREFIX" ]] || PREFIX="/opt/opencode-telegram"
  UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}"
  WANTED_BY="multi-user.target"
  SYSCTL=(systemctl)
  JCTL=(journalctl)
  [[ -n "$SYSTEM_USER" ]] || die "--system mode requires --user NAME (the account the service runs as)."
  id "$SYSTEM_USER" >/dev/null 2>&1 || die "User '$SYSTEM_USER' does not exist."
  if [[ -n "${SUDO_USER:-}" || "${EUID:-$(id -u)}" -eq 0 ]]; then
    : # already root (or root-adjacent) — proceed
  else
    if [[ "$DRY_RUN" == "true" ]]; then
      warn "--system mode needs root; [dry-run] skipping the sudo re-exec (a real run would re-exec via sudo)."
    elif command -v sudo >/dev/null 2>&1; then
      log "Re-executing via sudo for the system-wide install..."
      # shellcheck disable=SC2096
      exec sudo "$INSTALL_SCRIPT_ABS" "${ORIG_ARGS[@]}"
    else
      die "--system mode requires root. Re-run with sudo/as root, or sudo is not installed."
    fi
  fi
else
  [[ -n "$PREFIX" ]] || PREFIX="$HOME/.local/share/opencode-telegram"
  UNIT_PATH="$HOME/.config/systemd/user/${SERVICE_NAME}"
  WANTED_BY="default.target"
  SYSCTL=(systemctl --user)
  JCTL=(journalctl --user)
  if [[ -n "$SYSTEM_USER" ]]; then
    warn "Ignoring --user '$SYSTEM_USER' (only meaningful with --system)."
  fi
fi

if [[ "$ENV_FROM_GIVEN" == "true" ]]; then
  [[ -f "$ENV_FROM" ]] || die "--env-from file not found: $ENV_FROM"
elif [[ -f "$REPO_ROOT/.env" ]]; then
  ENV_FROM="$REPO_ROOT/.env" # default: this repo's .env, when present
fi

if [[ "$ENV_FROM_GIVEN" == "true" && "$UNINSTALL" == "true" ]]; then
  warn "Ignoring --env-from (not used with --uninstall)."
fi

HAVE_RSYNC="false"
if command -v rsync >/dev/null 2>&1; then
  HAVE_RSYNC="true"
fi

# ---------------------------------------------------------------------------
# Uninstall path
# ---------------------------------------------------------------------------

do_uninstall() {
  local stamp=""
  stamp="$(date +%Y%m%d-%H%M%S)"
  log "Uninstalling ${SERVICE_NAME} (${MODE} mode)..."
  log "  unit:   $UNIT_PATH"
  log "  prefix: $PREFIX"

  run_soft "${SYSCTL[@]}" stop "$SERVICE_NAME"
  run_soft "${SYSCTL[@]}" disable "$SERVICE_NAME"

  if [[ -f "$UNIT_PATH" ]]; then
    run cp -p "$UNIT_PATH" "${UNIT_PATH}.bak.${stamp}"
    run rm -f "$UNIT_PATH"
  else
    log "No unit file at $UNIT_PATH — nothing to remove."
  fi
  run "${SYSCTL[@]}" daemon-reload

  if [[ "$ASSUME_YES" != "true" ]]; then
    log "Keeping installed files at: $PREFIX"
    log "To also delete them (including data/), re-run with --uninstall --yes"
    log "(you will still be asked to type DELETE to confirm)."
    return 0
  fi

  # --yes given: still require typed confirmation, unless --non-interactive
  # (explicit --yes alone is then enough) or --dry-run (prints only).
  if [[ -z "$PREFIX" || "$PREFIX" == "/" || "$PREFIX" == "$HOME" ]]; then
    die "Refusing to delete unsafe prefix: '${PREFIX:-}'."
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[dry-run] would ask for typed DELETE confirmation, then run: rm -rf -- %q\n' "$PREFIX"
    return 0
  fi
  if [[ "$NON_INTERACTIVE" != "true" ]]; then
    local confirm=""
    read -r -p "Type DELETE to permanently delete ${PREFIX} (including data/): " confirm || confirm=""
    [[ "$confirm" == "DELETE" ]] || die "Aborted — installed files kept at $PREFIX."
  fi
  if [[ -e "$PREFIX" ]]; then
    run rm -rf -- "$PREFIX"
    log "Deleted $PREFIX."
  else
    log "Nothing to delete: $PREFIX does not exist."
  fi
}

if [[ "$UNINSTALL" == "true" ]]; then
  do_uninstall
  exit 0
fi

# ---------------------------------------------------------------------------
# Install path
# ---------------------------------------------------------------------------

# 1. bun must exist (absolute path goes into ExecStart).
BUN_BIN=""
BUN_BIN="$(resolve_bun || true)"
if [[ -z "$BUN_BIN" ]]; then
  die "bun not found. Install it (https://bun.sh — curl -fsSL https://bun.sh/install | bash), ensure 'bun' is on PATH, then re-run."
fi
log "Using bun: $BUN_BIN"

# 2. Preflight: required sources must exist in this repo.
for item in src package.json scripts scripts/setup-env.sh .env.example; do
  [[ -e "$REPO_ROOT/$item" ]] || die "Repo is missing required '${item}' — aborting."
done

# copy_item SRC DST [required|optional] — copy one allow-listed runtime item
# with rsync when present, else cp -a. No --delete: reinstalls must not wipe
# the installed copy's .env or data/.
copy_item() {
  local src="$1" dst="$2" req="${3:-required}" dstdir=""
  if [[ ! -e "$src" ]]; then
    if [[ "$req" == "optional" ]]; then
      warn "Source '${src}' missing — skipping."
      return 0
    fi
    die "Required source '${src}' missing — aborting."
  fi
  if [[ "$HAVE_RSYNC" == "true" ]]; then
    if [[ -d "$src" ]]; then
      run rsync -a "$src/" "$dst/"
    else
      dstdir="$(dirname "$dst")"
      run mkdir -p "$dstdir"
      run rsync -a "$src" "$dst"
    fi
  else
    if [[ -d "$src" ]]; then
      run mkdir -p "$dst"
      run cp -a "$src/." "$dst/"
    else
      dstdir="$(dirname "$dst")"
      run mkdir -p "$dstdir"
      run cp -a "$src" "$dst"
    fi
  fi
}

copy_runtime_files() {
  log "Copying runtime files: $REPO_ROOT -> $PREFIX"
  if [[ "$HAVE_RSYNC" != "true" ]]; then
    warn "rsync not found — falling back to cp -a."
  fi
  run mkdir -p "$PREFIX"
  copy_item "$REPO_ROOT/src" "$PREFIX/src"
  copy_item "$REPO_ROOT/scripts" "$PREFIX/scripts"
  copy_item "$REPO_ROOT/package.json" "$PREFIX/package.json"
  copy_item "$REPO_ROOT/.env.example" "$PREFIX/.env.example"
  copy_item "$REPO_ROOT/README.md" "$PREFIX/README.md" optional
  copy_item "$REPO_ROOT/LICENSE" "$PREFIX/LICENSE" optional
  local lock_matches=("$REPO_ROOT"/bun.lock*)
  if [[ -e "${lock_matches[0]}" ]]; then
    local lf="" base=""
    for lf in "${lock_matches[@]}"; do
      base="$(basename "$lf")"
      copy_item "$lf" "$PREFIX/$base" optional
    done
  else
    warn "No bun.lock* found — 'bun install' will resolve without a lockfile."
  fi
}

install_deps() {
  run mkdir -p "$PREFIX/data"
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[dry-run] would run: (cd %q && %q install --frozen-lockfile; on failure, warn and retry with --no-frozen-lockfile)\n' \
      "$PREFIX" "$BUN_BIN"
    return 0
  fi
  log "Installing dependencies in $PREFIX ..."
  if (cd "$PREFIX" && "$BUN_BIN" install --frozen-lockfile); then
    log "Dependencies installed (frozen lockfile)."
  else
    warn "'bun install --frozen-lockfile' failed — retrying with --no-frozen-lockfile."
    (cd "$PREFIX" && "$BUN_BIN" install --no-frozen-lockfile)
  fi
}

configure_env() {
  local target_env="$PREFIX/.env" stamp="" tok=""
  if [[ -n "$ENV_FROM" ]]; then
    log "Using env source: $ENV_FROM"
    tok="$(env_get "$ENV_FROM" "TELEGRAM_BOT_TOKEN")"
    log "  TELEGRAM_BOT_TOKEN in source ends with: $(mask_secret "$tok")"
    if [[ -f "$target_env" ]]; then
      if [[ "$ENV_FROM" -ef "$target_env" ]]; then
        log "Env source is already $target_env — nothing to copy."
        run chmod 600 "$target_env"
        return 0
      fi
      stamp="$(date +%Y%m%d-%H%M%S)"
      run cp -p "$target_env" "${target_env}.bak.${stamp}"
    fi
    run cp -p "$ENV_FROM" "$target_env"
    run chmod 600 "$target_env"
    return 0
  fi
  if [[ -f "$target_env" ]]; then
    log "Keeping existing $target_env (re-run safe; edit it or re-run setup-env.sh to change values)."
    return 0
  fi
  if [[ "$NON_INTERACTIVE" == "true" ]]; then
    die "No .env source available for a non-interactive install. Provide --env-from PATH, or run 'bash $PREFIX/scripts/setup-env.sh' first."
  fi
  log "No .env source found — launching the installed copy's setup-env.sh ..."
  run bash "$PREFIX/scripts/setup-env.sh"
}

fix_ownership() {
  # System mode runs everything above as root, leaving PREFIX (incl. data/
  # and .env) root-owned. The service runs as $SYSTEM_USER, which then cannot
  # write data/*.db — SQLite fails with "unable to open database file".
  # Hand the whole tree to the service account (no-op in user mode).
  if [[ "$MODE" == "system" ]]; then
    local grp=""
    grp="$(id -gn "$SYSTEM_USER")"
    log "Setting ownership of $PREFIX to ${SYSTEM_USER}:${grp} ..."
    run chown -R "${SYSTEM_USER}:${grp}" "$PREFIX"
  fi
}

write_unit() {
  local unit_dir="" stamp="" content="" grp=""
  unit_dir="$(dirname "$UNIT_PATH")"
  stamp="$(date +%Y%m%d-%H%M%S)"
  if [[ "$MODE" == "system" ]]; then
    grp="$(id -gn "$SYSTEM_USER")"
  fi

  content="# Generated by scripts/install.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)."
  content+=$'\n'"# Re-running scripts/install.sh is safe (this file is backed up first)."
  content+=$'\n'"[Unit]"
  content+=$'\n'"Description=OpenCode Telegram bot"
  content+=$'\n'"After=network-online.target"
  content+=$'\n'"Wants=network-online.target"
  content+=$'\n'""
  content+=$'\n'"[Service]"
  content+=$'\n'"Type=simple"
  content+=$'\n'"WorkingDirectory=${PREFIX}"
  content+=$'\n'"EnvironmentFile=${PREFIX}/.env"
  content+=$'\n'"ExecStart=${BUN_BIN} run start"
  content+=$'\n'"Restart=always"
  content+=$'\n'"RestartSec=5"
  content+=$'\n'"NoNewPrivileges=true"
  if [[ "$MODE" == "system" ]]; then
    content+=$'\n'"User=${SYSTEM_USER}"
    content+=$'\n'"Group=${grp}"
  fi
  content+=$'\n'""
  content+=$'\n'"[Install]"
  content+=$'\n'"WantedBy=${WANTED_BY}"
  content+=$'\n'

  run mkdir -p "$unit_dir"
  if [[ -f "$UNIT_PATH" ]]; then
    run cp -p "$UNIT_PATH" "${UNIT_PATH}.bak.${stamp}"
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[dry-run] would write %s with content:\n%s\n' "$UNIT_PATH" "$content"
  else
    printf '%s' "$content" >"$UNIT_PATH"
    chmod 644 "$UNIT_PATH"
    log "Wrote $UNIT_PATH"
  fi
}

enable_and_check() {
  local me="" linger_out="" ans="" api_port="" env_for_hint=""
  run "${SYSCTL[@]}" daemon-reload
  run "${SYSCTL[@]}" enable --now "$SERVICE_NAME"

  if [[ "$MODE" == "user" ]]; then
    me="${USER:-$(id -un)}"
    linger_out=""
    if command -v loginctl >/dev/null 2>&1; then
      linger_out="$(loginctl show-user "$me" -p Linger 2>/dev/null || true)"
    fi
    if [[ "$linger_out" != *"=yes"* ]]; then
      warn "User lingering is not enabled (${linger_out:-unknown}) — the user service may not start on boot until you log in once."
      if [[ "$DRY_RUN" == "true" ]]; then
        printf '[dry-run] would offer: sudo loginctl enable-linger %s\n' "$me"
      elif [[ "$NON_INTERACTIVE" == "true" ]]; then
        log "Non-interactive: enable lingering manually: sudo loginctl enable-linger $me"
      else
        ans=""
        read -r -p "Enable lingering now via 'sudo loginctl enable-linger $me'? [y/N]: " ans || ans=""
        if [[ "$ans" =~ ^[Yy]$ ]]; then
          run sudo loginctl enable-linger "$me"
        else
          log "Skipped. Enable later with: sudo loginctl enable-linger $me"
        fi
      fi
    else
      log "User lingering is enabled — the service will start on boot."
    fi
    log "Service start was requested now regardless of lingering."
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[dry-run] would check:'
    printf ' %q' "${SYSCTL[@]}" is-active "$SERVICE_NAME"
    printf '\n'
  elif "${SYSCTL[@]}" is-active --quiet "$SERVICE_NAME"; then
    log "Service is active."
  else
    warn "Service is not active yet — inspect with: ${JCTL[*]} -u $SERVICE_NAME -e --no-pager (or -f to follow)."
  fi

  env_for_hint="$PREFIX/.env"
  if [[ ! -f "$env_for_hint" && -n "$ENV_FROM" && -f "$ENV_FROM" ]]; then
    env_for_hint="$ENV_FROM"
  fi
  api_port="$(env_get "$env_for_hint" "API_PORT")"
  [[ -n "$api_port" ]] || api_port="4200"
  log "Follow logs:   ${JCTL[*]} -u $SERVICE_NAME -f"
  log "Health check:  curl -s http://localhost:${api_port}/api/health"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

copy_runtime_files
install_deps
configure_env
fix_ownership
write_unit
enable_and_check

log ""
log "Done. Mode: ${MODE}. Prefix: ${PREFIX}. Unit: ${UNIT_PATH}."
