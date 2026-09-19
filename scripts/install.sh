#!/usr/bin/env bash
#
# install.sh — install the opencode-telegram bot as a per-user systemd service
# so it runs on boot without keeping a terminal (or this repo checkout) open.
#
# USER-SPACE ONLY: this installer never needs root and never uses sudo for
# the install itself. Everything lives under your own account and the service
# runs via `systemctl --user` as that same account — so installed files
# (including data/*.db) are always writable by the service, by construction.
# There is no system-wide mode: a `--system` flag from an older version is
# rejected with an error (see argument parsing below).
#
# What it does:
#   1. Copies the runtime files (src/, package.json, bun.lock*, scripts/,
#      .env.example, README.md, LICENSE) from this repo into an install
#      prefix OUTSIDE the checkout (default
#      $HOME/.local/share/opencode-telegram), so the installed copy survives
#      independent of the source repo.
#   2. Runs `bun install` in the installed copy and creates its data/ dir.
#   3. Provides the installed copy's .env (copies --env-from, defaulting to
#      this repo's .env when present; otherwise runs the installed copy's
#      scripts/setup-env.sh interactively).
#   4. Writes a per-user systemd unit in ~/.config/systemd/user/ and
#      enables+starts it via `systemctl --user`.
#
# Examples:
#   bash scripts/install.sh
#   bash scripts/install.sh --dry-run
#   bash scripts/install.sh --prefix /tmp/oc-tg-test --dry-run
#   bash scripts/install.sh --env-from /path/to/.env --non-interactive
#   bash scripts/install.sh --uninstall
#   bash scripts/install.sh --uninstall --yes
#
# Re-running is safe (idempotent): the existing unit and .env are backed up
# with a timestamp suffix before being replaced. Secrets are never printed
# (a token is shown only as its last 4 characters, like setup-env.sh).
#
# Coming from an OLD system-wide install? This script only manages the
# per-user unit/prefix above. On uninstall it points out stale root-owned
# leftovers (old /opt prefix, old /etc unit) with the exact command you can
# run yourself to remove them — it never uses sudo on your behalf.

set -euo pipefail

START_DIR="$PWD"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SERVICE_NAME="opencode-telegram.service"

# Leftovers from pre-user-space-only (system-mode) installs. This script
# never writes, deletes, or chowns these paths — it only reports them.
LEGACY_SYSTEM_UNIT="/etc/systemd/system/${SERVICE_NAME}"
LEGACY_SYSTEM_PREFIX="/opt/opencode-telegram"

PREFIX=""
ENV_FROM=""
ENV_FROM_GIVEN="false"
NON_INTERACTIVE="false"
UNINSTALL="false"
DRY_RUN="false"
ASSUME_YES="false"
SHOW_HELP="false"
ALLOW_ROOT="false"
FORCE="false"

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

Install the opencode-telegram bot as a per-user systemd service
(systemctl --user), from an independent copy outside this repo checkout.
User-space-only: no root, no sudo, no system-wide unit. The service runs as
your own account, so it can always write its own files (incl. data/*.db).

Options:
  --prefix PATH       Install prefix (must be absolute).
                      Default: $HOME/.local/share/opencode-telegram
  --env-from PATH     Copy this .env file to <prefix>/.env (mode 600).
                      Default: this repo's .env, when present. Otherwise the
                      installed copy's scripts/setup-env.sh runs
                      interactively (unless --non-interactive, which aborts).
  --non-interactive   Never prompt. Aborts instead of running setup-env.sh
                      when no .env source exists, and skips the lingering
                      offer (prints the manual command instead).
  --uninstall         Stop+disable the user service and remove its unit file.
                      Installed files (<prefix>, incl. data/) are kept unless
                      --yes is ALSO given AND deletion is confirmed.
  --yes, -y           With --uninstall: allow deleting <prefix> (still asks
                      for typed DELETE confirmation unless --non-interactive
                      or --dry-run). Without --yes nothing is ever deleted.
  --dry-run           Print every mutation that would happen; change nothing.
  --allow-root        Permit running as root (containers without a login
                        user only). Without it, running as root aborts: a
                        user-space install into /root is almost never wanted.
  --force               Install even if a legacy system-wide unit
                        (/etc/systemd/system/opencode-telegram.service, or an
                        active/enabled system instance) is still present.
                        Without --force the installer aborts there: two bots
                        on one Telegram token fight (409 conflicts, stolen
                        updates, lost messages). Remove the old unit first
                        (the abort message gives the exact commands); pass
                        --force only once it is stopped, disabled, removed.
  --help, -h          Show this help and exit.

Removed flags (abort with an error if passed):
  --system, --user NAME
                      System-wide installs were removed. Your service runs
                      via `systemctl --user` as your own account — just
                      re-run without these flags.

Examples:
  bash scripts/install.sh
  bash scripts/install.sh --dry-run
  bash scripts/install.sh --env-from /path/to/.env --non-interactive
  bash scripts/install.sh --uninstall
  bash scripts/install.sh --uninstall --yes
EOF
}

# resolve_bun — print an absolute path to the bun binary, or fail.
# Tries PATH first, then common install locations.
# Read-only: safe under --dry-run.
resolve_bun() {
  local found="" cand=""
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
  return 1
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case "$1" in
    --system)
      die "system mode removed: this installer is user-space-only; your service runs via \`systemctl --user\` as your own account. Re-run without --system." ;;
    --user)
      die "--user NAME was removed with system mode: this installer is user-space-only and runs as your own account. Re-run without --user." ;;
    --user=*)
      die "--user NAME was removed with system mode: this installer is user-space-only and runs as your own account. Re-run without --user." ;;
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
    --non-interactive) NON_INTERACTIVE="true"; shift ;;
    --uninstall) UNINSTALL="true"; shift ;;
    --dry-run) DRY_RUN="true"; shift ;;
    --yes|-y) ASSUME_YES="true"; shift ;;
    --allow-root) ALLOW_ROOT="true"; shift ;;
    --force) FORCE="true"; shift ;;
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

# A user-space installer run as root would install into /root and recreate
# exactly the account-confusion this script exists to avoid. Fail fast,
# unless explicitly overridden for containers without a login user.
if [[ "${EUID:-$(id -u)}" -eq 0 && "$ALLOW_ROOT" != "true" ]]; then
  die "Refusing to run as root: this installer is user-space-only and would install into /root. Re-run as your normal user (the account that will run the service via 'systemctl --user'). Containers without a login user may re-run with --allow-root."
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
# User-space layout (the only flow — no system mode, no root paths)
# ---------------------------------------------------------------------------

[[ -n "$PREFIX" ]] || PREFIX="$HOME/.local/share/opencode-telegram"
UNIT_PATH="$HOME/.config/systemd/user/${SERVICE_NAME}"
WANTED_BY="default.target"
SYSCTL=(systemctl --user)
JCTL=(journalctl --user)

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

# note_stale_leftover PATH REMEDIATION — report a leftover from a
# pre-user-space-only (system-mode) install. Never touches it and never uses
# sudo: if it is removable as this user we say so (and still leave it in
# place — deletion gating applies to --prefix only); otherwise we print the
# exact one-line command the user can run themselves, then continue.
note_stale_leftover() {
  local path="$1" remediation="$2" self_rm="rm -f"
  if [[ ! -e "$path" ]]; then
    return 0
  fi
  if [[ -d "$path" ]]; then
    self_rm="rm -rf"
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[dry-run] would check stale leftover (read-only): %s\n' "$path"
    return 0
  fi
  if [[ -w "$path" && -w "$(dirname "$path")" ]]; then
    log "Leftover from an old install exists (left in place): $path"
    log "  It is removable as '$(id -un)' — delete it yourself if unneeded: $self_rm -- $path"
  else
    warn "Stale leftover from an old system-mode install is not removable as '$(id -un)': $path"
    log "  To remove it yourself, run: $remediation"
  fi
}

# legacy_system_present — read-only probe for a legacy system-wide install.
# Exits 0 when the old system unit file exists at $LEGACY_SYSTEM_UNIT or a
# system instance is active/enabled; exits 1 otherwise. Never uses sudo and
# never touches /etc or /opt: `test -e` needs no privilege, and `systemctl
# is-active`/`is-enabled` (system scope, no --user) are read-only queries.
legacy_system_present() {
  if [[ -e "$LEGACY_SYSTEM_UNIT" ]]; then
    return 0
  fi
  if command -v systemctl >/dev/null 2>&1; then
    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
      return 0
    fi
    if systemctl is-enabled --quiet "$SERVICE_NAME" 2>/dev/null; then
      return 0
    fi
  fi
  return 1
}

# guard_legacy_system_unit — preflight abort when a legacy system-wide unit
# is still present. Installing the per-user bot while the old system bot is
# still around would run TWO bots on the same Telegram token (Telegram serves
# getUpdates to one poller, so the bots fight: HTTP 409 conflicts, stolen
# updates, vanishing messages) — so abort unless --force is given. With
# --force, fall through to the existing read-only stale-leftover report and
# continue, nothing more. Read-only either way: never sudo, never touches
# /etc or /opt.
guard_legacy_system_unit() {
  local present="false"
  if legacy_system_present; then
    present="true"
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[dry-run] would check legacy system unit (read-only): %s\n' "$LEGACY_SYSTEM_UNIT"
    printf '[dry-run] would check legacy system state (read-only): systemctl is-active/is-enabled %s\n' "$SERVICE_NAME"
    if [[ "$present" == "true" ]]; then
      if [[ "$FORCE" == "true" ]]; then
        printf '[dry-run] legacy system unit detected — would warn (read-only report) but proceed (--force given).\n'
      else
        printf '[dry-run] legacy system unit detected — would abort (re-run with --force to override).\n'
      fi
    else
      printf '[dry-run] no legacy system unit detected — would proceed.\n'
    fi
    return 0
  fi
  if [[ "$present" != "true" ]]; then
    return 0
  fi
  if [[ "$FORCE" == "true" ]]; then
    warn "Legacy system-wide ${SERVICE_NAME} install still present — proceeding only because --force was given. Two bots on one Telegram token fight (409 conflicts) and lose messages; remove the old unit as soon as possible."
    note_stale_leftover "$LEGACY_SYSTEM_UNIT" "sudo rm -f ${LEGACY_SYSTEM_UNIT} && sudo systemctl daemon-reload"
    note_stale_leftover "$LEGACY_SYSTEM_PREFIX" "sudo rm -rf ${LEGACY_SYSTEM_PREFIX}"
    return 0
  fi
  printf 'ERROR: Refusing to install: a legacy system-wide %s install is still present (unit file %s exists, or the system instance is active/enabled).\n' "$SERVICE_NAME" "$LEGACY_SYSTEM_UNIT" >&2
  printf 'ERROR: Installing now would run TWO bots on the same Telegram token: the bots fight over updates (HTTP 409 conflicts), steal updates from each other, and messages vanish.\n' >&2
  printf 'ERROR: Remove the old system install first, then re-run without --force:\n' >&2
  printf 'ERROR:   sudo systemctl disable --now %s\n' "$SERVICE_NAME" >&2
  printf 'ERROR:   sudo rm -f %s && sudo systemctl daemon-reload\n' "$LEGACY_SYSTEM_UNIT" >&2
  printf 'ERROR:   sudo rm -rf %s\n' "$LEGACY_SYSTEM_PREFIX" >&2
  printf 'ERROR: Pass --force only once the old system unit is fully stopped, disabled, and removed.\n' >&2
  exit 1
}

do_uninstall() {
  local stamp=""
  stamp="$(date +%Y%m%d-%H%M%S)"
  log "Uninstalling ${SERVICE_NAME} (per-user service)..."
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
  else
    # --yes given: still require typed confirmation, unless --non-interactive
    # (explicit --yes alone is then enough) or --dry-run (prints only).
    if [[ -z "$PREFIX" || "$PREFIX" == "/" || "$PREFIX" == "$HOME" ]]; then
      die "Refusing to delete unsafe prefix: '${PREFIX:-}'."
    fi
    if [[ "$DRY_RUN" == "true" ]]; then
      printf '[dry-run] would ask for typed DELETE confirmation, then run: rm -rf -- %q\n' "$PREFIX"
    else
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
    fi
  fi

  # Stale root-owned leftovers from an old system-mode install are NOT
  # touched (no sudo): report them with a self-serve remediation instead.
  note_stale_leftover "$LEGACY_SYSTEM_UNIT" "sudo rm -f ${LEGACY_SYSTEM_UNIT} && sudo systemctl daemon-reload"
  note_stale_leftover "$LEGACY_SYSTEM_PREFIX" "sudo rm -rf ${LEGACY_SYSTEM_PREFIX}"
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

write_unit() {
  local unit_dir="" stamp="" content=""
  unit_dir="$(dirname "$UNIT_PATH")"
  stamp="$(date +%Y%m%d-%H%M%S)"

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

# Preflight (before any copy): refuse to install alongside a live legacy
# system-wide unit — two bots on one token means 409 conflicts and lost
# messages — unless --force explicitly overrides.
guard_legacy_system_unit

copy_runtime_files
install_deps
configure_env
write_unit
enable_and_check

log ""
log "Done. User-space install. Prefix: ${PREFIX}. Unit: ${UNIT_PATH}."
