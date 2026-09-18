#!/usr/bin/env bash
#
# setup-env.sh — interactively create (or update) the .env file.
#
# Usage:
#   bash scripts/setup-env.sh
#
# Re-running is safe: when .env already exists it is backed up to
# .env.bak.<timestamp> and its current values become the prompt defaults.
#
# Dependencies: bash + curl only. The sole network call is the optional
# Telegram getUpdates auto-detect for TELEGRAM_CHAT_ID (10s timeout, with
# manual-entry fallback on any failure).
#
# The chat-ID auto-detect generalizes the getUpdates pattern from
# get-chat-id.sh — it never hardcodes a bot username.

set -euo pipefail

# Always operate from the repository root (parent of this script's directory).
cd "$(dirname "${BASH_SOURCE[0]}")/.."

ENV_FILE=".env"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# env_get FILE KEY — print KEY's raw value from an env-style file without
# executing it. Prints an empty string when the key/file is missing.
# Always exits 0 (safe under `set -e` / `set -o pipefail`).
env_get() {
  local file="$1" key="$2" line=""
  line=$(grep -E "^${key}=" "$file" 2>/dev/null | tail -n 1 || true)
  line=${line#"${key}="}
  printf '%s' "$line"
}

# mask_secret VALUE — render only the last 4 characters of a secret.
# Never echo a full secret back to the terminal.
mask_secret() {
  local secret="${1:-}"
  if [[ -z "$secret" ]]; then
    printf '(empty)'
  else
    printf '...%s' "${secret: -4}"
  fi
}

# prompt_with_default VAR_NAME PROMPT DEFAULT — ask a question, store the
# answer in $VAR_NAME. Pressing Enter accepts DEFAULT.
prompt_with_default() {
  local var_name="$1" prompt="$2" default="$3" reply=""
  if [[ -n "$default" ]]; then
    printf '%s [%s]: ' "$prompt" "$default"
  else
    printf '%s: ' "$prompt"
  fi
  IFS= read -r reply || reply=""
  if [[ -z "$reply" ]]; then
    reply="$default"
  fi
  printf -v "$var_name" '%s' "$reply"
}

# prompt_number VAR_NAME PROMPT DEFAULT MIN MAX — ask for an integer in
# range; re-prompts until valid.
prompt_number() {
  local var_name="$1" prompt="$2" default="$3" min="$4" max="$5" num=""
  while true; do
    prompt_with_default num "$prompt" "$default"
    if [[ "$num" =~ ^[0-9]+$ ]] && (( 10#$num >= min && 10#$num <= max )); then
      printf -v "$var_name" '%s' "$num"
      return 0
    fi
    echo "  Please enter a whole number between ${min} and ${max}."
  done
}

# detect_chat_ids TOKEN — print distinct -100... supergroup IDs seen via
# getUpdates, one per line. Fails (non-zero) on any problem: no network,
# bad token, or no supergroup messages yet. Callers must fall back to
# manual entry.
detect_chat_ids() {
  local token="$1" resp="" ids=""
  resp=$(curl -sS --max-time 10 "https://api.telegram.org/bot${token}/getUpdates" 2>/dev/null || true)
  [[ -z "$resp" ]] && return 1
  ids=$(printf '%s' "$resp" | grep -oE -- '-100[0-9]+' | sort -u || true)
  [[ -z "$ids" ]] && return 1
  printf '%s\n' "$ids"
  return 0
}

# ---------------------------------------------------------------------------
# Load current .env values as defaults (and back it up)
# ---------------------------------------------------------------------------

echo "=== opencode-telegram .env setup ==="
echo ""

if [[ -f "$ENV_FILE" ]]; then
  BACKUP="${ENV_FILE}.bak.$(date +%Y%m%d-%H%M%S)"
  cp -p "$ENV_FILE" "$BACKUP"
  echo "Existing .env found — backed up to ${BACKUP}."
  echo "Current values are shown as defaults; press Enter to keep each one."
  echo ""
  HAVE_EXISTING_ENV=true
else
  echo "No .env found — a new one will be created."
  echo "Press Enter to accept each [default]."
  echo ""
  HAVE_EXISTING_ENV=false
fi

cur_token=""
cur_chat_id=""
cur_allowed=""
cur_general="true"
cur_opencode_path=""
cur_max_instances="10"
cur_port_start="4100"
cur_pool_size="100"
cur_base_path=""
cur_api_port="4200"

if [[ "$HAVE_EXISTING_ENV" == "true" ]]; then
  cur_token=$(env_get "$ENV_FILE" "TELEGRAM_BOT_TOKEN")
  cur_chat_id=$(env_get "$ENV_FILE" "TELEGRAM_CHAT_ID")
  cur_allowed=$(env_get "$ENV_FILE" "TELEGRAM_ALLOWED_USERS")
  cur_general=$(env_get "$ENV_FILE" "HANDLE_GENERAL_TOPIC")
  cur_opencode_path=$(env_get "$ENV_FILE" "OPENCODE_PATH")
  cur_max_instances=$(env_get "$ENV_FILE" "OPENCODE_MAX_INSTANCES")
  cur_port_start=$(env_get "$ENV_FILE" "OPENCODE_PORT_START")
  cur_pool_size=$(env_get "$ENV_FILE" "OPENCODE_PORT_POOL_SIZE")
  cur_base_path=$(env_get "$ENV_FILE" "PROJECT_BASE_PATH")
  cur_api_port=$(env_get "$ENV_FILE" "API_PORT")
  [[ -z "$cur_general" ]] && cur_general="true"
  [[ -z "$cur_max_instances" ]] && cur_max_instances="10"
  [[ -z "$cur_port_start" ]] && cur_port_start="4100"
  [[ -z "$cur_pool_size" ]] && cur_pool_size="100"
  [[ -z "$cur_api_port" ]] && cur_api_port="4200"
fi

# ---------------------------------------------------------------------------
# 1. Telegram bot token (required, format-validated, never echoed back)
# ---------------------------------------------------------------------------

TOKEN_RE='^[0-9]+:[A-Za-z0-9_-]{20,}$'
TELEGRAM_BOT_TOKEN=""
while true; do
  token_input=""
  if [[ -n "$cur_token" ]]; then
    printf 'Telegram bot token from @BotFather (required) [keep current ending %s]: ' "$(mask_secret "$cur_token")"
  else
    printf 'Telegram bot token from @BotFather (required): '
  fi
  IFS= read -r -s token_input || token_input=""
  echo ""
  if [[ -z "$token_input" ]]; then
    token_input="$cur_token"
  fi
  if [[ -z "$token_input" ]]; then
    echo "  A bot token is required. Paste the token @BotFather gave you."
    continue
  fi
  if [[ ! "$token_input" =~ $TOKEN_RE ]]; then
    echo "  That does not look like a Telegram bot token (expected <digits>:<long secret>). Try again."
    continue
  fi
  TELEGRAM_BOT_TOKEN="$token_input"
  echo "  Token accepted (ending $(mask_secret "$TELEGRAM_BOT_TOKEN"))."
  break
done
echo ""

# ---------------------------------------------------------------------------
# 2. Telegram supergroup chat ID (auto-detect offered, manual fallback)
# ---------------------------------------------------------------------------

TELEGRAM_CHAT_ID=""
CHAT_RE='^-100[0-9]+$'
detected_ids=()
if [[ -n "$TELEGRAM_BOT_TOKEN" ]]; then
  detect_answer=""
  prompt_with_default detect_answer "Try to auto-detect the supergroup chat ID via getUpdates?" "Y"
  if [[ ! "$detect_answer" =~ ^[Nn] ]]; then
    echo "  Querying getUpdates (10s timeout)..."
    echo "  NOTE: if nothing is found, add this bot as ADMIN to a"
    echo "  Topics-enabled supergroup, send a message there, and try again."
    ids_output=""
    if ids_output=$(detect_chat_ids "$TELEGRAM_BOT_TOKEN"); then
      echo "  Found supergroup chat ID(s):"
      i=1
      while IFS= read -r found_id; do
        [[ -z "$found_id" ]] && continue
        detected_ids+=("$found_id")
        echo "    ${i}) ${found_id}"
        i=$((i + 1))
      done <<< "$ids_output"
      echo "  Enter a number from the list, or paste a chat ID."
    else
      echo "  Auto-detect found nothing (network issue, bad token, or no"
      echo "  messages yet). Enter the chat ID manually."
    fi
  fi
fi

while true; do
  chat_input=""
  if [[ -n "$cur_chat_id" ]]; then
    printf 'Telegram supergroup chat ID (starts with -100) [%s]: ' "$cur_chat_id"
  else
    printf 'Telegram supergroup chat ID (starts with -100): '
  fi
  IFS= read -r chat_input || chat_input=""
  if [[ -z "$chat_input" ]]; then
    chat_input="$cur_chat_id"
  elif [[ "$chat_input" =~ ^[0-9]+$ ]] && (( 10#$chat_input >= 1 && 10#$chat_input <= ${#detected_ids[@]} )); then
    chat_input="${detected_ids[$((10#$chat_input - 1))]}"
  fi
  if [[ "$chat_input" =~ $CHAT_RE ]]; then
    TELEGRAM_CHAT_ID="$chat_input"
    break
  fi
  echo "  Chat ID must look like -1001234567890 (a supergroup/channel ID starting with -100)."
done
echo ""

# ---------------------------------------------------------------------------
# 3. Allowed users (optional)
# ---------------------------------------------------------------------------

ALLOWED_RE='^[0-9]+( *, *[0-9]+)*$'
TELEGRAM_ALLOWED_USERS=""
while true; do
  allowed_input=""
  prompt_with_default allowed_input "Allowed Telegram user IDs, comma-separated (optional, empty = all users)" "$cur_allowed"
  # Normalize: strip all whitespace.
  allowed_input=${allowed_input//[[:space:]]/}
  if [[ -z "$allowed_input" ]]; then
    TELEGRAM_ALLOWED_USERS=""
    break
  fi
  if [[ "$allowed_input" =~ $ALLOWED_RE ]]; then
    TELEGRAM_ALLOWED_USERS="$allowed_input"
    break
  fi
  echo "  Use comma-separated numeric user IDs (e.g. 123456,789012) or leave empty."
done

# ---------------------------------------------------------------------------
# 4. Handle General topic (default true)
# ---------------------------------------------------------------------------

HANDLE_GENERAL_TOPIC="true"
while true; do
  general_input=""
  prompt_with_default general_input "Handle messages in the General topic? (true/false)" "$cur_general"
  case "${general_input,,}" in
    "" ) HANDLE_GENERAL_TOPIC="$cur_general"; break ;;
    true|1|yes|y ) HANDLE_GENERAL_TOPIC="true"; break ;;
    false|0|no|n ) HANDLE_GENERAL_TOPIC="false"; break ;;
    * ) echo "  Please answer true or false." ;;
  esac
done

# ---------------------------------------------------------------------------
# 5. OpenCode binary path
# ---------------------------------------------------------------------------

if [[ -z "$cur_opencode_path" ]]; then
  if command -v opencode >/dev/null 2>&1; then
    cur_opencode_path="opencode"
  elif [[ -x "${HOME}/.opencode/bin/opencode" ]]; then
    cur_opencode_path="${HOME}/.opencode/bin/opencode"
  else
    cur_opencode_path="opencode"
  fi
fi
OPENCODE_PATH=""
prompt_with_default OPENCODE_PATH "Path to the opencode binary" "$cur_opencode_path"
if [[ "$OPENCODE_PATH" == *"/"* ]]; then
  if [[ ! -x "$OPENCODE_PATH" ]]; then
    echo "  WARNING: '${OPENCODE_PATH}' is not executable (or missing). Continuing anyway — fix it before starting the bot."
  fi
else
  if ! command -v "$OPENCODE_PATH" >/dev/null 2>&1; then
    echo "  WARNING: '${OPENCODE_PATH}' was not found on PATH. Continuing anyway — fix it before starting the bot."
  fi
fi

# ---------------------------------------------------------------------------
# 6-8. Instance/port numbers
# ---------------------------------------------------------------------------

OPENCODE_MAX_INSTANCES=""
prompt_number OPENCODE_MAX_INSTANCES "Max concurrent OpenCode instances" "$cur_max_instances" 1 1000

OPENCODE_PORT_START=""
prompt_number OPENCODE_PORT_START "First port for OpenCode instances" "$cur_port_start" 1 65535

OPENCODE_PORT_POOL_SIZE=""
prompt_number OPENCODE_PORT_POOL_SIZE "Number of ports reserved for OpenCode instances" "$cur_pool_size" 1 65535

# ---------------------------------------------------------------------------
# 9. Project base path (absolute, ~ expanded, created)
# ---------------------------------------------------------------------------

if [[ -z "$cur_base_path" ]]; then
  cur_base_path="${HOME}/oc-bot"
fi
PROJECT_BASE_PATH=""
while true; do
  base_input=""
  prompt_with_default base_input "Base path for project directories (absolute path)" "$cur_base_path"
  # Expand a leading ~ to $HOME (intentional literal "~" comparison;
  # expansion is done manually on the following lines).
  # shellcheck disable=SC2088
  if [[ "$base_input" == "~" ]]; then
    base_input="$HOME"
  elif [[ "$base_input" == "~/"* ]]; then
    base_input="${HOME}/${base_input#"~/"}"
  fi
  if [[ "$base_input" != /* ]]; then
    echo "  Path must be absolute (e.g. ${HOME}/oc-bot)."
    continue
  fi
  PROJECT_BASE_PATH="$base_input"
  break
done
if ! mkdir -p "$PROJECT_BASE_PATH"; then
  echo "  WARNING: could not create '${PROJECT_BASE_PATH}'. Continuing anyway — create it before starting the bot."
fi

# ---------------------------------------------------------------------------
# 10. API server port
# ---------------------------------------------------------------------------

API_PORT=""
prompt_number API_PORT "Port for the external-registration API server" "$cur_api_port" 1 65535

# ---------------------------------------------------------------------------
# Write .env (owner-only) + summary
# ---------------------------------------------------------------------------

cat > "$ENV_FILE" <<EOF
# Generated by bash scripts/setup-env.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ).
# Re-run the script any time to update values (previous file is backed up).
TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
TELEGRAM_ALLOWED_USERS=${TELEGRAM_ALLOWED_USERS}
HANDLE_GENERAL_TOPIC=${HANDLE_GENERAL_TOPIC}
OPENCODE_PATH=${OPENCODE_PATH}
OPENCODE_MAX_INSTANCES=${OPENCODE_MAX_INSTANCES}
OPENCODE_PORT_START=${OPENCODE_PORT_START}
OPENCODE_PORT_POOL_SIZE=${OPENCODE_PORT_POOL_SIZE}
PROJECT_BASE_PATH=${PROJECT_BASE_PATH}
API_PORT=${API_PORT}
EOF
chmod 600 "$ENV_FILE"

echo ""
echo "=== Wrote ${ENV_FILE} (mode 600, owner-only) ==="
echo "  TELEGRAM_BOT_TOKEN:     $(mask_secret "$TELEGRAM_BOT_TOKEN")"
echo "  TELEGRAM_CHAT_ID:       ${TELEGRAM_CHAT_ID}"
if [[ -z "$TELEGRAM_ALLOWED_USERS" ]]; then
  echo "  TELEGRAM_ALLOWED_USERS: (empty = all users allowed)"
else
  echo "  TELEGRAM_ALLOWED_USERS: ${TELEGRAM_ALLOWED_USERS}"
fi
echo "  HANDLE_GENERAL_TOPIC:   ${HANDLE_GENERAL_TOPIC}"
echo "  OPENCODE_PATH:          ${OPENCODE_PATH}"
echo "  OPENCODE_MAX_INSTANCES: ${OPENCODE_MAX_INSTANCES}"
echo "  OPENCODE_PORT_START:    ${OPENCODE_PORT_START}"
echo "  OPENCODE_PORT_POOL_SIZE: ${OPENCODE_PORT_POOL_SIZE}"
echo "  PROJECT_BASE_PATH:      ${PROJECT_BASE_PATH}"
echo "  API_PORT:               ${API_PORT}"
echo ""
echo "Next steps:"
echo "  1. Re-run any time (safe — keeps current values as defaults):"
echo "       bash scripts/setup-env.sh"
echo "  2. Install dependencies:"
echo "       bun install"
echo "  3. Start the bot:"
echo "       bun run start"
echo "  4. Health check (once running):"
echo "       curl -s http://localhost:${API_PORT}/api/health"
