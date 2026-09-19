/**
 * Duplicate-bot guard.
 *
 * Prevents two polling bot instances from fighting over `getUpdates`
 * (the ghost-poller class of bug: live 409s from a stale process stealing
 * updates). Primary mechanism is a PID lockfile under `data/`; Telegram
 * 409-conflict failures from `bot.start()` are classified separately via
 * {@link isTelegramConflictError} so "another poller is polling" is
 * distinguishable from a stale lockfile. Never logs the bot token —
 * error output carries PID and lockfile path only.
 */

import { mkdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs"
import { dirname } from "node:path"

/** Default lockfile path (gitignored runtime dir, alongside `*.db`). */
export const BOT_LOCKFILE_PATH =
  process.env.BOT_LOCKFILE_PATH || "./data/bot.lock"

export interface BotLockResult {
  /** True when this process now owns the lock. */
  acquired: boolean
  /** PID named in a pre-existing lock that is still alive. */
  stalePid?: number
  /** Lockfile path that was checked. */
  lockfile: string
}

/**
 * Check whether a PID currently refers to a live process.
 * `process.kill(pid, 0)` performs no signalling — it only probes existence.
 */
export function isPidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

function readLockPid(lockfile: string): number | null {
  let raw: string
  try {
    raw = readFileSync(lockfile, "utf8")
  } catch {
    return null
  }
  const pid = parseInt(raw.split("\n")[0]?.trim() ?? "", 10)
  return Number.isInteger(pid) && pid > 0 ? pid : null
}

/**
 * Acquire the singleton poller lock.
 *
 * - No lockfile (or unreadable PID) → write our PID and acquire.
 * - Lockfile names a LIVE pid (including our own) → refuse (`acquired: false`).
 * - Lockfile names a DEAD pid → log takeover and re-acquire.
 */
export function acquireBotLock(lockfile: string = BOT_LOCKFILE_PATH): BotLockResult {
  const existingPid = readLockPid(lockfile)
  if (existingPid !== null && (existingPid === process.pid || isPidAlive(existingPid))) {
    return { acquired: false, stalePid: existingPid, lockfile }
  }
  if (existingPid !== null) {
    console.log(
      `[BotGuard] Stale lock (dead PID ${existingPid}) at ${lockfile} — taking over`
    )
  }
  mkdirSync(dirname(lockfile), { recursive: true })
  writeFileSync(lockfile, `${process.pid}\n`)
  return { acquired: true, lockfile }
}

/**
 * Release the lock, but only if this process still owns it.
 * Never throws — safe to call on every shutdown path.
 */
export function releaseBotLock(lockfile: string = BOT_LOCKFILE_PATH): void {
  try {
    if (readLockPid(lockfile) === process.pid) {
      unlinkSync(lockfile)
    }
  } catch {
    // Best-effort cleanup; ignore errors during shutdown.
  }
}

/**
 * Loud, non-zero-exit error for a refused start. Names the stale PID and
 * the lockfile path — never the bot token.
 */
export function formatDuplicateBotError(stalePid: number, lockfile: string): string {
  return [
    "",
    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
    "[BotGuard] DUPLICATE BOT INSTANCE DETECTED — refusing to start.",
    `[BotGuard] Another poller is already running (PID ${stalePid}, lock: ${lockfile}).`,
    "[BotGuard] Two bots polling the same token steal each other's getUpdates (Telegram 409).",
    `[BotGuard] If PID ${stalePid} is dead, delete ${lockfile} and restart.`,
    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
    "",
  ].join("\n")
}

/**
 * Classify a `bot.start()` failure as a Telegram 409 getUpdates conflict
 * (i.e. another poller is polling right now), distinct from a stale lockfile.
 */
export function isTelegramConflictError(error: unknown): boolean {
  const message =
    error instanceof Error
      ? `${error.name}: ${error.message}`
      : String(error)
  return (
    message.includes("409") ||
    /conflict/i.test(message) ||
    /terminated by other getUpdates/i.test(message)
  )
}
