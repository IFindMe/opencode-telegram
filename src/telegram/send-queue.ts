/**
 * Shared Telegram send queue with aware pacing (enhance-5 task 03).
 *
 * Sparrow-style pacer: bot-wide throughput cap (~25 msg/s, under Telegram's
 * ~30/s group budget) plus the existing per-message ~1 edit/s floor (the
 * STREAM_UPDATE_INTERVAL_MS floor stays upstream in StreamHandler AND is
 * re-enforced here for edits).
 *
 * Priority order: finals/cards/notices ("final") > progress edits
 * ("progress"). The "final" class is NEVER dropped: it queues behind the
 * pacer, waits out per-message floors and 429 backoffs, and retries. The
 * "progress" class alone may skip when saturated (superseded coalescing +
 * queue-cap shedding); callers already treat progress-send failures as skip.
 *
 * 429 responses update a shared backoff (honor `retry-after`, +500ms cushion
 * per the pre-existing integration.ts convention) and the item is re-queued
 * at the front of its lane. "message is not modified" resolves as success.
 *
 * No grammy import here: the actual API call is an injected `sender`, so this
 * module is unit-testable without Telegram. Stdlib timers only. Drain timers
 * are deliberately NOT unref'd while items remain queued (never-drop class).
 */

import type { InlineKeyboardButton } from "../opencode/types"

// =============================================================================
// Types
// =============================================================================

/** Queue priority. Default everywhere is "final" (never drop). */
export type SendQueuePriority = "progress" | "final"

/** A single Telegram send/edit request. */
export interface SendQueueRequest {
  chatId: number
  topicId?: number
  text: string
  editMessageId?: number
  parseMode?: "HTML" | "Markdown" | "MarkdownV2"
  replyToMessageId?: number
  inlineKeyboard?: InlineKeyboardButton[][]
  priority: SendQueuePriority
}

/** Performs the actual Telegram API call for one request. */
export type SendQueueSender = (
  request: SendQueueRequest
) => Promise<{ messageId: number }>

export interface SendQueueOptions {
  /** Performs the actual Telegram API call. */
  sender: SendQueueSender
  /** Bot-wide floor between API calls in ms (default 40 ≈ 25 msg/s). */
  sendIntervalMs?: number
  /** Per-message edit floor in ms (default 1000; reuse STREAM_UPDATE_INTERVAL_MS). */
  editFloorMs?: number
  /** Cap for queued progress items; overflow skips (default 50). Final lane is uncapped. */
  maxProgressQueue?: number
}

// =============================================================================
// Constants
// =============================================================================

export const DEFAULT_SEND_INTERVAL_MS = 40 // ~25 msg/s bot-wide
export const DEFAULT_EDIT_FLOOR_MS = 1000 // per-message edit floor
export const DEFAULT_MAX_PROGRESS_QUEUE = 50

/** Total attempts (initial + retries) for never-drop items before surfacing. */
const FINAL_MAX_ATTEMPTS = 3
/** Total attempts for progress items before skipping (shared backoff still honored). */
const PROGRESS_MAX_ATTEMPTS = 2

const RETRY_AFTER_CUSHION_MS = 500 // pre-existing integration.ts convention

interface QueueEntry {
  request: SendQueueRequest
  /** Per-message lane key for edits (`chatId:editMessageId`). */
  key: string | null
  attempts: number
  superseded: boolean
  resolve: (value: { messageId: number }) => void
  reject: (error: Error) => void
}

// =============================================================================
// Queue
// =============================================================================

export class TelegramSendQueue {
  private readonly sender: SendQueueSender
  private readonly sendIntervalMs: number
  private readonly editFloorMs: number
  private readonly maxProgressQueue: number

  /** Never-drop lane: finals, cards, notices, all new sends. Uncapped. */
  private readonly high: QueueEntry[] = []
  /** Skippable lane: progress edits/sends only. Capped + coalesced. */
  private readonly low: QueueEntry[] = []
  private pumping = false

  /** Shared 429 backoff: no API call before this timestamp. */
  private backoffUntil = 0
  /** Timestamp of the last API call (bot-wide pacing). */
  private lastSendAt = 0
  /** Per-message last-edit timestamps (edit floor). */
  private readonly lastEditAt = new Map<string, number>()

  constructor(options: SendQueueOptions) {
    this.sender = options.sender
    this.sendIntervalMs = options.sendIntervalMs ?? DEFAULT_SEND_INTERVAL_MS
    this.editFloorMs = options.editFloorMs ?? DEFAULT_EDIT_FLOOR_MS
    this.maxProgressQueue = options.maxProgressQueue ?? DEFAULT_MAX_PROGRESS_QUEUE
  }

  /**
   * Submit a request. Final-priority always queues (never drops, never skips).
   * Progress-priority may reject with a skip error when saturated or
   * superseded — callers treat progress failures as skip (preserved behavior).
   */
  submit(request: SendQueueRequest): Promise<{ messageId: number }> {
    return new Promise<{ messageId: number }>((resolve, reject) => {
      const key =
        request.editMessageId !== undefined
          ? `${request.chatId}:${request.editMessageId}`
          : null
      const entry: QueueEntry = {
        request,
        key,
        attempts: 0,
        superseded: false,
        resolve,
        reject,
      }

      if (request.priority === "progress") {
        // Coalesce: a queued progress edit for the same message is superseded
        // by this newer one — only the tail ever sends (never the final tail:
        // finals are "final" priority and never enter this lane).
        if (key !== null) {
          for (const queued of this.low) {
            if (!queued.superseded && queued.key === key) {
              queued.superseded = true
            }
          }
        }
        // Shed load at entry when saturated — never by dropping finals (they
        // are not in this lane).
        if (this.low.length >= this.maxProgressQueue) {
          reject(
            new Error(
              `Send queue saturated (${this.low.length} queued progress edits), skipping progress edit`
            )
          )
          return
        }
        this.low.push(entry)
      } else {
        this.high.push(entry)
      }
      this.kick()
    })
  }

  /** Queue depths (for verification/logging; no secrets). */
  depths(): { high: number; low: number; backoffMs: number } {
    return {
      high: this.high.length,
      low: this.low.length,
      backoffMs: Math.max(0, this.backoffUntil - Date.now()),
    }
  }

  private kick(): void {
    if (this.pumping) return
    if (this.high.length === 0 && this.low.length === 0) return
    this.pumping = true
    void this.drain()
  }

  private pick(): QueueEntry | undefined {
    return this.high[0] ?? this.low[0]
  }

  private remove(entry: QueueEntry): void {
    const lane = entry.request.priority === "final" ? this.high : this.low
    const index = lane.indexOf(entry)
    if (index >= 0) lane.splice(index, 1)
  }

  private moveToFront(entry: QueueEntry): void {
    const lane = entry.request.priority === "final" ? this.high : this.low
    const index = lane.indexOf(entry)
    if (index > 0) {
      lane.splice(index, 1)
      lane.unshift(entry)
    }
  }

  private async drain(): Promise<void> {
    try {
      while (this.high.length > 0 || this.low.length > 0) {
        // Shared 429 backoff pauses the whole pump (both lanes).
        const backoffWait = this.backoffUntil - Date.now()
        if (backoffWait > 0) {
          await sleep(backoffWait)
          continue
        }

        const entry = this.pick()
        if (!entry) break

        // Superseded progress edits never send — skip (a newer tail for the
        // same message is queued behind, or the state went idle).
        if (entry.superseded) {
          this.remove(entry)
          entry.reject(new Error("Superseded by a newer progress edit, skipping"))
          continue
        }

        // Per-message edit floor. Finals WAIT (never drop); progress edits
        // skip (the upstream 1s throttle owns cadence; a flush re-delivers).
        if (entry.key !== null) {
          const last = this.lastEditAt.get(entry.key) ?? 0
          const floorWait = this.editFloorMs - (Date.now() - last)
          if (floorWait > 0) {
            if (entry.request.priority === "progress") {
              this.remove(entry)
              entry.reject(
                new Error(
                  `Progress edit inside per-message floor (${floorWait}ms left), skipping`
                )
              )
              continue
            }
            await sleep(floorWait)
            continue
          }
        }

        // Bot-wide pacing: at most ~1 call per sendIntervalMs.
        const paceWait = this.sendIntervalMs - (Date.now() - this.lastSendAt)
        if (paceWait > 0) {
          await sleep(paceWait)
          continue
        }
        if (Date.now() < this.backoffUntil) continue

        try {
          const result = await this.sender(entry.request)
          this.lastSendAt = Date.now()
          if (entry.key !== null) this.lastEditAt.set(entry.key, this.lastSendAt)
          this.remove(entry)
          entry.resolve(result)
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error)

          // "message is not modified" stays a success (preserved convention).
          if (message.includes("message is not modified")) {
            this.lastSendAt = Date.now()
            this.remove(entry)
            entry.resolve({ messageId: entry.request.editMessageId ?? 0 })
            continue
          }

          const retryAfterMs = parseRetryAfterMs(message)
          if (retryAfterMs !== null) {
            // Honor retry-after + cushion, re-queue at the front of its lane.
            this.backoffUntil = Date.now() + retryAfterMs + RETRY_AFTER_CUSHION_MS
            entry.attempts += 1
            const maxAttempts =
              entry.request.priority === "final"
                ? FINAL_MAX_ATTEMPTS
                : PROGRESS_MAX_ATTEMPTS
            if (entry.attempts >= maxAttempts) {
              // Attempts exhausted: surface to the caller (final fallbacks in
              // StreamHandler compose on top — bounded, no infinite retry).
              this.remove(entry)
              entry.reject(error instanceof Error ? error : new Error(message))
              continue
            }
            this.moveToFront(entry)
            console.log(
              `[SendQueue] 429, backing off ${retryAfterMs}ms+${RETRY_AFTER_CUSHION_MS}ms ` +
                `(attempt ${entry.attempts}/${maxAttempts}, ${entry.request.priority})`
            )
            continue
          }

          // Non-429 errors fail fast to the caller (preserved convention).
          this.remove(entry)
          entry.reject(error instanceof Error ? error : new Error(message))
        }
      }
    } finally {
      this.pumping = false
      // Items may have arrived during teardown — re-kick (no timer leaks:
      // the loop exits with zero pending timers when both lanes are empty).
      if (this.high.length > 0 || this.low.length > 0) this.kick()
    }
  }
}

// =============================================================================
// Helpers
// =============================================================================

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/** Parse Telegram `retry after N` (seconds) → ms; null when not a 429. */
export function parseRetryAfterMs(message: string): number | null {
  if (!message.includes("429") && !message.includes("Too Many Requests")) {
    return null
  }
  const match = message.match(/retry after (\d+)/i)
  const seconds = match ? parseInt(match[1], 10) : 3
  return (Number.isFinite(seconds) ? seconds : 3) * 1000
}
