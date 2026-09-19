/**
 * Stream Handler
 * 
 * Bridges OpenCode SSE events to Telegram progress messages.
 * Handles throttling, formatting, and state management for streaming responses.
 */

import type {
  SSEEvent,
  StreamingState,
  StreamHandlerConfig,
  TelegramSendCallback,
  TelegramDeleteCallback,
  Part,
  TextPart,
  ToolInvocationPart,
  Permission,
  InlineKeyboardButton,
  TokenUsage,
  MessageInfo,
} from "./types"
import { DEFAULT_STREAM_HANDLER_CONFIG } from "./types"
import { markdownToTelegramHtml, truncateForTelegram } from "./telegram-markdown"

/**
 * Pending permission request info
 */
export interface PendingPermission {
  permission: Permission
  telegramMessageId?: number
  chatId: number
  topicId: number
  /** Epoch ms when the permission card was posted (H2 reminder) */
  createdAt: number
  /** Whether the single stalled-permission reminder was already sent */
  reminderSent?: boolean
}

/**
 * P2 (I-3): snapshot of one completed turn for the Full-output toggle.
 * `collapsedHtml` is the exact text sent to the final card; `fullAnswerHtml`
 * is the untruncated answer (capped — Telegram can show at most ~2 messages
 * worth, the rest gets an explicit note, never silent).
 */
export interface TurnSnapshot {
  chatId: number
  topicId: number
  /** Final card message id (EDIT target for expand/collapse) */
  messageId: number
  /** Exact collapsed text rendered on the card (toggle-back target) */
  collapsedHtml: string
  /** Untruncated answer HTML (capped at FULL_STORE_CAP chars) */
  fullAnswerHtml: string
  /** True when the card shows the truncated form (button attached) */
  answerTruncated: boolean
  /** Full answer exceeded even the 2-part expansion budget */
  beyondBudget: boolean
  /** Set once expanded (collapse restores the collapsed form) */
  expanded: boolean
}

/**
 * Callback fired when a session goes idle (response complete)
 */
export type SessionIdleCallback = (
  sessionId: string,
  chatId: number,
  topicId: number
) => void | Promise<void>

/**
 * Streaming state for a session
 */
export class StreamHandler {
  private readonly config: StreamHandlerConfig
  private readonly states: Map<string, StreamingState> = new Map()
  private readonly sendCallback: TelegramSendCallback
  private readonly deleteCallback?: TelegramDeleteCallback

  /** Mapping from sessionId to Telegram chat/topic info */
  private readonly sessionToTelegram: Map<string, { chatId: number; topicId: number }> = new Map()

  /** Mapping from sessionId to streaming enabled state */
  private readonly sessionStreamingEnabled: Map<string, boolean> = new Map()

  /** Pending permission requests - keyed by permissionId */
  private readonly pendingPermissions: Map<string, PendingPermission> = new Map()

  /** Reminder timers for pending permissions - keyed by permissionId (H2) */
  private readonly permissionTimers: Map<string, ReturnType<typeof setTimeout>> = new Map()

  /** Task 05: permission types auto-answered "once" (lowercased exact match; empty = deny all). Set via setPermissionsAutoAllow. */
  private permissionsAutoAllow = new Set<string>()

  /** Task 05: injected OpenCode responder for auto-allow (integration-owned resolve chain). Unset = ask (never auto-allow without an API path). */
  private permissionAutoResponder?: (
    sessionId: string,
    permissionId: string,
    destination: { chatId: number; topicId: number }
  ) => Promise<boolean>

  /** Task 05: recently auto-allowed permission ids (suppresses asked/updated duplicates; ids are unique per request; capped). */
  private readonly autoAllowedPermissionIds = new Set<string>()

  /** One-shot trailing-flush timers - keyed by sessionId (at most one per session) */
  private readonly flushTimers: Map<string, ReturnType<typeof setTimeout>> = new Map()

  /** Last successfully rendered progress text - keyed by sessionId (dirty check) */
  private readonly lastRenderedText: Map<string, string> = new Map()

  /**
   * P2 (S3/S4): per-kind one-line tool summaries (`<cmd …>` / path / host),
   * keyed `${sessionId}:${callId}`. Raw (unescaped — escaped at render, like
   * titles). Cleared with the turn state; rides state cleanup, never touched
   * by types.ts (scope fence).
   */
  private readonly toolSummaries: Map<string, string> = new Map()

  /**
   * P2 (I-3): completed-turn store for the Full-output toggle. Keyed
   * `${chatId}:${turnKey}` (turnKey = short random key, ≤8 ch, so callback
   * data stays far under 64 B). Ring buffer: cap ~20 turns/topic, oldest
   * evicted. Entries are render snapshots from already-received SSE state —
   * never fetched from OpenCode. Survives unregisterSession (old finals stay
   * expandable); bounded by the ring cap.
   */
  private readonly turnStore: Map<string, TurnSnapshot> = new Map()
  private readonly turnOrderByTopic: Map<number, string[]> = new Map()

  /** P2 (I-3): ring capacity — completed turns kept per topic. */
  private static readonly TURN_RING_CAP = 20
  /**
   * P2 (I-3): full-answer store cap (~2 Telegram messages). Expansion shows
   * at most an EDIT + one 2/2 follow-up; anything beyond gets an explicit
   * note — never silent, never unbounded.
   */
  private static readonly FULL_STORE_CAP = 8100
  /** P2 (I-4): tool lines cap in the threaded details reply. */
  private static readonly DETAILS_TOOL_CAP = 20

  /** Track message roles: messageId -> role */
  private readonly messageRoles: Map<string, "user" | "assistant"> = new Map()

  /** Track which user messages we've already sent to Telegram (to prevent duplicates) */
  private readonly sentUserMessages: Set<string> = new Set()

  /** Track messages that originated from Telegram (so we don't echo them back) */
  private readonly messagesFromTelegram: Set<string> = new Set()

  /** Callback fired when a session goes idle */
  private onSessionIdleCallback?: SessionIdleCallback

  constructor(
    sendCallback: TelegramSendCallback,
    deleteCallback?: TelegramDeleteCallback,
    config?: Partial<StreamHandlerConfig>
  ) {
    this.sendCallback = sendCallback
    this.deleteCallback = deleteCallback
    this.config = { ...DEFAULT_STREAM_HANDLER_CONFIG, ...config }
  }

  /**
   * Set callback for when a session goes idle (response complete)
   * Useful for updating topic names after first message
   */
  setOnSessionIdle(callback: SessionIdleCallback): void {
    this.onSessionIdleCallback = callback
  }

  // ===========================================================================
  // Session Registration
  // ===========================================================================

  /**
   * Register a session with its Telegram destination
   */
  registerSession(sessionId: string, chatId: number, topicId: number, streamingEnabled = false): void {
    this.sessionToTelegram.set(sessionId, { chatId, topicId })
    this.sessionStreamingEnabled.set(sessionId, streamingEnabled)
  }

  /**
   * Unregister a session
   */
  unregisterSession(sessionId: string): void {
    this.clearFlushTimer(sessionId)
    this.clearToolSummaries(sessionId)
    this.lastRenderedText.delete(sessionId)
    this.sessionToTelegram.delete(sessionId)
    this.sessionStreamingEnabled.delete(sessionId)
    this.states.delete(sessionId)
    // H2/H5: drop pending permissions tied to a superseded session so stale
    // 🔐 buttons can never block a new session silently.
    for (const [permId, pending] of this.pendingPermissions) {
      if (pending.permission.sessionID === sessionId) {
        this.clearPermissionTimer(permId)
        this.pendingPermissions.delete(permId)
      }
    }
  }

  /**
   * Update streaming preference for a session
   */
  setStreamingEnabled(sessionId: string, enabled: boolean): void {
    this.sessionStreamingEnabled.set(sessionId, enabled)
  }

  /**
   * Check if streaming is enabled for a session
   */
  isStreamingEnabled(sessionId: string): boolean {
    return this.sessionStreamingEnabled.get(sessionId) ?? false
  }

  /**
   * Get Telegram destination for a session
   */
  getTelegramDestination(sessionId: string): { chatId: number; topicId: number } | undefined {
    return this.sessionToTelegram.get(sessionId)
  }

  /**
   * Mark a message text as originating from Telegram (so we don't echo it back)
   * Call this when sending a message from Telegram to OpenCode
   */
  markMessageFromTelegram(sessionId: string, messageText: string): void {
    // Use a composite key of sessionId + normalized text to identify the message
    const key = `${sessionId}:${messageText.trim()}`
    this.messagesFromTelegram.add(key)
    
    // Clean up old entries to prevent memory leak (keep last 100)
    if (this.messagesFromTelegram.size > 100) {
      const entries = Array.from(this.messagesFromTelegram)
      this.messagesFromTelegram.clear()
      for (const entry of entries.slice(-50)) {
        this.messagesFromTelegram.add(entry)
      }
    }
  }

  /**
   * Check if a message originated from Telegram
   */
  private isMessageFromTelegram(sessionId: string, messageText: string): boolean {
    const key = `${sessionId}:${messageText.trim()}`
    if (this.messagesFromTelegram.has(key)) {
      // Remove it after checking (one-time use)
      this.messagesFromTelegram.delete(key)
      return true
    }
    return false
  }

  // ===========================================================================
  // Event Handling
  // ===========================================================================

  /**
   * Handle an SSE event from OpenCode
   */
  async handleEvent(event: SSEEvent): Promise<void> {
    // Extract sessionID from various possible locations in the event
    const props = event.properties as Record<string, any>
    const sessionId = 
      props.sessionID ||                    // session.idle, session.status, session.diff
      props.info?.sessionID ||              // message.updated
      props.part?.sessionID ||              // message.part.updated
      props.permission?.sessionID ||        // permission.updated (nested envelope variant, H2)
      (event.type === 'session.updated' ? props.info?.id : null) ||  // session.updated has id not sessionID
      null
    
    if (!sessionId) {
      // Only log for events that should have sessionID (skip heartbeat, server.connected)
      if (!['server.heartbeat', 'server.connected'].includes(event.type)) {
        console.log(`[StreamHandler] Event ${event.type} has no sessionID`)
      }
      return
    }

    const destination = this.sessionToTelegram.get(sessionId)
    if (!destination) {
      console.log(`[StreamHandler] Session ${sessionId} not registered, registered sessions:`, Array.from(this.sessionToTelegram.keys()))
      return // Session not registered with us
    }

    switch (event.type) {
      case "message.part.updated":
        await this.handlePartUpdated(sessionId, event, destination)
        break

      case "message.updated":
        await this.handleMessageUpdated(sessionId, event, destination)
        break

      case "tool.execute":
        await this.handleToolExecute(sessionId, event, destination)
        break

      case "tool.result":
        await this.handleToolResult(sessionId, event, destination)
        break

      case "file.edited":
        await this.handleFileEdited(sessionId, event, destination)
        break

      case "session.idle":
        await this.handleSessionIdle(sessionId, destination)
        break

      case "session.error":
        await this.handleSessionError(sessionId, event, destination)
        break

      case "session.updated":
        await this.handleSessionUpdated(sessionId, event, destination)
        break

      case "permission.asked":
      case "permission.updated":
        await this.handlePermissionUpdated(event, destination)
        break

      case "permission.replied":
        await this.handlePermissionReplied(event)
        break
    }
  }

  /**
   * Handle message part updates (streaming text)
   */
  private async handlePartUpdated(
    sessionId: string,
    event: SSEEvent,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    const props = event.properties as Record<string, any>
    const part = props.part as Record<string, any>
    const messageId = part.messageID || props.messageID

    // Check if this is a user message
    const messageRole = messageId ? this.messageRoles.get(messageId) : undefined
    if (messageRole === "user") {
      // For user messages, send as a separate "echo" message (once per message)
      if (part.type === "text" && part.text && messageId && !this.sentUserMessages.has(messageId)) {
        this.sentUserMessages.add(messageId)
        const userText = part.text.trim()
        if (userText) {
          // Check if this message originated from Telegram - if so, don't echo it
          if (this.isMessageFromTelegram(sessionId, userText)) {
            // Message came from Telegram, no need to echo
            return
          }
          
          try {
            // Send user message with a prefix to distinguish it (from TUI)
            await this.sendCallback(
              destination.chatId,
              destination.topicId,
              `<b>📝 From TUI:</b>\n${this.escapeHtml(userText)}`,
              { parseMode: "HTML" }
            )
          } catch (error) {
            console.error(`[StreamHandler] Failed to echo user message:`, error)
          }
        }
      }
      return // Don't process user messages as streaming state
    }

    let state = this.states.get(sessionId)
    if (!state) {
      state = this.createState(sessionId)
      this.states.set(sessionId, state)
    }

    state.messageId = messageId
    state.isProcessing = true

    // Handle text parts (type: "text")
    if (part.type === "text" && part.text) {
      state.currentText = part.text
    }

    // Handle tool parts (type: "tool") - OpenCode uses "tool" not "tool-invocation"
    if (part.type === "tool" && part.callID && part.tool) {
      const existingTool = state.toolsInvoked.find(t => t.callId === part.callID)
      if (!existingTool) {
        state.toolsInvoked.push({
          name: part.tool,
          callId: part.callID,
          startedAt: new Date(),
        })
        // P2 (S3): tool.execute usually arrives first with args, but a bare
        // part-update may be all we get — capture opportunistically.
        const partArgs = part.input ?? part.args
        if (partArgs && typeof partArgs === "object") {
          const summary = this.summarizeToolCall(
            String(part.tool),
            partArgs as Record<string, unknown>
          )
          if (summary) {
            this.toolSummaries.set(`${sessionId}:${String(part.callID)}`, summary)
          }
        }
      }
      // Check if tool has result (state field or result field)
      if (part.state === "result" || part.result !== undefined) {
        const tool = state.toolsInvoked.find(t => t.callId === part.callID)
        if (tool && !tool.completedAt) {
          tool.completedAt = new Date()
        }
      }
    }

    // Handle step-finish (marks end of a tool execution step)
    if (part.type === "step-finish") {
      // Mark all running tools as completed
      for (const tool of state.toolsInvoked) {
        if (!tool.completedAt) {
          tool.completedAt = new Date()
        }
      }
    }

    // Handle legacy tool-invocation format (in case it's still used)
    if (part.type === "tool-invocation" && part.toolInvocation) {
      const { toolInvocation } = part
      if (toolInvocation.state === "call") {
        const existingTool = state.toolsInvoked.find(
          (t) => t.callId === toolInvocation.toolCallId
        )
        if (!existingTool) {
          state.toolsInvoked.push({
            name: toolInvocation.toolName,
            callId: toolInvocation.toolCallId,
            startedAt: new Date(),
          })
        }
      } else if (toolInvocation.state === "result") {
        const tool = state.toolsInvoked.find(
          (t) => t.callId === toolInvocation.toolCallId
        )
        if (tool) {
          tool.completedAt = new Date()
        }
      }
    }

    // Throttled update to Telegram
    await this.maybeUpdateTelegram(sessionId, state, destination)
  }

  /**
   * Handle message updates (contains token info)
   */
  private async handleMessageUpdated(
    sessionId: string,
    event: SSEEvent,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    const props = event.properties as Record<string, any>
    const info = props.info as MessageInfo | undefined

    if (!info) return

    // Track message role so we can filter user messages in handlePartUpdated
    if (info.id && info.role) {
      this.messageRoles.set(info.id, info.role)
    }

    // Skip user messages - we only want to show assistant responses
    if (info.role === "user") {
      return
    }

    let state = this.states.get(sessionId)
    if (!state) {
      state = this.createState(sessionId)
      this.states.set(sessionId, state)
    }

    // Update token info
    if (info.tokens) {
      state.tokens = info.tokens
    }

    // Update model info
    if (info.model) {
      state.model = info.model
    }

    // Throttled update to Telegram
    await this.maybeUpdateTelegram(sessionId, state, destination)
  }

  /**
   * Handle tool execution start
   */
  private async handleToolExecute(
    sessionId: string,
    event: SSEEvent,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    const props = event.properties as {
      sessionID: string
      tool: string
      callID: string
      args: Record<string, unknown>
    }

    let state = this.states.get(sessionId)
    if (!state) {
      state = this.createState(sessionId)
      this.states.set(sessionId, state)
    }

    state.isProcessing = true

    // Add tool to list if not already there
    const existingTool = state.toolsInvoked.find((t) => t.callId === props.callID)
    if (!existingTool) {
      state.toolsInvoked.push({
        name: props.tool,
        callId: props.callID,
        startedAt: new Date(),
      })
      // P2 (S3): capture the per-kind one-line summary from the
      // already-received args (command / path / url). Raw — escaped at render.
      const summary = this.summarizeToolCall(props.tool, props.args)
      if (summary) {
        this.toolSummaries.set(`${sessionId}:${props.callID}`, summary)
      }
    }

    // Force update to show tool is running
    await this.updateTelegram(sessionId, state, destination, true)
  }

  /**
   * Handle tool result
   */
  private async handleToolResult(
    sessionId: string,
    event: SSEEvent,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    const props = event.properties as {
      sessionID: string
      tool: string
      callID: string
      title?: string
      metadata?: Record<string, unknown>
    }

    const state = this.states.get(sessionId)
    if (!state) return

    // Mark tool as completed
    const tool = state.toolsInvoked.find((t) => t.callId === props.callID)
    if (tool) {
      tool.completedAt = new Date()
      tool.title = props.title
    }

    // Update Telegram to show tool completed
    await this.maybeUpdateTelegram(sessionId, state, destination)
  }

  /**
   * P2 (S5): file.edited is an implicit tool-completion signal. A matching
   * running edit/write line flips ✅ with the path basename; otherwise one
   * synthetic completed line is appended. Folds into the next scheduled
   * progress edit — never forces one alone (throttled path only). No state =
   * no turn in flight, nothing to fold into — return without creating any.
   */
  private async handleFileEdited(
    sessionId: string,
    event: SSEEvent,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    const props = event.properties as Record<string, any>
    const rawPath =
      (typeof props.path === "string" && props.path) ||
      (typeof props.file === "string" && props.file) ||
      null
    if (!rawPath) return
    const state = this.states.get(sessionId)
    if (!state) return
    const basename = String(rawPath).split("/").pop() || String(rawPath)
    const match = [...state.toolsInvoked]
      .reverse()
      .find(
        (t) =>
          !t.completedAt &&
          (t.name.toLowerCase() === "edit" || t.name.toLowerCase() === "write")
      )
    const now = new Date()
    if (match) {
      match.completedAt = now
      match.title = basename
    } else {
      state.toolsInvoked.push({
        name: "edit",
        callId: `file:${now.getTime()}:${basename}`,
        startedAt: now,
        completedAt: now,
        title: basename,
      })
    }
    // Throttled only: fold into the next scheduled edit. Guard the card-less
    // idle case — a stray file event must not mint a progress card alone.
    if (!state.telegramMessageId && !state.isProcessing) return
    await this.maybeUpdateTelegram(sessionId, state, destination)
  }

  /**
   * Handle session becoming idle (response complete)
   */
  private async handleSessionIdle(
    sessionId: string,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    const state = this.states.get(sessionId)
    if (!state) return

    state.isProcessing = false

    // A pending trailing flush is superseded by the final render below.
    this.clearFlushTimer(sessionId)
    // Send final response - edit the progress message if we have one
    if (state.currentText.trim()) {
      // Convert Markdown to Telegram HTML for proper rendering
      const htmlContent = markdownToTelegramHtml(state.currentText.trim())
      const finalContent = truncateForTelegram(htmlContent)

      // P2 (I-3): truncation affordance + Full-output toggle, primary paths
      // only (fallbacks below keep P1 behavior verbatim: readable final, no
      // toggle — affordance and button stay atomic). Collapsed base uses a
      // 3900 cap so base + affordance line stays under the 4096 hard limit.
      const answerTruncated = htmlContent.length > 4000
      const turnKey = answerTruncated ? this.makeTurnKey(destination.chatId) : null
      const collapsedHtml = answerTruncated
        ? `${truncateForTelegram(htmlContent, 3900)}\n\n<i>Output truncated — tap Full output for the rest.</i>`
        : finalContent
      const collapsedKeyboard = turnKey ? this.turnExpandKeyboard(turnKey) : []
      // finalMessageId = the card the user sees (EDIT target for expand /
      // collapse, reply target for the tool-details thread). Primary paths
      // set it; fallbacks set it when a NEW message actually goes out.
      let finalMessageId: number | undefined
      let primaryRendered = false

      try {
        if (state.telegramMessageId) {
          // Edit the progress message to show final response (P1: strip the
          // ⏹ Cancel keyboard on the S8 final — buttons change only on the
          // busy→idle transition; P2: the Full-output toggle rides this same
          // transition edit, zero extra calls).
          const res = await this.sendCallback(
            destination.chatId,
            destination.topicId,
            collapsedHtml,
            {
              parseMode: "HTML",
              editMessageId: state.telegramMessageId,
              inlineKeyboard: collapsedKeyboard,
            }
          )
          finalMessageId = res.messageId
          primaryRendered = true
        } else {
          // No progress message, send new one
          const res = await this.sendCallback(
            destination.chatId,
            destination.topicId,
            collapsedHtml,
            { parseMode: "HTML", inlineKeyboard: collapsedKeyboard }
          )
          finalMessageId = res.messageId
          primaryRendered = true
        }
      } catch (error) {
        const errorMsg = error instanceof Error ? error.message : String(error)
        // H4: always log lengths so over-length/parse failures are diagnosable.
        const finalLen = finalContent.length
        const rawLen = state.currentText.trim().length
        
        // Ignore "message is not modified" - content is already correct
        if (errorMsg.includes('message is not modified')) {
          // Already showing the right content, nothing to do
        } else if (state.telegramMessageId && errorMsg.includes('message to edit not found')) {
          // Original message was deleted, send as new message
          console.log(`[StreamHandler] Original message deleted, sending final as new message (finalLen=${finalLen} rawLen=${rawLen})`)
          try {
            const res = await this.sendCallback(
              destination.chatId,
              destination.topicId,
              finalContent,
              { parseMode: "HTML" }
            )
            finalMessageId = res.messageId
          } catch (fallbackError) {
            const fbMsg = fallbackError instanceof Error ? fallbackError.message : String(fallbackError)
            console.error(`[StreamHandler] Final fallback send failed (finalLen=${finalLen} rawLen=${rawLen}): ${fbMsg.slice(0, 120)}`)
          }
        } else if (errorMsg.includes("can't parse entities")) {
          // HTML parsing failed, try sending as plain text
          console.log(`[StreamHandler] HTML parsing failed, falling back to plain text (finalLen=${finalLen} rawLen=${rawLen})`)
          try {
            await this.sendCallback(
              destination.chatId,
              destination.topicId,
              state.currentText.trim(),
              { editMessageId: state.telegramMessageId }
            )
            finalMessageId = state.telegramMessageId
          } catch (plainError) {
            const plMsg = plainError instanceof Error ? plainError.message : String(plainError)
            console.error(`[StreamHandler] Plain-text final edit failed (rawLen=${rawLen}): ${plMsg.slice(0, 120)}, sending as new message`)
            try {
              const res = await this.sendCallback(
                destination.chatId,
                destination.topicId,
                state.currentText.trim(),
                {}
              )
              finalMessageId = res.messageId
            } catch (plainFallbackError) {
              const pfbMsg = plainFallbackError instanceof Error ? plainFallbackError.message : String(plainFallbackError)
              console.error(`[StreamHandler] Plain-text fallback send failed (rawLen=${rawLen}): ${pfbMsg.slice(0, 120)}`)
            }
          }
        } else if (errorMsg.includes('429') || errorMsg.includes('Too Many Requests') || errorMsg.includes('Rate limited')) {
          // Rate limited - wait and retry the final response (it's important!)
          const retryMatch = errorMsg.match(/retry after (\d+)/)
          const retryAfter = retryMatch ? parseInt(retryMatch[1], 10) * 1000 : 5000
          console.log(`[StreamHandler] Rate limited on final response, waiting ${retryAfter}ms to retry`)
          
          // Wait for rate limit to expire, then retry
          await new Promise(resolve => setTimeout(resolve, retryAfter + 500))
          
          try {
            if (state.telegramMessageId) {
              await this.sendCallback(
                destination.chatId,
                destination.topicId,
                finalContent,
                {
                  parseMode: "HTML",
                  editMessageId: state.telegramMessageId,
                  inlineKeyboard: [],
                }
              )
              finalMessageId = state.telegramMessageId
              console.log(`[StreamHandler] Final response sent after rate limit wait`)
            } else {
              const res = await this.sendCallback(
                destination.chatId,
                destination.topicId,
                finalContent,
                { parseMode: "HTML" }
              )
              finalMessageId = res.messageId
              console.log(`[StreamHandler] Final response sent as new message after rate limit wait`)
            }
          } catch (retryError) {
            // If retry also fails, try sending as a new message
            const rtMsg = retryError instanceof Error ? retryError.message : String(retryError)
            console.log(`[StreamHandler] Retry failed (${rtMsg.slice(0, 80)}), sending final response as new message (finalLen=${finalLen} rawLen=${rawLen})`)
            try {
              const res = await this.sendCallback(
                destination.chatId,
                destination.topicId,
                finalContent,
                { parseMode: "HTML" }
              )
              finalMessageId = res.messageId
            } catch (finalRetryError) {
              const frMsg = finalRetryError instanceof Error ? finalRetryError.message : String(finalRetryError)
              console.error(`[StreamHandler] Failed to send final response even after retry (finalLen=${finalLen} rawLen=${rawLen}): ${frMsg.slice(0, 120)}`)
            }
          }
        } else {
          // H4: ANY other final-edit failure must still surface the answer as a
          // new message — never leave a frozen progress message as silence.
          console.log(`[StreamHandler] Final edit failed (${errorMsg.slice(0, 80)} finalLen=${finalLen} rawLen=${rawLen}), sending as new message`)
          try {
            const res = await this.sendCallback(
              destination.chatId,
              destination.topicId,
              finalContent,
              { parseMode: "HTML" }
            )
            finalMessageId = res.messageId
          } catch (fallbackError) {
            const fbMsg = fallbackError instanceof Error ? fallbackError.message : String(fallbackError)
            console.error(`[StreamHandler] Final fallback send failed (finalLen=${finalLen} rawLen=${rawLen}): ${fbMsg.slice(0, 120)}`)
          }
        }
      }

      // P2 (I-3/I-4): turn follow-ups from already-received SSE state (never
      // fetched). The toggle store is written ONLY for the primary render —
      // stored collapsedHtml must match the card exactly. The details thread
      // follows whatever final the user actually sees (primary or fallback).
      if (primaryRendered && turnKey && finalMessageId !== undefined) {
        this.storeTurn(turnKey, {
          chatId: destination.chatId,
          topicId: destination.topicId,
          messageId: finalMessageId,
          collapsedHtml,
          fullAnswerHtml: htmlContent.slice(0, StreamHandler.FULL_STORE_CAP),
          answerTruncated: true,
          beyondBudget: htmlContent.length > StreamHandler.FULL_STORE_CAP,
          expanded: false,
        })
      }
      if (finalMessageId !== undefined && state.toolsInvoked.length > 0) {
        await this.postToolDetailsReply(sessionId, state, destination, finalMessageId)
      }
    } else if (state.telegramMessageId) {
      // S9: never delete silently — EDIT the card to an explicit
      // empty-completion receipt (same message, zero new-message cost, final
      // lane so the receipt is never dropped). Keyboard stripped: nothing to
      // cancel once the turn is over.
      const toolCount = state.toolsInvoked.length
      const receipt = toolCount > 0
        ? `<i>Done — no text reply. (Tools ran: ${toolCount}. Send another message or /cancel to reset.)</i>`
        : `<i>Done — nothing to report. Try rephrasing, or send /cancel to reset the turn.</i>`
      try {
        await this.sendCallback(
          destination.chatId,
          destination.topicId,
          receipt,
          {
            parseMode: "HTML",
            editMessageId: state.telegramMessageId,
            inlineKeyboard: [],
          }
        )
        this.lastRenderedText.set(sessionId, receipt)
      } catch {
        // Best-effort receipt; the idle cleanup below still runs.
      }
    }

    // Clean up state
    this.states.delete(sessionId)
    this.clearToolSummaries(sessionId)
    this.lastRenderedText.delete(sessionId)
    
    // Clean up message roles and sent user messages (keep maps from growing indefinitely)
    // We can't easily filter by session, so just clear old entries periodically
    if (this.messageRoles.size > 100) {
      // Keep only the most recent 50 entries
      const entries = Array.from(this.messageRoles.entries())
      this.messageRoles.clear()
      for (const [key, value] of entries.slice(-50)) {
        this.messageRoles.set(key, value)
      }
    }
    if (this.sentUserMessages.size > 100) {
      // Keep only the most recent 50 entries
      const entries = Array.from(this.sentUserMessages)
      this.sentUserMessages.clear()
      for (const key of entries.slice(-50)) {
        this.sentUserMessages.add(key)
      }
    }

    // Fire the session idle callback (for topic name updates, etc.)
    if (this.onSessionIdleCallback) {
      try {
        await this.onSessionIdleCallback(sessionId, destination.chatId, destination.topicId)
      } catch (error) {
        console.error(`[StreamHandler] onSessionIdle callback error:`, error)
      }
    }
  }

  /**
   * P1 (S10/S19): park the live progress card as Stopped (EDIT + strip
   * keyboard, so no frozen "Thinking" remains), then post the NEW error card
   * with Retry + Cancel (final priority — never dropped). Callers own
   * isProcessing/state cleanup; this helper only renders.
   */
  private async postStoppedAndErrorCard(
    sessionId: string,
    destination: { chatId: number; topicId: number },
    rawError: string,
    header = "Something went wrong"
  ): Promise<void> {
    const state = this.states.get(sessionId)
    if (state) {
      this.clearFlushTimer(sessionId)
      if (state.telegramMessageId) {
        try {
          await this.sendCallback(
            destination.chatId,
            destination.topicId,
            `⏹ <b>Stopped</b>`,
            {
              parseMode: "HTML",
              editMessageId: state.telegramMessageId,
              inlineKeyboard: [],
            }
          )
          this.lastRenderedText.set(sessionId, `⏹ <b>Stopped</b>`)
        } catch {
          // Park is best-effort; the NEW card below is the real receipt.
        }
      }
    }
    // 120-char slice in <code>; full error stays in server logs only
    // (secret-leak + length risk). Never-dropped final lane (no progress flag).
    const errSlice = this.escapeHtml(rawError.slice(0, 120) || "unknown error")
    const sessShort = sessionId.slice(0, 8)
    const retryKeyboard: InlineKeyboardButton[][] = [
      [
        { text: "🔁 Retry last message", callback_data: `retry:${destination.topicId}` },
        { text: "⏹ Cancel", callback_data: `cancel:${sessShort}` },
      ],
    ]
    try {
      await this.sendCallback(
        destination.chatId,
        destination.topicId,
        `❌ <b>${header}</b>\n<code>${errSlice}</code>\n<i>The session is still active.</i>`,
        { parseMode: "HTML", inlineKeyboard: retryKeyboard }
      )
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error)
      console.error(`[StreamHandler] Error card send failed: ${msg.slice(0, 120)}`)
    }
  }

  /**
   * P2 (I-3): mint a short server-side turn key (≤8 ch — callback data
   * `full:<key>:<i>` stays far under 64 B). Uniqueness is per-chat (message
   * ids, the natural alternative, are chat-scoped too).
   */
  private makeTurnKey(chatId = 0): string {
    for (;;) {
      const key = (Math.random().toString(36).slice(2) + "00000000").slice(0, 8)
      if (!this.turnStore.has(`${chatId}:${key}`)) return key
    }
  }

  /** P2 (I-3): store a completed turn, evicting the oldest beyond the ring cap. */
  private storeTurn(turnKey: string, snap: TurnSnapshot): void {
    const mapKey = `${snap.chatId}:${turnKey}`
    this.turnStore.set(mapKey, snap)
    const order = this.turnOrderByTopic.get(snap.topicId) ?? []
    order.push(mapKey)
    while (order.length > StreamHandler.TURN_RING_CAP) {
      const evicted = order.shift()
      if (evicted !== undefined) this.turnStore.delete(evicted)
    }
    this.turnOrderByTopic.set(snap.topicId, order)
  }

  /**
   * P2 (I-3): server-side lookup for a Full-output tap. Undefined = unknown
   * or ring-evicted key — the caller must ack visibly (never silent).
   */
  getTurnEntry(turnKey: string, chatId: number): TurnSnapshot | undefined {
    return this.turnStore.get(`${chatId}:${turnKey}`)
  }

  /** P2 (I-3): toggle keyboards — attached/changed on tap transitions only. */
  turnExpandKeyboard(turnKey: string): InlineKeyboardButton[][] {
    return [[{ text: "Full output", callback_data: `full:${turnKey}:0` }]]
  }

  /** P2 (I-3): toggle keyboards — attached/changed on tap transitions only. */
  turnCollapseKeyboard(turnKey: string): InlineKeyboardButton[][] {
    return [[{ text: "Collapse", callback_data: `fullx:${turnKey}:0` }]]
  }

  /** P2 (I-3): record the toggle state (collapse restores collapsedHtml). */
  setTurnExpanded(turnKey: string, chatId: number, expanded: boolean): void {
    const entry = this.turnStore.get(`${chatId}:${turnKey}`)
    if (entry) entry.expanded = expanded
  }

  /**
   * P2 (I-3): compose the expanded view from stored SSE state. Telegram 4096
   * cap: single EDIT when it fits, else EDIT first chunk + NEW 2/2 follow-up;
   * beyond the 2-part budget an explicit note (never silent, never failing).
   */
  composeTurnExpansion(entry: TurnSnapshot): { main: string; followUp?: string } {
    const full = entry.fullAnswerHtml
    const shownNote = `\n\n<i>Showing full output — tap Collapse to hide.</i>`
    const shown12Note = `\n\n<i>Full output 1/2 — tap Collapse to hide.</i>`
    const contHead = `<i>Full output 2/2</i>\n`
    const overNote = `\n\n<i>(…truncated, 4096-char Telegram limit)</i>`
    if (full.length + shownNote.length <= 3900) {
      return { main: `${full}${shownNote}` }
    }
    const budget = 3900 - shown12Note.length
    const main = `${truncateForTelegram(full, budget)}${shown12Note}`
    // Remainder starts at the main chunk's raw cut (same mid-tag back-up
    // rule as truncateForTelegram), re-safed on its own. Approximate overlap
    // of a few tag-closer chars is cosmetic; send-time failures fall back to
    // an explicit note in the caller (never silent).
    let cut = budget
    const tagStart = full.lastIndexOf("<", cut)
    const tagEnd = full.lastIndexOf(">", cut)
    if (tagStart > tagEnd) cut = tagStart
    if (cut >= full.length) {
      // Degenerate (note-length arithmetic) — single EDIT covers it all.
      return { main: `${truncateForTelegram(full, 3900 - shownNote.length)}${shownNote}` }
    }
    const rest = truncateForTelegram(full.slice(cut), 3900 - contHead.length)
    const followUp = `${contHead}${rest}` + (entry.beyondBudget ? overNote : "")
    return { main, followUp }
  }

  /**
   * P2 (I-4): tool-details follow-up as a threaded reply to the final answer
   * (keeps the topic scannable). Tool list + per-tool durations + footer
   * stats from already-received SSE state. Best-effort: the final answer is
   * the receipt; failures log only.
   */
  private async postToolDetailsReply(
    sessionId: string,
    state: StreamingState,
    destination: { chatId: number; topicId: number },
    finalMessageId: number
  ): Promise<void> {
    const tools = state.toolsInvoked
    const lines: string[] = [`<b>Tools (${tools.length}):</b>`]
    const shown = tools.slice(0, StreamHandler.DETAILS_TOOL_CAP)
    for (const tool of shown) {
      const icon = this.toolKindIcon(tool.name)
      const name = this.escapeHtml(tool.name)
      const rawDetail = tool.title ? tool.title.slice(0, 80) : null
      const summary = this.toolSummaries.get(`${sessionId}:${tool.callId}`)
      const detail = rawDetail ?? summary
      const dur = tool.completedAt
        ? ` · ${this.formatElapsedShort(tool.completedAt.getTime() - tool.startedAt.getTime())}`
        : ""
      lines.push(
        detail
          ? `${icon} <code>${name}</code> — <code>${this.escapeHtml(detail)}</code><i>${dur}</i>`
          : `${icon} <code>${name}</code><i>${dur}</i>`
      )
    }
    if (tools.length > shown.length) {
      lines.push(`<i>+${tools.length - shown.length} more…</i>`)
    }
    // Footer stats (omit zero parts; tokens only when known).
    const stats: string[] = [`🔧 ${tools.length} tool${tools.length === 1 ? "" : "s"}`]
    stats.push(`⏱ ${this.formatElapsedShort(Date.now() - state.startedAt.getTime())}`)
    if (state.tokens) {
      stats.push(`📊 ${this.formatTokenCount(state.tokens.input + state.tokens.output)}`)
    }
    lines.push("", `<i>${stats.join(" • ")}</i>`)
    try {
      await this.sendCallback(
        destination.chatId,
        destination.topicId,
        lines.join("\n"),
        { parseMode: "HTML", replyToMessageId: finalMessageId }
      )
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error)
      console.log(`[StreamHandler] Tool-details reply failed (${msg.slice(0, 80)}) — final answer above is unaffected`)
    }
  }

  /**
   * Handle session error (S10)
   */
  private async handleSessionError(
    sessionId: string,
    event: SSEEvent,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    const props = event.properties as {
      sessionID: string
      error: string
    }

    const state = this.states.get(sessionId)
    if (state) {
      state.isProcessing = false
      state.error = props.error
    }

    await this.postStoppedAndErrorCard(sessionId, destination, props.error ?? "unknown error")

    // Clean up state
    this.states.delete(sessionId)
    this.clearToolSummaries(sessionId)
    this.lastRenderedText.delete(sessionId)
  }

  /**
   * Handle session status update
   */
  private async handleSessionUpdated(
    sessionId: string,
    event: SSEEvent,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    const props = event.properties as {
      sessionID: string
      status: "idle" | "running" | "error"
    }

    if (props.status === "running") {
      // Session started processing — post the live progress card immediately
      // (S1: NEW with ⏹ Cancel) so tap → silence never exceeds ~1 s. Later
      // S2–S5 deltas EDIT the same card via the throttled path.
      let state = this.states.get(sessionId)
      if (!state) {
        state = this.createState(sessionId)
        this.states.set(sessionId, state)
      }
      state.isProcessing = true
      await this.updateTelegram(sessionId, state, destination, true)
    } else if (props.status === "error") {
      // S19: session.updated status=error (distinct from session.error event)
      // — S10-lite: park the progress card as Stopped, then NEW error card
      // with Retry (final priority, never dropped).
      await this.postStoppedAndErrorCard(
        sessionId,
        destination,
        ("error" in props && typeof (props as { error?: unknown }).error === "string"
          ? (props as { error: string }).error
          : null) ?? "unknown error",
        "Session error"
      )
      const errState = this.states.get(sessionId)
      if (errState) errState.isProcessing = false
      this.clearFlushTimer(sessionId)
    }
  }

  /**
   * Handle permission request from OpenCode (H2: envelope validation + reminder)
   */
  private async handlePermissionUpdated(
    event: SSEEvent,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    // Defensively unwrap: live envelope may nest the permission object.
    // NOTE: the live `permission.asked` flat shape carries a STRING field
    // `permission` (e.g. "external_directory"), so a naive
    // `raw.permission ?? raw` unwrap would yield that string instead of the
    // object. Only unwrap when nested value is actually an object.
    const raw = event.properties as Record<string, any>
    const nested = raw?.permission
    const candidate = (
      nested && typeof nested === "object" ? nested : raw
    ) as Partial<Permission> & Record<string, any>
    const permission = candidate as Permission

    if (!permission || typeof permission.id !== "string" || !permission.id) {
      console.error(
        `[StreamHandler] permission.updated with missing/invalid id, raw shape: ${JSON.stringify(raw).slice(0, 500)}`
      )
      // F3: never leave the session blocked silently on an unknown envelope —
      // best-effort fallback notice (the H2 reminder path can't run without an id).
      try {
        await this.sendCallback(destination.chatId, destination.topicId,
          `⚠️ <b>OpenCode needs permission</b> but the request couldn't be displayed. Please resend your last message.`,
          {
            parseMode: "HTML",
            inlineKeyboard: [
              [{ text: "🔁 Resend last message", callback_data: `retry:${destination.topicId}` }],
            ],
          })
      } catch { /* best-effort notice */ }
      return
    }
    if (!permission.sessionID) {
      console.error(
        `[StreamHandler] permission.updated ${permission.id} missing sessionID, raw shape: ${JSON.stringify(raw).slice(0, 500)}`
      )
      // F3: same fallback — session is blocked until answered, so stay loud.
      try {
        await this.sendCallback(destination.chatId, destination.topicId,
          `⚠️ <b>OpenCode needs permission</b> but the request couldn't be displayed. Please resend your last message.`,
          {
            parseMode: "HTML",
            inlineKeyboard: [
              [{ text: "🔁 Resend last message", callback_data: `retry:${destination.topicId}` }],
            ],
          })
      } catch { /* best-effort notice */ }
      return
    }

    // Live `permission.asked` flat shape uses `permission` (kind string) and
    // `patterns` where Permission uses `type`/`pattern`/`title`. Coalesce so
    // the card renders — critically, formatPermissionMessage calls escapeHtml
    // on type/title, which would throw on undefined and leave the session
    // blocked with no card. No mutation of the stored shape beyond display.
    const kind =
      permission.type ??
      (typeof raw.permission === "string" ? raw.permission : undefined) ??
      "unknown"
    const flatPattern = (raw.patterns ?? raw.pattern) as unknown
    const displayTitle =
      permission.title ??
      (kind !== "unknown" ? `${kind} access request` : "Permission request")
    const display: Permission = {
      ...permission,
      type: kind,
      title: displayTitle,
      ...(flatPattern !== undefined && permission.pattern === undefined
        ? { pattern: flatPattern as string | string[] }
        : {}),
    }

    // Task 05: allowlist check — AFTER envelope validation, BEFORE card send.
    // Default-deny: only an explicit rule match auto-answers "once"; unknown
    // kinds keep the ask flow below byte-identical. True = no card (answered,
    // duplicate-suppressed, or loudly notified); false = fall through to card
    // (only when no API path is configured — safe ask).
    if (this.isPermissionAutoAllowed(kind)) {
      if (await this.tryAutoAllowPermission(display, kind, destination)) {
        return
      }
    }

    console.log(`[StreamHandler] Permission request (${event.type}): ${display.type} - ${display.title}`)

    // Format permission message
    const messageText = this.formatPermissionMessage(display)

    // Create inline keyboard with approve/deny buttons
    const keyboard: InlineKeyboardButton[][] = [
      [
        { text: "✅ Allow Once", callback_data: `perm:once:${permission.id}` },
        { text: "✅ Always Allow", callback_data: `perm:always:${permission.id}` },
      ],
      [
        { text: "❌ Deny", callback_data: `perm:reject:${permission.id}` },
      ],
    ]

    // Defensive dedupe: `.asked` and `.updated` may both fire for one request.
    // Refresh the stored entry and edit the existing card in place — never
    // post a second card. If no card was ever posted, skip resend (the
    // reminder/validation paths already cover loud fallback).
    const existing = this.pendingPermissions.get(permission.id)
    if (existing) {
      existing.permission = display
      if (existing.telegramMessageId) {
        try {
          await this.sendCallback(
            existing.chatId,
            existing.topicId,
            messageText,
            {
              parseMode: "HTML",
              inlineKeyboard: keyboard,
              editMessageId: existing.telegramMessageId,
            }
          )
        } catch {
          // Keep the original card; edit failures must not duplicate it.
        }
      }
      this.clearPermissionTimer(permission.id)
      this.schedulePermissionReminder(permission.id)
      return
    }

    try {
      const result = await this.sendCallback(
        destination.chatId,
        destination.topicId,
        messageText,
        {
          parseMode: "HTML",
          inlineKeyboard: keyboard,
        }
      )

      // Store pending permission for later resolution + schedule one reminder (H2)
      this.clearPermissionTimer(permission.id)
      this.pendingPermissions.set(permission.id, {
        permission: display,
        telegramMessageId: result.messageId,
        chatId: destination.chatId,
        topicId: destination.topicId,
        createdAt: Date.now(),
        reminderSent: false,
      })
      this.schedulePermissionReminder(permission.id)
    } catch (error) {
      console.error(`[StreamHandler] Failed to send permission prompt:`, error)
    }
  }

  /**
   * H2: single reminder if a permission sits unanswered (session is blocked
   * until answered, so silence here looks exactly like silent-stop).
   * No auto-deny here — StreamHandler has no OpenCode client; the resolve
   * path in integration.ts notifies loudly instead. Timer is stdlib only.
   */
  private schedulePermissionReminder(permissionId: string): void {
    const REMINDER_AFTER_MS = 5 * 60 * 1000 // 5 min
    const timer = setTimeout(async () => {
      const pending = this.pendingPermissions.get(permissionId)
      if (!pending || pending.reminderSent) return
      pending.reminderSent = true
      console.log(`[StreamHandler] Permission ${permissionId} still pending after 5min, sending reminder`)
      try {
        await this.sendCallback(
          pending.chatId,
          pending.topicId,
          `⏳ <b>Still waiting:</b> ${this.escapeHtml(pending.permission.title)}\n` +
          `<i>Tap Allow/Deny above — the session is paused until you answer.</i>`,
          { parseMode: "HTML" }
        )
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error)
        console.error(`[StreamHandler] Permission reminder send failed (${permissionId}): ${msg.slice(0, 120)}`)
      }
    }, REMINDER_AFTER_MS)
    // Don't keep the process alive just for a reminder.
    if (typeof (timer as unknown as { unref?: () => void }).unref === "function") {
      (timer as unknown as { unref: () => void }).unref()
    }
    this.permissionTimers.set(permissionId, timer)
  }

  private clearPermissionTimer(permissionId: string): void {
    const timer = this.permissionTimers.get(permissionId)
    if (timer) {
      clearTimeout(timer)
      this.permissionTimers.delete(permissionId)
    }
  }

  /**
   * Handle permission reply confirmation
   */
  private async handlePermissionReplied(event: SSEEvent): Promise<void> {
    const props = event.properties as {
      sessionID: string
      permissionID: string
      response: string
    }

    console.log(`[StreamHandler] Permission ${props.permissionID} replied: ${props.response}`)

    // Clean up pending permission
    const pending = this.pendingPermissions.get(props.permissionID)
    if (pending) {
      // Receipt (S7): wording matches the button labels exactly; keyboard
      // stripped on edit (no buttons on receipts).
      if (pending.telegramMessageId) {
        try {
          const responseText = props.response === "reject"
            ? "❌ Permission denied"
            : props.response === "always"
              ? "✅ Permission granted (always)"
              : "✅ Permission granted (once)"
          await this.sendCallback(
            pending.chatId,
            pending.topicId,
            `${responseText}\n\n<i>${this.escapeHtml(pending.permission.title)}</i>`,
            {
              parseMode: "HTML",
              editMessageId: pending.telegramMessageId,
              inlineKeyboard: [],
            }
          )
        } catch {
          // Ignore edit errors
        }
      }
      this.clearPermissionTimer(props.permissionID)
      this.pendingPermissions.delete(props.permissionID)
    }
  }

  /**
   * Format a permission request message for Telegram (P1/S6: per-kind copy).
   * Header states the consequence; unknown kinds keep the generic card
   * verbatim — never hide an unknown kind behind a pretty template.
   */
  private formatPermissionMessage(permission: Permission): string {
    const kind = (permission.type ?? "unknown").toLowerCase()
    const meta = (permission.metadata ?? {}) as Record<string, unknown>
    const metaStr = (v: unknown): string | null =>
      typeof v === "string" && v ? v : null
    const patternStr = permission.pattern
      ? (Array.isArray(permission.pattern) ? permission.pattern.join(", ") : String(permission.pattern))
      : null
    const header = `<b>🔐 Permission needed</b>\nSession paused — answer to continue.`

    const withPattern = (lines: string[]): string[] =>
      patternStr ? [...lines, `<b>Pattern:</b> <code>${this.escapeHtml(patternStr)}</code>`] : lines

    if (kind === "bash") {
      const cmd = metaStr(meta.command) ?? patternStr ?? permission.title
      return withPattern([
        header,
        ``,
        `<b>Run command?</b>`,
        `<pre>${this.escapeHtml(cmd.slice(0, 300))}</pre>`,
        `<b>Kind:</b> <code>bash</code>`,
      ]).join("\n")
    }
    if (kind === "edit" || kind === "write") {
      const path = metaStr(meta.path) ?? patternStr ?? permission.title
      const lines = [header, ``, `<b>Edit file?</b>`, `<b>Path:</b> <code>${this.escapeHtml(path)}</code>`]
      const diffstat = metaStr(meta.diffstat) ?? metaStr(meta.diffStat) ?? metaStr(meta.stat)
      if (diffstat) lines.push(`<i>${this.escapeHtml(diffstat.slice(0, 120))}</i>`)
      return withPattern(lines).join("\n")
    }
    if (kind === "read") {
      const path = metaStr(meta.path) ?? patternStr ?? permission.title
      return withPattern([
        header,
        ``,
        `<b>Read file?</b>`,
        `<b>Path:</b> <code>${this.escapeHtml(path)}</code>`,
      ]).join("\n")
    }
    if (kind === "external_directory") {
      const path = metaStr(meta.path) ?? patternStr ?? permission.title
      return withPattern([
        header,
        ``,
        `<b>Access outside project?</b>`,
        `<b>Path:</b> <code>${this.escapeHtml(path)}</code>`,
        `<i>Only allow if you recognise this path.</i>`,
      ]).join("\n")
    }
    if (kind === "webfetch" || kind === "fetch" || kind.startsWith("network")) {
      const url = metaStr(meta.url) ?? metaStr(meta.href) ?? metaStr(meta.host) ?? patternStr ?? permission.title
      return withPattern([
        header,
        ``,
        `<b>Fetch URL?</b>`,
        `<code>${this.escapeHtml(url.slice(0, 300))}</code>`,
      ]).join("\n")
    }

    // Unknown kind: generic card verbatim (current shape, header upgraded).
    const parts: string[] = []
    parts.push(header)
    parts.push("")
    parts.push(`<b>Type:</b> <code>${this.escapeHtml(permission.type)}</code>`)
    parts.push(`<b>Action:</b> ${this.escapeHtml(permission.title)}`)
    if (patternStr) {
      parts.push(`<b>Pattern:</b> <code>${this.escapeHtml(patternStr)}</code>`)
    }
    const command = metaStr(meta.command)
    if (command) {
      parts.push("")
      parts.push(`<pre>${this.escapeHtml(command)}</pre>`)
    }
    const path = metaStr(meta.path)
    if (path) {
      parts.push(`<b>Path:</b> <code>${this.escapeHtml(path)}</code>`)
    }
    return parts.join("\n")
  }

  /**
   * Get a pending permission by ID
   */
  getPendingPermission(permissionId: string): PendingPermission | undefined {
    return this.pendingPermissions.get(permissionId)
  }

  /**
   * Remove a pending permission (after it's been handled)
   */
  removePendingPermission(permissionId: string): void {
    this.clearPermissionTimer(permissionId)
    this.pendingPermissions.delete(permissionId)
  }

  /**
   * Task 05: configure which permission kinds auto-answer "once".
   * Matching is case-insensitive exact-match on the coalesced kind;
   * anything not listed (including unknown kinds) keeps the ask flow.
   */
  setPermissionsAutoAllow(kinds: string[]): void {
    this.permissionsAutoAllow = new Set(kinds.map((k) => k.toLowerCase()))
  }

  /**
   * Task 05: inject the OpenCode responder used for auto-allow.
   * Owned by integration.ts so TUI/discovered/restarted sessions resolve
   * identically to manual Allow-Once. Resolves true when "once" was accepted.
   */
  setPermissionAutoResponder(
    responder: (
      sessionId: string,
      permissionId: string,
      destination: { chatId: number; topicId: number }
    ) => Promise<boolean>
  ): void {
    this.permissionAutoResponder = responder
  }

  /**
   * Task 05: default-deny matcher — true only on explicit rule match.
   */
  isPermissionAutoAllowed(kind: string): boolean {
    return this.permissionsAutoAllow.has(kind.toLowerCase())
  }

  /**
   * Task 05: auto-answer a matching permission with "once" via the injected
   * responder. Skips the card, never creates a pending entry or reminder
   * timer. Returns true when no card should be created (answered,
   * duplicate-suppressed, or loudly notified on failure); false only when no
   * API path is configured, letting the caller fall through to the ask flow.
   */
  private async tryAutoAllowPermission(
    display: Permission,
    kind: string,
    destination: { chatId: number; topicId: number }
  ): Promise<boolean> {
    const permissionId = display.id
    const sessionId = display.sessionID

    // Duplicate suppression: `.asked` and `.updated` may both fire for one
    // request — the first success already answered "once", never call twice.
    if (this.autoAllowedPermissionIds.has(permissionId)) {
      console.log(`[StreamHandler] Permission ${permissionId} already auto-allowed, skipping duplicate`)
      return true
    }

    // No API path (integration didn't inject) — fall through to the card.
    if (!this.permissionAutoResponder) {
      console.warn(`[StreamHandler] Permission ${permissionId} matched allowlist (${kind}) but no auto-responder is configured — falling through to ask flow`)
      return false
    }

    const patternStr = Array.isArray(display.pattern)
      ? display.pattern.join(", ")
      : (display.pattern ?? "")
    // LOUD log: kind + pattern/title + session only — never metadata/secrets.
    console.log(
      `[StreamHandler] ✅ AUTO-ALLOW permission ${permissionId} kind="${kind}" title="${String(display.title).slice(0, 200)}"` +
      (patternStr ? ` pattern="${String(patternStr).slice(0, 200)}"` : "") +
      ` session=${sessionId}`
    )

    let ok = false
    try {
      ok = await this.permissionAutoResponder(sessionId, permissionId, destination)
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error)
      console.error(`[StreamHandler] Auto-allow responder threw for ${permissionId}: ${msg.slice(0, 120)}`)
      ok = false
    }

    if (!ok) {
      // LOUD fallback (H2 expired pattern): never leave the session blocked silently.
      console.error(`[StreamHandler] Auto-allow failed for ${permissionId} (session ${sessionId}) — notifying topic to resend`)
      try {
        await this.sendCallback(
          destination.chatId,
          destination.topicId,
          `⚠️ Permission request expired: the OpenCode session restarted and the pending approval can no longer be answered.\n\n` +
          `Please resend your message.`,
          {
            parseMode: "HTML",
            inlineKeyboard: [
              [{ text: "🔁 Resend last message", callback_data: `retry:${destination.topicId}` }],
            ],
          }
        )
      } catch { /* best-effort notice */ }
      return true
    }

    // Success: remember (duplicate suppression, capped) + clear any raced
    // card/timer like a manual allow, then a subtle (button-free) chat note.
    this.autoAllowedPermissionIds.add(permissionId)
    if (this.autoAllowedPermissionIds.size > 200) {
      const oldest = this.autoAllowedPermissionIds.values().next().value
      if (oldest !== undefined) this.autoAllowedPermissionIds.delete(oldest)
    }
    const raced = this.pendingPermissions.get(permissionId)
    if (raced) {
      if (raced.telegramMessageId) {
        try {
          await this.sendCallback(
            raced.chatId,
            raced.topicId,
            `<i>Permission Approved (auto-allowed)</i>`,
            { parseMode: "HTML", editMessageId: raced.telegramMessageId }
          )
        } catch { /* keep the original card on edit failure */ }
      }
      this.clearPermissionTimer(permissionId)
      this.pendingPermissions.delete(permissionId)
    }
    try {
      await this.sendCallback(
        destination.chatId,
        destination.topicId,
        `✅ Auto-allowed <code>${this.escapeHtml(kind)}</code> — ${this.escapeHtml(String(display.title))}`,
        { parseMode: "HTML" }
      )
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error)
      console.error(`[StreamHandler] Auto-allow note send failed (${permissionId}): ${msg.slice(0, 120)}`)
    }
    return true
  }

  /**
   * Get all pending permissions
   */
  getAllPendingPermissions(): Map<string, PendingPermission> {
    return this.pendingPermissions
  }

  // ===========================================================================
  // Telegram Updates
  // ===========================================================================

  /**
   * Update Telegram if enough time has passed since last update
   */
  private async maybeUpdateTelegram(
    sessionId: string,
    state: StreamingState,
    destination: { chatId: number; topicId: number }
  ): Promise<void> {
    const now = Date.now()
    const lastUpdate = state.lastTelegramUpdateAt?.getTime() ?? 0
    
    // Telegram is strict about message edits (~1/sec per message is safe);
    // both streaming and non-streaming paths share the same 1s floor.
    // The `streamingEnabled` branch lives in effectiveUpdateIntervalMs so a
    // future split is easy.
    const updateInterval = this.effectiveUpdateIntervalMs(sessionId)
    
    if (now - lastUpdate >= updateInterval) {
      await this.updateTelegram(sessionId, state, destination, false)
    } else {
      // Throttled: schedule a one-shot trailing flush so a paused tail still
      // renders promptly without extra edits during bursts.
      this.scheduleTrailingFlush(sessionId, state, destination, updateInterval)
    }
  }

  /**
   * Effective minimum interval between Telegram edits for a session.
   * Both paths resolve to the same 1s floor today (streaming and non-streaming
   * unified); the `streamingEnabled` branch is kept so a future split is easy.
   */
  private effectiveUpdateIntervalMs(sessionId: string): number {
    return this.isStreamingEnabled(sessionId) ? 1000 : this.config.updateIntervalMs
  }

  /**
   * Schedule a one-shot trailing flush ~1 interval later that renders the
   * latest state only if still dirty and the session is still active.
   * Never stacks: at most one pending timer per session (replaced on newer
   * events). Stdlib setTimeout only — no new dependencies.
   */
  private scheduleTrailingFlush(
    sessionId: string,
    state: StreamingState,
    destination: { chatId: number; topicId: number },
    delayMs: number
  ): void {
    this.clearFlushTimer(sessionId)
    const timer = setTimeout(() => {
      this.flushTimers.delete(sessionId)
      // Session still active? (unregistered / idle-rendered / cleared → skip)
      const dest = this.sessionToTelegram.get(sessionId) ?? destination
      if (!this.sessionToTelegram.has(sessionId)) return
      const current = this.states.get(sessionId)
      if (!current || current !== state) return
      // Re-check the throttle window: an intermediate render already showed
      // everything this timer knew about, so there is nothing unrendered.
      const lastUpdate = current.lastTelegramUpdateAt?.getTime() ?? 0
      if (Date.now() - lastUpdate < this.effectiveUpdateIntervalMs(sessionId)) return
      // updateTelegram re-checks dirty state; all error handling lives there.
      void this.updateTelegram(sessionId, current, dest, false)
    }, delayMs)
    // Don't keep the process alive just for a flush.
    if (typeof (timer as unknown as { unref?: () => void }).unref === "function") {
      (timer as unknown as { unref: () => void }).unref()
    }
    this.flushTimers.set(sessionId, timer)
  }

  /**
   * Cancel any pending trailing flush for a session (idempotent).
   */
  private clearFlushTimer(sessionId: string): void {
    const timer = this.flushTimers.get(sessionId)
    if (timer) {
      clearTimeout(timer)
      this.flushTimers.delete(sessionId)
    }
  }

  /**
   * Send/update progress message in Telegram
   */
  private async updateTelegram(
    sessionId: string,
    state: StreamingState,
    destination: { chatId: number; topicId: number },
    force: boolean
  ): Promise<void> {
    // Skip if we're already waiting for a message to be sent
    if (state.pendingSend) {
      return
    }
    
    const progressText = this.formatProgressMessage(state, sessionId)
    // P1 (E1/S1): in-flight progress carries a ⏹ Cancel button, attached on
    // card creation only (edits omit markup to stay inside the ~1/s budget).
    // sessShort = sessionId slice 0:8 — server resolves the full session by
    // topic, keeping callback_data far under 64 bytes. Legacy full-id
    // callbacks remain accepted by the topic-resolved handler.
    const sessShort = sessionId.slice(0, 8)
    const cancelKeyboard: InlineKeyboardButton[][] = [
      [{ text: "⏹ Cancel", callback_data: `cancel:${sessShort}` }],
    ]
    // Dirty check: skip the edit when the formatted text is unchanged since
    // the last successful render — avoids useless edits burning rate budget.
    // (Backstop: the "message is not modified" catch below.)
    if (state.telegramMessageId && progressText === this.lastRenderedText.get(sessionId)) {
      return
    }
    
    try {
      if (state.telegramMessageId) {
        // Edit existing message (progress class: skippable when saturated;
        // finals/cards/notices default to the never-drop class). The Cancel
        // keyboard rides this already-happening text edit (zero extra calls,
        // dirty-check still gates) so a card stripped by a prior receipt
        // re-arms on the next turn's first progress edit.
        await this.sendCallback(
          destination.chatId,
          destination.topicId,
          progressText,
          {
            parseMode: "HTML",
            editMessageId: state.telegramMessageId,
            queuePriority: "progress",
            inlineKeyboard: state.isProcessing ? cancelKeyboard : [],
          }
        )
      } else {
        // Mark that we're sending to prevent duplicate sends
        state.pendingSend = true
        try {
          // Send new message (P1: attach ⏹ Cancel at creation; progress
          // priority — skippable when saturated, never-drop lane is for finals)
          const result = await this.sendCallback(
            destination.chatId,
            destination.topicId,
            progressText,
            { parseMode: "HTML", queuePriority: "progress", inlineKeyboard: cancelKeyboard }
          )
          state.telegramMessageId = result.messageId
        } finally {
          state.pendingSend = false
        }
      }
      
      state.lastTelegramUpdateAt = new Date()
      this.lastRenderedText.set(sessionId, progressText)
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error)
      
      // Ignore "message is not modified" errors - this just means content is the same
      if (errorMsg.includes('message is not modified')) {
        state.lastTelegramUpdateAt = new Date()
        this.lastRenderedText.set(sessionId, progressText)
        return
      }
      
      // For rate limit errors, just skip this update - don't send new message
      if (errorMsg.includes('429') || errorMsg.includes('Too Many Requests')) {
        console.log(`[StreamHandler] Rate limited, skipping update`)
        // Update the timestamp to prevent immediate retry
        state.lastTelegramUpdateAt = new Date()
        return
      }
      
      // Only send new message if the original was deleted (not for other errors)
      // Check for "message to edit not found" or similar
      if (state.telegramMessageId && errorMsg.includes('message to edit not found')) {
        console.log(`[StreamHandler] Original message deleted, sending new one`)
        state.telegramMessageId = undefined // Clear the old ID
        try {
          const result = await this.sendCallback(
            destination.chatId,
            destination.topicId,
            progressText,
            { parseMode: "HTML", queuePriority: "progress", inlineKeyboard: cancelKeyboard }
          )
          state.telegramMessageId = result.messageId
          state.lastTelegramUpdateAt = new Date()
          this.lastRenderedText.set(sessionId, progressText)
        } catch {
          // Give up on this update
        }
      } else {
        // For other errors, just log and skip
        console.log(`[StreamHandler] Edit failed (${errorMsg.slice(0, 80)}), skipping`)
      }
    }
  }

  /**
   * Format a progress message for Telegram
   */
  private formatProgressMessage(state: StreamingState, sessionId: string): string {
    const streamingEnabled = this.isStreamingEnabled(sessionId)
    const parts: string[] = []

    // Current tool status
    const runningTools = state.toolsInvoked.filter((t) => !t.completedAt)
    const completedTools = state.toolsInvoked.filter((t) => t.completedAt)

    // In streaming mode, show status on one line at top
    if (streamingEnabled) {
      const statusParts: string[] = []
      if (runningTools.length > 0 && this.config.showToolNames) {
        const toolName = runningTools[runningTools.length - 1].name
        statusParts.push(`🔧 ${toolName}`)
      } else if (state.isProcessing && !state.currentText.trim()) {
        statusParts.push("💭 Thinking...")
      }
      
      const elapsed = Math.round((Date.now() - state.startedAt.getTime()) / 1000)
      if (elapsed > 0) {
        statusParts.push(`${elapsed}s`)
      }
      
      if (statusParts.length > 0) {
        parts.push(`<i>${statusParts.join(" | ")}</i>`)
        parts.push("")
      }

      // In streaming mode, show full text converted to HTML (truncated to Telegram limit)
      if (state.currentText.trim()) {
        let text = state.currentText.trim()
        // Telegram message limit is ~4096 chars, leave room for status
        const maxLength = 3600
        if (text.length > maxLength) {
          text = text.slice(-maxLength) // Show the END (most recent) text
          text = "..." + text
        }
        // Convert markdown to HTML for proper rendering during streaming
        const htmlText = markdownToTelegramHtml(text)
        parts.push(truncateForTelegram(htmlText, 3800))
      }
    } else {
      // Non-streaming mode: show detailed status with tokens, tools, and text
      
      // === Header: Status + Time + Tokens ===
      const elapsed = Math.round((Date.now() - state.startedAt.getTime()) / 1000)
      const headerParts: string[] = []
      
      // Status indicator
      if (runningTools.length > 0) {
        headerParts.push("⏳ Working")
      } else if (state.isProcessing) {
        headerParts.push("💭 Thinking")
      } else {
        headerParts.push("✅ Done")
      }
      
      // Elapsed time
      if (elapsed > 0) {
        const mins = Math.floor(elapsed / 60)
        const secs = elapsed % 60
        headerParts.push(mins > 0 ? `${mins}m ${secs}s` : `${secs}s`)
      }
      
      // Token count
      if (state.tokens) {
        const totalTokens = state.tokens.input + state.tokens.output
        const tokenStr = this.formatTokenCount(totalTokens)
        headerParts.push(`📊 ${tokenStr}`)
      }
      
      parts.push(`<b>${headerParts.join(" • ")}</b>`)
      
      // === Tools Section ===
      if (state.toolsInvoked.length > 0) {
        parts.push("")
        parts.push("<b>Tools:</b>")

        // P2 (S3/S4/S20): one line per tool, titles only (never bodies).
        // Running: ⏳ name — kind summary. Completed: ✅ name — title (≤50)
        // else the running-line summary. Cap 8 lines, collapse the rest.
        const shown = state.toolsInvoked.slice(0, 8)
        for (const tool of shown) {
          const toolName = this.escapeHtml(tool.name)
          if (tool.completedAt) {
            const summary = this.toolSummaries.get(`${state.sessionId}:${tool.callId}`)
            const title = tool.title
              ? this.escapeHtml(tool.title.slice(0, 50))
              : summary
                ? this.escapeHtml(summary)
                : null
            parts.push(
              title
                ? `✅ <code>${toolName}</code> - ${title}`
                : `✅ <code>${toolName}</code>`
            )
          } else {
            const summary = this.toolSummaries.get(`${state.sessionId}:${tool.callId}`)
            parts.push(
              summary
                ? `⏳ <code>${toolName}</code> - <code>${this.escapeHtml(summary)}</code>`
                : `⏳ <code>${toolName}</code>`
            )
          }
        }
        const hidden = state.toolsInvoked.length - shown.length
        if (hidden > 0) {
          parts.push(`<i>+${hidden} more tools…</i>`)
        }
      }
      
      // === Response Text (most recent, truncated) ===
      if (state.currentText.trim()) {
        parts.push("")
        parts.push("<b>Response:</b>")
        
        let text = state.currentText.trim()
        
        // Calculate available space for text
        // Telegram limit is 4096, leave room for header/tools section
        const headerLength = parts.join("\n").length
        const maxTextLength = Math.max(500, 3800 - headerLength)
        
        if (text.length > maxTextLength) {
          // Show the END (most recent) text with ellipsis at start
          text = "..." + text.slice(-maxTextLength)
        }
        
        // Convert markdown to HTML for proper rendering
        const htmlText = markdownToTelegramHtml(text)
        parts.push(truncateForTelegram(htmlText, maxTextLength))
      }
    }

    return parts.join("\n") || "<i>Processing...</i>"
  }

  /**
   * Escape HTML special characters
   */
  private escapeHtml(text: string): string {
    return text
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
  }

  /**
   * P2 (S3): kind icon for a tool line. Running lines in the progress card
   * keep the literal ⏳/✅ sketches (§1-S3/S4); these icons identify kinds in
   * the threaded details reply (tool lines only, per the emoji table).
   */
  private toolKindIcon(name: string): string {
    const kind = name.toLowerCase()
    if (kind === "bash" || kind.includes("shell") || kind.includes("command")) return "⌨"
    if (kind === "read") return "📖"
    if (kind === "edit") return "✏"
    if (kind === "write") return "📝"
    if (kind === "webfetch" || kind === "fetch" || kind.startsWith("network")) return "🌐"
    return "🔧"
  }

  /**
   * P2 (S3): one-line per-kind summary from already-received tool args.
   * bash → first 60 chars of command; read/edit/write → basename or last 2
   * path segments; webfetch → host/path; other/unknown → null (never dump
   * raw args). Returns raw text (escaped at render); null = bare name line.
   */
  private summarizeToolCall(tool: string, args?: Record<string, unknown>): string | null {
    if (!args) return null
    const kind = tool.toLowerCase()
    const str = (v: unknown): string | null =>
      typeof v === "string" && v ? v : null
    const get = (...keys: string[]): string | null => {
      for (const key of keys) {
        const v = str(args[key])
        if (v) return v
      }
      return null
    }
    if (kind === "bash" || kind.includes("shell") || kind.includes("command")) {
      const cmd = get("command", "cmd", "script")
      return cmd ? cmd.slice(0, 60) : null
    }
    if (kind === "read" || kind === "edit" || kind === "write") {
      const p = get("path", "file", "filePath", "filename")
      return p ? this.shortenPath(p) : null
    }
    if (kind === "webfetch" || kind === "fetch" || kind.startsWith("network")) {
      const u = get("url", "href", "host")
      return u ? this.shortenUrl(u) : null
    }
    return null
  }

  /** Last-2-segments path shortener (cap ~60 chars). */
  private shortenPath(p: string): string {
    if (p.length <= 60) {
      const segs = p.split("/").filter(Boolean)
      return segs.length > 2 ? segs.slice(-2).join("/") : p
    }
    return p.slice(0, 57) + "…"
  }

  /** Host-first URL shortener (cap ~60 chars). */
  private shortenUrl(u: string): string {
    try {
      const parsed = new URL(u.includes("://") ? u : `https://${u}`)
      const short = parsed.host + (parsed.pathname !== "/" ? parsed.pathname : "")
      return short.length <= 60 ? short : short.slice(0, 57) + "…"
    } catch {
      return u.slice(0, 60)
    }
  }

  /** Elapsed `Ss` / `Mm Ss` (matches the progress-card vocabulary). */
  private formatElapsedShort(ms: number): string {
    const secs = Math.max(0, Math.round(ms / 1000))
    if (secs < 60) return `${secs}s`
    return `${Math.floor(secs / 60)}m ${secs % 60}s`
  }

  /** Drop one session's tool summaries (called wherever its state is deleted). */
  private clearToolSummaries(sessionId: string): void {
    const prefix = `${sessionId}:`
    for (const key of Array.from(this.toolSummaries.keys())) {
      if (key.startsWith(prefix)) this.toolSummaries.delete(key)
    }
  }

  /**
   * Format token count for display (e.g., 1.2k, 15.3k)
   */
  private formatTokenCount(tokens: number): string {
    if (tokens < 1000) {
      return `${tokens} tokens`
    } else if (tokens < 10000) {
      return `${(tokens / 1000).toFixed(1)}k tokens`
    } else {
      return `${Math.round(tokens / 1000)}k tokens`
    }
  }

  // ===========================================================================
  // State Management
  // ===========================================================================

  /**
   * Create initial streaming state for a session
   */
  private createState(sessionId: string): StreamingState {
    return {
      sessionId,
      currentText: "",
      toolsInvoked: [],
      startedAt: new Date(),
      isProcessing: false,
    }
  }

  /**
   * Get current state for a session
   */
  getState(sessionId: string): StreamingState | undefined {
    return this.states.get(sessionId)
  }

  /**
   * Check if a session is currently processing
   */
  isProcessing(sessionId: string): boolean {
    return this.states.get(sessionId)?.isProcessing ?? false
  }

  /**
   * Mark a session's turn as aborted (Task 02 cancel path).
   * Resets isProcessing and clears any trailing flush so the idle path
   * treats the turn as finished — never stuck. Keeps the session
   * registered so the next message still works (no unregisterSession).
   */
  markAborted(sessionId: string): void {
    this.clearFlushTimer(sessionId)
    const state = this.states.get(sessionId)
    if (state) {
      state.isProcessing = false
    }
  }

  /**
   * Get all active sessions
   */
  getActiveSessions(): string[] {
    return Array.from(this.states.keys())
  }

  /**
   * Clear all state (for shutdown)
   */
  clear(): void {
    for (const timer of this.permissionTimers.values()) {
      clearTimeout(timer)
    }
    this.permissionTimers.clear()
    for (const timer of this.flushTimers.values()) {
      clearTimeout(timer)
    }
    this.flushTimers.clear()
    this.lastRenderedText.clear()
    this.toolSummaries.clear()
    this.turnStore.clear()
    this.turnOrderByTopic.clear()
    this.states.clear()
    this.sessionToTelegram.clear()
    this.sessionStreamingEnabled.clear()
    this.pendingPermissions.clear()
    this.messageRoles.clear()
    this.sentUserMessages.clear()
    this.messagesFromTelegram.clear()
  }
}

/**
 * Create a stream handler with the given callbacks
 */
export function createStreamHandler(
  sendCallback: TelegramSendCallback,
  deleteCallback?: TelegramDeleteCallback,
  config?: Partial<StreamHandlerConfig>
): StreamHandler {
  return new StreamHandler(sendCallback, deleteCallback, config)
}
