/**
 * Integration Layer
 * 
 * Wires together all components:
 * - Telegram bot (grammY)
 * - Forum topic manager
 * - OpenCode instance orchestrator
 * - SSE stream handler
 */

import { Bot, type Context } from "grammy"
import type { AppConfig } from "./config"
import { toManagerConfig, toTopicManagerConfig } from "./config"
import { InstanceManager, type OrchestratorEvent, type InstanceInfo } from "./orchestrator"
import { TopicManager } from "./forum/topic-manager"
import { TopicStore } from "./forum/topic-store"
import { 
  createForumHandlers, 
  createForumCommands,
  sendToTopic,
  type ActiveSessionInfo,
  type ConnectResult,
  type DisconnectResult,
  type CreateTopicResult,
  type ManagedProjectInfo,
} from "./bot/handlers/forum"
import { 
  OpenCodeClient, 
  StreamHandler,
  buildTelegramSystemPrompt,
  discoverSessions,
  isPortAlive,
  findSession,
  type SSEEvent,
  type TelegramSendCallback,
  type TelegramDeleteCallback,
  type DiscoveredSession,
} from "./opencode"
import type { IOpenCodeClient, ResponseHandler, ForumMessageContext, MessageRouteResult } from "./types/forum"
import { ApiServer, createApiServer } from "./api-server"
import { TelegramSendQueue } from "./telegram/send-queue"
import { BOT_LOCKFILE_PATH, isTelegramConflictError } from "./bot-guard"

// =============================================================================
// Types
// =============================================================================

/**
 * Integrated application instance
 */
export interface IntegratedApp {
  /** grammY bot instance */
  bot: Bot
  
  /** Topic manager for forum topic → session mapping */
  topicManager: TopicManager
  
  /** Instance manager for OpenCode processes */
  instanceManager: InstanceManager
  
  /** Stream handler for SSE → Telegram bridging */
  streamHandler: StreamHandler
  
  /** API server for external instance registration */
  apiServer: ApiServer
  
  /** Start the application */
  start(): Promise<void>
  
  /** Stop the application gracefully */
  stop(): Promise<void>
  
  /** Get instance for a topic */
  getInstance(topicId: number): InstanceInfo | null
  
  /** Get OpenCode client for an instance */
  getClient(instanceId: string): OpenCodeClient | undefined
}

// =============================================================================
// Integration
// =============================================================================

/**
 * Create the fully integrated application
 */
export async function createIntegratedApp(config: AppConfig): Promise<IntegratedApp> {
  console.log("[Integration] Initializing components...")

  // Create the grammY bot
  const bot = new Bot(config.telegram.botToken)

  // Create instance manager (orchestrator)
  const instanceManager = new InstanceManager(toManagerConfig(config))

  // Map of instanceId → OpenCodeClient
  const clients = new Map<string, OpenCodeClient>()

  // Map of instanceId → SSE abort function
  const sseSubscriptions = new Map<string, () => void>()

  // Map of sessionId → instanceId for reverse lookup
  const sessionToInstance = new Map<string, string>()

  // H3: last accepted prompt per topic (set on prompt_async 2xx, cleared on
  // session idle). hadActive is set on the first SSE event for the session so
  // the restart notice survives crash-path state purges (F1: streamHandler
  // states are wiped by unregisterSession before instance:ready runs).
  const lastPromptByTopic = new Map<number, { text: string; sessionId: string; at: number; hadActive: boolean }>()
  // H3/H5: last known session per topic, to detect superseded sessionIds.
  const lastSessionByTopic = new Map<number, string>()

  // Telegram-context system prompt: sessionIds of freshly bot-created sessions
  // still needing the one-time `system` injection on their first prompt_async.
  // Populated ONLY in the instance:ready creation branch below — never for
  // found-existing, discovered, TUI, or external sessions.
  const systemPromptPending = new Set<string>()

  // Task 04: per-topic burst-coalescing buffer. Design choice: DELAY-FIRST
  // sliding window. The first message starts a ~10s timer instead of sending
  // immediately; arrivals inside the window join newline-separated in arrival
  // order and each arrival extends the window (slides expiry to now+10s);
  // expiry sends exactly ONE prompt_async with the joined text. This is the
  // only choice satisfying "N rapid messages -> ONE prompt_async" (send-first
  // could never un-send message 1). Cost, accepted by the task: every message
  // waits out the window (idle singles included — "no added latency beyond
  // the window design"). Busy-session composition: arrivals while the session
  // is in-flight append to the same buffer and flush at the next
  // idle/abort-aware boundary (or the busy-retry timer below) — never lost,
  // never reordered, never merged across topics (keyed by effectiveTopicId).
  // Losslessness backstop: flush is ALWAYS timer-driven, so a missed idle
  // event (error/crash/unregistered session) can delay but never strand text.
  const COALESCE_WINDOW_MS = 10_000
  const COALESCE_BUSY_RETRY_MS = 3_000
  interface CoalescedBurst {
    parts: string[]
    chatId: number
    timer?: ReturnType<typeof setTimeout>
  }
  const coalesceByTopic = new Map<number, CoalescedBurst>()

  // Shared Telegram send queue (task 03): bot-wide pacing (~25 msg/s, under
  // Telegram's ~30/s group budget) + the per-message edit floor, shared 429
  // backoff (honor retry-after +500ms), priority finals/cards/notices over
  // progress edits. Single construction site — ALL sends/edits go through it.
  const sendQueue = new TelegramSendQueue({
    sender: async (item) => {
      const reply_markup = item.inlineKeyboard
        ? { inline_keyboard: item.inlineKeyboard }
        : undefined
      if (item.editMessageId !== undefined) {
        await bot.api.editMessageText(item.chatId, item.editMessageId, item.text, {
          parse_mode: item.parseMode,
          reply_markup,
        })
        return { messageId: item.editMessageId }
      }
      const result = await bot.api.sendMessage(item.chatId, item.text, {
        message_thread_id: item.topicId || undefined,
        parse_mode: item.parseMode,
        reply_to_message_id: item.replyToMessageId,
        reply_markup,
      })
      return { messageId: result.message_id }
    },
    sendIntervalMs: config.opencode.sendIntervalMs,
    editFloorMs: config.opencode.streamUpdateIntervalMs,
  })

  // Create Telegram send callback for stream handler: routes ALL sends/edits
  // through the shared queue. Priority defaults to "final" (never dropped —
  // finals, cards, notices queue + retry); only the progress-render path
  // passes queuePriority "progress" (skippable when saturated).
  const sendCallback: TelegramSendCallback = async (chatId, topicId, text, options) => {
    try {
      return await sendQueue.submit({
        chatId,
        topicId,
        text,
        editMessageId: options?.editMessageId,
        parseMode: options?.parseMode,
        replyToMessageId: options?.replyToMessageId,
        inlineKeyboard: options?.inlineKeyboard,
        priority: options?.queuePriority ?? "final",
      })
    } catch (error) {
      const lastError = error instanceof Error ? error : new Error(String(error))
      
      // "message is not modified" is not a real error - return success
      // (backstop; the queue already resolves this as success)
      if (lastError.message.includes('message is not modified')) {
        return { messageId: options?.editMessageId ?? 0 }
      }
      
      // 429 after the queue's shared backoff + retries: log and surface so
      // the StreamHandler final-retry/fallback paths compose on top (bounded).
      if (lastError.message.includes('429') || lastError.message.includes('Too Many Requests')) {
        console.log(`[Integration] Send queue exhausted retries (429), surfacing to caller`)
      }
      
      throw lastError
    }
  }

  // Create Telegram delete callback
  const deleteCallback: TelegramDeleteCallback = async (chatId, messageId) => {
    try {
      await bot.api.deleteMessage(chatId, messageId)
    } catch (error) {
      // Ignore delete errors (message may already be deleted)
      console.warn("[Integration] Failed to delete message:", error)
    }
  }

  // Create stream handler
  // Note: updateIntervalMs defaults to 1000ms (~1 edit/sec per message, within
  // Telegram's per-message edit budget); override via STREAM_UPDATE_INTERVAL_MS (500-5000)
  // Telegram allows ~30 messages/second to a group, but edits to same message are more restricted
  const streamHandler = new StreamHandler(sendCallback, deleteCallback, {
    updateIntervalMs: config.opencode.streamUpdateIntervalMs,
    showToolNames: true,
    deleteProgressOnComplete: true,
  })

  // Task 05: permission allowlist wiring — matching kinds auto-answer "once"
  // via this resolve chain (identical to the manual perm: branch below, so
  // TUI/discovered/restarted sessions behave the same). Default-deny: the
  // StreamHandler only calls this on explicit rule match; unknown kinds ask.
  // Returns true when OpenCode accepted "once"; false when no client resolved
  // or the call failed (StreamHandler then notifies loudly — never silent).
  // Never sends chat messages here (StreamHandler owns the subtle/loud
  // notices via the shared queue); logs carry ids only, never secrets.
  streamHandler.setPermissionsAutoAllow(config.opencode.permissionsAutoAllow)
  streamHandler.setPermissionAutoResponder(async (sessionId, permissionId, destination) => {
    let client: OpenCodeClient | undefined
    const instanceId = sessionToInstance.get(sessionId)
    if (instanceId) {
      client = clients.get(instanceId)
    }
    if (!client) {
      const dest = streamHandler.getTelegramDestination(sessionId) ?? destination
      if (dest) {
        client = clients.get(`discovered_${dest.topicId}`)
      }
    }
    if (!client) {
      const topicInstance = instanceManager.getInstanceByTopic(destination.topicId)
      if (topicInstance) {
        client = clients.get(topicInstance.config.instanceId)
      }
      if (!client) {
        client = clients.get(`discovered_${destination.topicId}`)
      }
    }
    if (!client) {
      console.error(`[Integration] Auto-allow: no client for session ${sessionId} (topic ${destination.topicId}, perm ${permissionId})`)
      return false
    }
    try {
      await client.respondToPermission(sessionId, permissionId, "once")
      console.log(`[Integration] Auto-allowed permission ${permissionId} (session ${sessionId}) with "once"`)
      return true
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error)
      console.error(`[Integration] Auto-allow failed for perm ${permissionId} (session ${sessionId}): ${reason.slice(0, 120)}`)
      return false
    }
  })

  // Create topic store for direct access
  const topicStore = new TopicStore(config.storage.topicDbPath)

  // Track which sessions have already had their topic names updated
  const topicNamesUpdated = new Set<string>()

  // Set up session idle callback to update topic names after first message
  streamHandler.setOnSessionIdle(async (sessionId, chatId, topicId) => {
    // P1 (I-2): turn boundary — stop the typing heartbeat promptly (the
    // interval also self-terminates on its next tick via the busy check).
    stopTypingHeartbeat(topicId)
    // H3: response completed — the tracked prompt is no longer orphanable.
    const tracked = lastPromptByTopic.get(topicId)
    if (tracked && tracked.sessionId === sessionId) {
      lastPromptByTopic.delete(topicId)
    }
    // Task 04: turn boundary — flush text buffered while the session was busy
    // as ONE joined prompt (the next turn). Fire-and-forget: flush is
    // busy-gated + take-and-delete, so a stray trigger cannot double-send.
    // Awaited callers below (topic-name update) are unaffected on no-op.
    void flushCoalescedTopic(topicId)
    // Only update once per session
    if (topicNamesUpdated.has(sessionId)) {
      return
    }

    try {
      // Get the topic mapping
      const mapping = topicStore.getMapping(chatId, topicId)
      if (!mapping) {
        return
      }

      // Find the client for this session
      let client: OpenCodeClient | undefined
      
      // Check managed instances
      const instanceId = sessionToInstance.get(sessionId)
      if (instanceId) {
        client = clients.get(instanceId)
      }
      
      // Check discovered sessions
      if (!client) {
        client = clients.get(`discovered_${topicId}`)
      }

      if (!client) {
        return
      }

      // Get the current session info to check for title
      const session = await client.getSession(sessionId)
      if (!session?.title) {
        return // No title yet
      }

      // Extract project name from work directory
      const projectName = mapping.workDir?.split('/').pop() || mapping.topicName.split('-')[0]
      
      // Build expected topic name: <project>-<session title>
      const expectedTopicName = `${projectName}-${session.title}`
      
      // Check if topic name already includes the session title
      if (mapping.topicName === expectedTopicName || mapping.topicName.includes(session.title)) {
        topicNamesUpdated.add(sessionId)
        return // Already has the right name
      }

      // Update the topic name in Telegram
      console.log(`[Integration] Updating topic name: "${mapping.topicName}" -> "${expectedTopicName}"`)
      
      await bot.api.editForumTopic(chatId, topicId, { name: expectedTopicName })
      
      // Update the mapping in the store
      topicStore.updateName(chatId, topicId, expectedTopicName)
      
      // Mark as updated
      topicNamesUpdated.add(sessionId)
      
      console.log(`[Integration] Topic name updated to "${expectedTopicName}"`)
    } catch (error) {
      console.error(`[Integration] Failed to update topic name:`, error)
    }
  })

  // API server will be created after bot setup (needs bot reference)
  let apiServer: ApiServer

  // Helper to find instance by session ID
  function findInstanceBySession(sessionId: string): InstanceInfo | null {
    const instanceId = sessionToInstance.get(sessionId)
    if (instanceId) {
      return instanceManager.getInstance(instanceId)
    }
    // Fallback: search all instances
    for (const instance of instanceManager.getAllInstances()) {
      if (instance.sessionId === sessionId) {
        return instance
      }
    }
    return null
  }

  // H1: targeted Telegram notice on unrecoverable SSE loss — never log-only.
  // Resolves affected topic(s) from the instance + session registrations.
  async function notifySseLoss(instanceKey: string, error: unknown): Promise<void> {
    const errMsg = error instanceof Error ? error.message : String(error)
    console.error(`[Integration] SSE unrecoverable for ${instanceKey}:`, errMsg)
    const topicIds = new Set<number>()
    if (instanceKey.startsWith("discovered_")) {
      const tid = parseInt(instanceKey.slice("discovered_".length), 10)
      if (Number.isFinite(tid)) topicIds.add(tid)
    }
    const inst = instanceManager.getInstance(instanceKey)
    if (inst) topicIds.add(inst.config.topicId)
    for (const [sid, iid] of sessionToInstance) {
      if (iid === instanceKey) {
        const dest = streamHandler.getTelegramDestination(sid)
        if (dest) topicIds.add(dest.topicId)
      }
    }
    if (topicIds.size === 0) return
    const chatId = config.telegram.chatId
    for (const topicId of topicIds) {
      // P1/S14: standard loss card (NEW, final lane) with reconnect affordance.
      stopTypingHeartbeat(topicId)
      try {
        await sendCallback(
          chatId,
          topicId,
          `🔌 <b>Live updates interrupted</b>\n<i>Check /status in General — a restart may be needed to restore live updates.</i>`,
          {
            parseMode: "HTML",
            inlineKeyboard: [
              [{ text: "🔄 Reconnect", callback_data: `restart:${topicId}` }],
            ],
          }
        )
      } catch (notifyError) {
        console.error(`[Integration] Failed to send SSE-loss notice to topic ${topicId}:`, notifyError)
      }
    }
  }

  // F1: extract the sessionId from an SSE event (same locations as
  // StreamHandler.handleEvent) so the H3 liveness flag survives state purges.
  function extractSseSessionId(sseEvent: SSEEvent): string | null {
    const props = sseEvent.properties as Record<string, any>
    return (
      props.sessionID ||
      props.info?.sessionID ||
      props.part?.sessionID ||
      props.permission?.sessionID ||
      null
    )
  }

  // F1: persistently record that a tracked prompt produced stream activity.
  // Lives on lastPromptByTopic (not streamHandler states) so the crash-path
  // purge (unregisterSession) cannot wipe it before instance:ready runs.
  function markPromptHadActivity(sessionId: string): void {
    for (const entry of lastPromptByTopic.values()) {
      if (entry.sessionId === sessionId) entry.hadActive = true
    }
  }

  // H5: drop superseded sessionIds for an instance, keeping sessionToInstance
  // and the stream handler in sync. Returns retired sessionIds.
  function retireSupersededSessions(instanceId: string, keepSessionId: string): string[] {    const retired: string[] = []
    for (const [sid, iid] of Array.from(sessionToInstance.entries())) {
      if (iid === instanceId && sid !== keepSessionId) {
        sessionToInstance.delete(sid)
        streamHandler.unregisterSession(sid)
        retired.push(sid)
      }
    }
    return retired
  }

  // Create OpenCode client adapter for TopicManager
  const openCodeAdapter: IOpenCodeClient = {
    async createSession(sessionConfig) {
      // This is called when a new topic is created
      // We don't create sessions here - we create instances
      // Return a placeholder that will be replaced when instance is ready
      const id = `pending_${Date.now()}`
      return { id }
    },

    async sendMessage(sessionId, message) {
      // Find the instance for this session
      const instance = findInstanceBySession(sessionId)
      if (!instance) {
        console.error(`[Integration] No instance found for session ${sessionId}`)
        return
      }

      const client = clients.get(instance.config.instanceId)
      if (!client) {
        console.error(`[Integration] No client for instance ${instance.config.instanceId}`)
        return
      }

      // Send message asynchronously (SSE will handle response)
      await client.sendMessageAsync(sessionId, message)
    },

    async getSession(sessionId) {
      const instance = findInstanceBySession(sessionId)
      if (!instance) return null
      return { id: sessionId, status: instance.state }
    },

    async closeSession(sessionId) {
      const instance = findInstanceBySession(sessionId)
      if (instance) {
        await instanceManager.stopInstance(instance.config.instanceId)
      }
    },
  }

  // Response handler that sends to Telegram topics
  const responseHandler: ResponseHandler = async (chatId, topicId, response) => {
    await sendToTopic(bot, chatId, topicId, response)
  }

  // Create topic manager (pass the shared topicStore)
  const topicManager = new TopicManager(
    openCodeAdapter,
    responseHandler,
    toTopicManagerConfig(config),
    topicStore  // Share the same store instance
  )

  // Handle orchestrator events
  instanceManager.on(async (event: OrchestratorEvent) => {
    console.log(`[Integration] Orchestrator event: ${event.type}`)

    switch (event.type) {
      case "instance:ready": {
        // Clean up any existing client/subscription for this instance first
        const existingAbort = sseSubscriptions.get(event.instanceId)
        if (existingAbort) {
          existingAbort()
          sseSubscriptions.delete(event.instanceId)
        }
        const existingClient = clients.get(event.instanceId)
        if (existingClient) {
          existingClient.close()
          clients.delete(event.instanceId)
        }
        
        // Instance is ready - create client and subscribe to SSE
        const client = new OpenCodeClient({
          baseUrl: `http://localhost:${event.port}`,
        })
        clients.set(event.instanceId, client)

        // Get the instance to find its working directory
        const instanceInfo = instanceManager.getInstance(event.instanceId)
        const instanceWorkDir = instanceInfo?.config.workDir

        // Get or create session - MUST match the instance's working directory
        const sessions = await client.listSessions()
        
        // Find a session that matches this instance's working directory
        // Sessions have a 'directory' field that indicates where they were created
        let sessionId: string | undefined
        
        if (instanceWorkDir) {
          // Look for a session in the same directory
          const matchingSession = sessions.find((s: any) => 
            s.directory === instanceWorkDir
          )
          sessionId = matchingSession?.id
          
          if (matchingSession) {
            console.log(`[Integration] Found existing session ${sessionId} for directory ${instanceWorkDir}`)
          } else {
            console.log(`[Integration] No session found for directory ${instanceWorkDir}, creating new one`)
          }
        }

        // If no matching session found, create a new one
        if (!sessionId) {
          const projectTitle = instanceInfo?.config.name
          const session = await client.createSession(projectTitle ? { title: projectTitle } : undefined)
          sessionId = session.id
          // Freshly bot-created: needs the one-time Telegram system prompt
          // injection on its first prompt_async (managed send path below).
          systemPromptPending.add(sessionId)
          console.log(`[Integration] Created new session ${sessionId}`)
        }

        // H3/H5: capture the previous session for this topic before overwriting,
        // so a restart with a NEW sessionId can surface an orphaned prompt.
        const readyTopicId = instanceInfo?.config.topicId
        const prevSessionForTopic =
          readyTopicId !== undefined ? lastSessionByTopic.get(readyTopicId) : undefined

        // F1: snapshot liveness BEFORE retireSupersededSessions (which calls
        // unregisterSession → deletes streamHandler states). Reading states
        // after any unregister is always false, which made the H3 notice
        // unreachable. (Does not cover the crash path — those states were
        // purged by the earlier instance:crashed event — hence the persistent
        // pending.hadActive flag below.)
        const prevHadActiveSnapshot = prevSessionForTopic
          ? streamHandler.getState(prevSessionForTopic) !== undefined ||
            streamHandler.isProcessing(prevSessionForTopic)
          : false

        // Track session → instance mapping
        sessionToInstance.set(sessionId, event.instanceId)
        // H5: drop any other sessionIds still pointing at this instance.
        retireSupersededSessions(event.instanceId, sessionId)
        
        // Update the instance's sessionId in the orchestrator
        // This is important so that createTopicWithInstance can wait for it
        instanceManager.updateSessionId(event.instanceId, sessionId)

        // Update instance with session ID
        const instance = instanceManager.getInstance(event.instanceId)
        if (instance) {
          // Get topic mapping to check streaming preference
          const topicId = instance.config.topicId
          const mapping = topicStore.getMapping(config.telegram.chatId, topicId)
          const streamingEnabled = mapping?.streamingEnabled ?? false

          // Register session with stream handler (include streaming preference)
          streamHandler.registerSession(sessionId, config.telegram.chatId, topicId, streamingEnabled)

          // Update topic mapping with real session ID
          // Note: We recreate the mapping with the new session ID
          if (mapping && mapping.sessionId.startsWith("pending_")) {
            // Delete old mapping and create new one with real session ID
            topicStore.deleteMapping(config.telegram.chatId, topicId)
            topicStore.createMapping(
              config.telegram.chatId,
              topicId,
              mapping.topicName,
              sessionId,
              {
                creatorUserId: mapping.creatorUserId,
                iconColor: mapping.iconColor,
                iconEmojiId: mapping.iconEmojiId,
              }
            )
            // Preserve streaming preference if it was set
            if (streamingEnabled) {
              topicStore.toggleStreaming(config.telegram.chatId, topicId, true)
            }
          }
        }

        // Subscribe to SSE events
        const abort = client.subscribe(
          (sseEvent: SSEEvent) => {
            console.log(`[Integration] SSE event: ${sseEvent.type}`, JSON.stringify(sseEvent.properties).slice(0, 200))
            // F1: record stream liveness on the tracked prompt (survives the
            // crash/ready state purges that wipe streamHandler states).
            const evtSessionId = extractSseSessionId(sseEvent)
            if (evtSessionId) markPromptHadActivity(evtSessionId)
            streamHandler.handleEvent(sseEvent)
            
            // Record activity on any event
            instanceManager.recordActivity(event.instanceId)
          },
          (error) => {
            // H1: client already retried with backoff — this is unrecoverable,
            // so notify the affected topic(s) instead of logging only.
            void notifySseLoss(event.instanceId, error)
          }
        )
        sseSubscriptions.set(event.instanceId, abort)

        console.log(`[Integration] Instance ${event.instanceId} ready with session ${sessionId}`)

        // H3: if the restart produced a NEW session while a recent prompt on
        // the old session never completed, tell the user to resend — never silent.
        // H5: unregister the superseded sessionId and record the new one.
        if (readyTopicId !== undefined) {
          if (prevSessionForTopic && prevSessionForTopic !== sessionId) {
            streamHandler.unregisterSession(prevSessionForTopic)
            const pending = lastPromptByTopic.get(readyTopicId)
            const recent = !!pending && Date.now() - pending.at < 10 * 60 * 1000
            // F1: hadActive combines the pre-unregister snapshot with the
            // persistent per-prompt flag (the flag covers the crash path, where
            // states were already purged long before instance:ready runs).
            // Notice-only: no auto-resend (duplicate-prompt risk).
            const hadActive = prevHadActiveSnapshot || pending?.hadActive === true
            if (recent && hadActive) {
              // P1/S13: same text, one-tap Retry added (NEW, final lane).
              try {
                await sendCallback(
                  config.telegram.chatId,
                  readyTopicId,
                  `🔄 <b>OpenCode restarted.</b> Your last message may not have completed — tap to resend, or ignore if a reply appears.`,
                  {
                    parseMode: "HTML",
                    inlineKeyboard: [
                      [{ text: "🔁 Resend last message", callback_data: `retry:${readyTopicId}` }],
                    ],
                  }
                )
              } catch (notifyError) {
                console.error(`[Integration] Failed to send restart notice to topic ${readyTopicId}:`, notifyError)
              }
            }
          }
          lastSessionByTopic.set(readyTopicId, sessionId)
        }
        break
      }

      case "instance:stopped":
      case "instance:crashed":
      case "instance:failed": {
        // Clean up client and SSE subscription
        const abort = sseSubscriptions.get(event.instanceId)
        if (abort) {
          abort()
          sseSubscriptions.delete(event.instanceId)
        }

        const client = clients.get(event.instanceId)
        if (client) {
          client.close()
          clients.delete(event.instanceId)
        }

        // Clean up session mapping (H5: remove ALL stale entries, keep the
        // stream handler in sync so no superseded session stays registered)
        for (const [staleSessionId, instId] of Array.from(sessionToInstance.entries())) {
          if (instId === event.instanceId) {
            sessionToInstance.delete(staleSessionId)
            streamHandler.unregisterSession(staleSessionId)
          }
        }

        // Notify in Telegram if crashed (P1/S12: standard crash card, NEW,
        // final lane via sendCallback so it is never dropped; Retry on
        // willRestart, one-tap restart affordance otherwise).
        if (event.type === "instance:crashed") {
          const instance = instanceManager.getInstance(event.instanceId)
          if (instance) {
            stopTypingHeartbeat(instance.config.topicId)
            const crashEvent = event as { error: string; willRestart: boolean }
            const errSlice = escapeHtmlCard(String(crashEvent.error ?? "unknown error").slice(0, 120))
            const topicId = instance.config.topicId
            if (crashEvent.willRestart) {
              try {
                await sendCallback(
                  config.telegram.chatId,
                  topicId,
                  `⚠️ <b>OpenCode instance crashed</b>\n<code>${errSlice}</code>\n` +
                  `<i>Restarting… your session will resume. Resend your last message if no reply appears.</i>`,
                  {
                    parseMode: "HTML",
                    inlineKeyboard: [
                      [{ text: "🔁 Resend last message", callback_data: `retry:${topicId}` }],
                    ],
                  }
                )
              } catch (notifyError) {
                console.error(`[Integration] Failed to send crash notice to topic ${topicId}:`, notifyError)
              }
            } else {
              try {
                await sendCallback(
                  config.telegram.chatId,
                  topicId,
                  `⚠️ <b>OpenCode instance crashed</b>\n<code>${errSlice}</code>\n` +
                  `<i>It will not restart automatically. Send a message to start a fresh session.</i>`,
                  {
                    parseMode: "HTML",
                    inlineKeyboard: [
                      [{ text: "🔄 Start fresh session", callback_data: `restart:${topicId}` }],
                    ],
                  }
                )
              } catch (notifyError) {
                console.error(`[Integration] Failed to send crash notice to topic ${topicId}:`, notifyError)
              }
            }
          }
        }
        break
      }

      case "instance:idle-timeout": {
        const instance = instanceManager.getInstance(event.instanceId)
        if (instance) {
          stopTypingHeartbeat(instance.config.topicId)
          // P1/S15: standard copy (button-free NEW — any message restarts).
          await sendToTopic(
            bot,
            config.telegram.chatId,
            instance.config.topicId,
            `💤 <b>Session paused (inactive).</b> Send any message to restart — history is kept.`,
            "HTML"
          )
        }
        break
      }
    }
  })

  // Task 04: true when the topic's session has a turn in-flight (busy).
  // Resolves via mapping first, then the topic's current managed instance
  // (covers mapping-sessionId-stale races). Never throws — unknown means idle
  // so the timer path still delivers.
  function isTopicSessionBusy(chatId: number, topicId: number): boolean {
    try {
      const mapping = topicStore.getMapping(chatId, topicId)
      if (mapping?.sessionId && streamHandler.isProcessing(mapping.sessionId)) {
        return true
      }
      const topicInstance = instanceManager.getInstanceByTopic(topicId)
      if (topicInstance?.sessionId && streamHandler.isProcessing(topicInstance.sessionId)) {
        return true
      }
    } catch {
      // Ignore lookup errors — treat as idle, timer path delivers.
    }
    return false
  }

  // P1 (I-2): typing-indicator heartbeat, one ~4s interval per busy topic.
  // Purely additive: best-effort sendChatAction, never blocks sends, never
  // logged with text. Self-terminating — each tick re-checks
  // isTopicSessionBusy and stops when the turn ends, so a missed stop event
  // can only leave one stray 4s blip. Rate math: 1 action / 4s / busy topic;
  // 10 concurrent busy topics ≈ 2.5 actions/s, far under Telegram's ~30/s
  // group budget and orthogonal to the 1/s per-message edit floor.
  const typingTimers = new Map<number, ReturnType<typeof setInterval>>()
  function startTypingHeartbeat(chatId: number, topicId: number): void {
    if (typingTimers.has(topicId)) return
    void bot.api.sendChatAction(chatId, "typing", { message_thread_id: topicId || undefined }).catch(() => {})
    const timer = setInterval(() => {
      if (!isTopicSessionBusy(chatId, topicId)) {
        stopTypingHeartbeat(topicId)
        return
      }
      void bot.api.sendChatAction(chatId, "typing", { message_thread_id: topicId || undefined }).catch(() => {})
    }, 4000)
    if (typeof (timer as unknown as { unref?: () => void }).unref === "function") {
      (timer as unknown as { unref: () => void }).unref()
    }
    typingTimers.set(topicId, timer)
  }
  function stopTypingHeartbeat(topicId: number): void {
    const timer = typingTimers.get(topicId)
    if (timer) {
      clearInterval(timer)
      typingTimers.delete(topicId)
    }
  }

  /** P1: escape &<>" for Telegram HTML cards (ids/errors only, never secrets). */
  function escapeHtmlCard(text: string): string {
    return text
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
  }

  // Task 04: drop a topic's buffered burst (timer + parts). Called on topic
  // cleanup paths (disconnect/stale-cleanup) where the destination is gone.
  function clearCoalesceBuffer(topicId: number): void {
    const burst = coalesceByTopic.get(topicId)
    if (!burst) return
    if (burst.timer) clearTimeout(burst.timer)
    coalesceByTopic.delete(topicId)
  }

  // Task 04: flush one topic's buffer as ONE joined prompt_async via the
  // existing router below (which owns echo guard, system-prompt injection,
  // lastPromptByTopic, and 429-safe send — all preserved untouched).
  // Busy gate: while the session is in-flight, keep buffering and re-arm a
  // short retry so a missed idle event can never strand text. Take-and-delete
  // is synchronous, so concurrent timer/idle/abort triggers cannot double-send.
  async function flushCoalescedTopic(topicId: number): Promise<void> {
    const burst = coalesceByTopic.get(topicId)
    if (!burst || burst.parts.length === 0) return
    if (isTopicSessionBusy(burst.chatId, topicId)) {
      if (burst.timer) clearTimeout(burst.timer)
      burst.timer = setTimeout(() => void flushCoalescedTopic(topicId), COALESCE_BUSY_RETRY_MS)
      return
    }
    const joined = burst.parts.join("\n")
    const chatId = burst.chatId
    if (burst.timer) clearTimeout(burst.timer)
    coalesceByTopic.delete(topicId)
    try {
      // Joined text flows through the unchanged router, so its existing
      // markMessageFromTelegram(sessionId, text) sites cover the JOINED text.
      await routeMessageToInstance({
        messageId: 0,
        chatId,
        topicId,
        userId: 0,
        text: joined,
        isGeneralTopic: topicId === 0,
        isReply: false,
      })
    } catch (error) {
      // The router posts its own error notice to the topic on failure (same
      // as today's single-send semantics: user resends). Log lengths only —
      // never message text (secrets).
      const reason = error instanceof Error ? error.message : String(error)
      console.error(`[Integration] Coalesced flush failed for topic ${topicId} (len=${joined.length}): ${reason.slice(0, 120)}`)
    }
  }

  // Task 04: coalescing entry point — per-topic sliding window + busy hold.
  // Same signature as the router; returns success-pending (response arrives
  // via SSE as today). Callers only use success/error/isNewSession.
  async function routeMessageToInstanceCoalesced(
    context: ForumMessageContext
  ): Promise<MessageRouteResult> {
    const { chatId, topicId, text } = context
    const effectiveTopicId = context.isGeneralTopic ? 0 : topicId
    const busy = isTopicSessionBusy(chatId, effectiveTopicId)
    let burst = coalesceByTopic.get(effectiveTopicId)
    if (!burst) {
      burst = { parts: [], chatId }
      coalesceByTopic.set(effectiveTopicId, burst)
    }
    // Chronological append — order is arrival order, never reordered, and
    // buffers are keyed per topic so text can never leak across topics.
    burst.parts.push(text)
    burst.chatId = chatId
    if (burst.timer) clearTimeout(burst.timer)
    // Idle: sliding ~10s window (each arrival extends). Busy: hold for the
    // next idle/abort boundary, with a short retry backstop (see flush).
    burst.timer = setTimeout(
      () => void flushCoalescedTopic(effectiveTopicId),
      busy ? COALESCE_BUSY_RETRY_MS : COALESCE_WINDOW_MS
    )
    // Permission/cancel interplay: buffering never touches permission cards
    // (posted independently by the stream handler) and never blocks /cancel
    // (handleCancelRequest aborts the turn, then flushes promptly — see below).
    const mapping = topicStore.getMapping(chatId, effectiveTopicId)
    return { success: true, sessionId: mapping?.sessionId }
  }

  // Custom message router that uses our instances
  async function routeMessageToInstance(
    context: ForumMessageContext
  ): Promise<MessageRouteResult> {
    const { chatId, topicId, text } = context
    const effectiveTopicId = context.isGeneralTopic ? 0 : topicId

    // Check if this topic is linked to an external OpenCode instance
    if (apiServer.isExternalTopic(effectiveTopicId)) {
      const external = apiServer.getExternalByTopic(effectiveTopicId)
      if (external?.sessionId) {
        // Mark this message as coming from Telegram so we don't echo it back
        streamHandler.markMessageFromTelegram(external.sessionId, text)
      }
      const success = await apiServer.routeMessageToExternal(effectiveTopicId, text)
      if (success) {
        startTypingHeartbeat(chatId, effectiveTopicId)
        return { success: true, sessionId: external?.sessionId }
      } else {
        await sendToTopic(bot, chatId, effectiveTopicId, "Failed to send message to external OpenCode instance.")
        return { success: false, error: "External instance not reachable" }
      }
    }

    // Check if this topic is linked to a discovered session
    const discoveredKey = `discovered_${effectiveTopicId}`
    const discoveredClient = clients.get(discoveredKey)
    if (discoveredClient) {
      // This topic is connected to a discovered session - use that client
      const mapping = topicStore.getMapping(chatId, effectiveTopicId)
      if (mapping?.sessionId) {
        try {
          // Mark this message as coming from Telegram so we don't echo it back
          streamHandler.markMessageFromTelegram(mapping.sessionId, text)
          await discoveredClient.sendMessageAsync(mapping.sessionId, text)
          console.log(`[Integration] Sent message to discovered session ${mapping.sessionId}`)
          // H3: track last accepted prompt for orphan detection on restart.
          lastPromptByTopic.set(effectiveTopicId, { text, sessionId: mapping.sessionId, at: Date.now(), hadActive: false })
          startTypingHeartbeat(chatId, effectiveTopicId)
          return { success: true, sessionId: mapping.sessionId }
        } catch (error) {
          const errorMsg = error instanceof Error ? error.message : String(error)
          console.error(`[Integration] Failed to send to discovered session:`, errorMsg)
          
          // Check if the session is still alive
          const isHealthy = await discoveredClient.isHealthy()
          if (!isHealthy) {
            // Clean up the dead discovered session
            const abort = sseSubscriptions.get(discoveredKey)
            if (abort) {
              abort()
              sseSubscriptions.delete(discoveredKey)
            }
            discoveredClient.close()
            clients.delete(discoveredKey)
            streamHandler.unregisterSession(mapping.sessionId)
            
            // Try to auto-reconnect: discover TUI sessions in the same directory
            console.log(`[Integration] Attempting to reconnect to TUI session in ${mapping.workDir}`)
            const discovered = await discoverSessions()
            // Only reconnect to TUI instances, not managed 'opencode serve' instances
            const reconnectSession = discovered.find(s => 
              s.instance.isTui && (s.directory === mapping.workDir || s.id === mapping.sessionId)
            )
            
            if (reconnectSession) {
              console.log(`[Integration] Found session to reconnect: ${reconnectSession.id} on port ${reconnectSession.instance.port}`)
              
              // Create new client for the reconnected session
              const newClient = new OpenCodeClient({
                baseUrl: `http://localhost:${reconnectSession.instance.port}`,
              })
              
              // Subscribe to SSE events
              const newAbort = newClient.subscribe(
                (sseEvent: SSEEvent) => {
                  console.log(`[Integration] SSE event from reconnected session:`, sseEvent.type)
                  const evtSessionId = extractSseSessionId(sseEvent)
                  if (evtSessionId) markPromptHadActivity(evtSessionId)
                  streamHandler.handleEvent(sseEvent)
                },
                (error) => {
                  // H1: unrecoverable after client backoff — notify the topic.
                  void notifySseLoss(discoveredKey, error)
                }
              )
              
              // Store the new subscription
              sseSubscriptions.set(discoveredKey, newAbort)
              clients.set(discoveredKey, newClient)
              
              // Update the mapping if session ID changed
              if (reconnectSession.id !== mapping.sessionId) {
                // H5: unregister the superseded sessionId; keep the maps in sync.
                streamHandler.unregisterSession(mapping.sessionId)
                sessionToInstance.delete(mapping.sessionId)
                topicStore.deleteMapping(chatId, effectiveTopicId)
                topicStore.createMapping(chatId, effectiveTopicId, mapping.topicName, reconnectSession.id, {
                  creatorUserId: mapping.creatorUserId,
                  iconColor: mapping.iconColor,
                  iconEmojiId: mapping.iconEmojiId,
                })
                topicStore.updateWorkDir(chatId, effectiveTopicId, mapping.workDir!)
                topicStore.toggleStreaming(chatId, effectiveTopicId, mapping.streamingEnabled ?? false)
              }
              
              // Re-register with stream handler
              streamHandler.registerSession(reconnectSession.id, chatId, effectiveTopicId, mapping.streamingEnabled ?? false)
              
              // Now send the message
              try {
                streamHandler.markMessageFromTelegram(reconnectSession.id, text)
                await newClient.sendMessageAsync(reconnectSession.id, text)
                console.log(`[Integration] Reconnected and sent message to session ${reconnectSession.id}`)
                // H3: track last accepted prompt for orphan detection on restart.
                lastPromptByTopic.set(effectiveTopicId, { text, sessionId: reconnectSession.id, at: Date.now(), hadActive: false })
                startTypingHeartbeat(chatId, effectiveTopicId)
                
                // Notify user of successful reconnection (P1/S14 standard copy)
                await sendToTopic(bot, chatId, effectiveTopicId,
                  `✅ <b>Reconnected</b> <i>— live updates resumed.</i>`,
                  "HTML"
                )
                
                return { success: true, sessionId: reconnectSession.id }
              } catch (reconnectError) {
                console.error(`[Integration] Failed to send after reconnect:`, reconnectError)
                // Fall through to show error message
              }
            }
            
            await sendToTopic(bot, chatId, effectiveTopicId, 
              "⚠️ The discovered session is no longer available.\n\n" +
              "The OpenCode instance may have been closed. " +
              "Send another message to start a new managed instance, or use `/connect` to link to a different session."
            )
            return { success: false, error: "Discovered session no longer available" }
          }
          
          await sendToTopic(bot, chatId, effectiveTopicId, `Error: ${errorMsg}`)
          return { success: false, error: errorMsg }
        }
      }
    }

    // Get topic mapping from store
    const mapping = topicStore.getMapping(chatId, effectiveTopicId)
    const topicName = mapping?.topicName || (effectiveTopicId === 0 ? "General" : `topic-${effectiveTopicId}`)
    
    // Use custom workDir if linked, otherwise use default path
    // Special case: General topic (topicId=0) uses /tmp for direct OpenCode conversations
    const workDir = mapping?.workDir || (effectiveTopicId === 0 ? "/tmp" : `${config.project.basePath}/${topicName}`)

    // Before creating a managed instance, check if there's an existing TUI we can connect to
    // This handles the case where:
    // 1. User connected to a discovered session via /connect
    // 2. The TUI was closed
    // 3. User reopened the TUI
    // 4. We should reconnect to the TUI instead of creating a new managed instance
    if (mapping?.workDir) {
      console.log(`[Integration] Checking for existing TUI in ${workDir}`)
      const discovered = await discoverSessions()
      // Only connect to TUI instances, not managed 'opencode serve' instances
      const existingSession = discovered.find(s => s.directory === workDir && s.instance.isTui)
      
      if (existingSession) {
        console.log(`[Integration] Found existing TUI session: ${existingSession.id} on port ${existingSession.instance.port}`)
        
        // Connect to the existing TUI instead of creating a managed instance
        const newClient = new OpenCodeClient({
          baseUrl: `http://localhost:${existingSession.instance.port}`,
        })
        
        // Subscribe to SSE events
        const discoveredKey = `discovered_${effectiveTopicId}`
        const newAbort = newClient.subscribe(
          (sseEvent: SSEEvent) => {
            console.log(`[Integration] SSE event from reconnected TUI:`, sseEvent.type)
            const evtSessionId = extractSseSessionId(sseEvent)
            if (evtSessionId) markPromptHadActivity(evtSessionId)
            streamHandler.handleEvent(sseEvent)
          },
          (error) => {
            // H1: unrecoverable after client backoff — notify the topic.
            void notifySseLoss(discoveredKey, error)
          }
        )
        
        // Store the subscription
        sseSubscriptions.set(discoveredKey, newAbort)
        clients.set(discoveredKey, newClient)
        
        // Update the mapping if session ID changed
        if (existingSession.id !== mapping.sessionId) {
          // H5: unregister the superseded sessionId; keep the maps in sync.
          streamHandler.unregisterSession(mapping.sessionId)
          sessionToInstance.delete(mapping.sessionId)
          topicStore.deleteMapping(chatId, effectiveTopicId)
          topicStore.createMapping(chatId, effectiveTopicId, mapping.topicName, existingSession.id, {
            creatorUserId: mapping.creatorUserId,
            iconColor: mapping.iconColor,
            iconEmojiId: mapping.iconEmojiId,
          })
          topicStore.updateWorkDir(chatId, effectiveTopicId, workDir)
          topicStore.toggleStreaming(chatId, effectiveTopicId, mapping.streamingEnabled ?? false)
        }
        
        // Register with stream handler
        streamHandler.registerSession(existingSession.id, chatId, effectiveTopicId, mapping.streamingEnabled ?? false)
        
        // Send the message
        try {
          streamHandler.markMessageFromTelegram(existingSession.id, text)
          await newClient.sendMessageAsync(existingSession.id, text)
          console.log(`[Integration] Connected to existing TUI and sent message to session ${existingSession.id}`)
          // H3: track last accepted prompt for orphan detection on restart.
          lastPromptByTopic.set(effectiveTopicId, { text, sessionId: existingSession.id, at: Date.now(), hadActive: false })
          startTypingHeartbeat(chatId, effectiveTopicId)
          
          await sendToTopic(bot, chatId, effectiveTopicId,
            `✅ <b>Reconnected</b> <i>— live updates resumed.</i>`,
            "HTML"
          )
          
          return { success: true, sessionId: existingSession.id }
        } catch (reconnectError) {
          console.error(`[Integration] Failed to send to existing TUI:`, reconnectError)
          // Fall through to create managed instance
        }
      }
    }

    // Ensure directory exists (only for non-linked directories)
    if (!mapping?.workDir && config.project.autoCreateDirs) {
      try {
        await Bun.$`mkdir -p ${workDir}`.quiet()
      } catch {
        // Ignore errors
      }
    }

    const instance = await instanceManager.getOrCreateInstance(effectiveTopicId, workDir, {
      name: topicName,
    })

    if (!instance) {
      await sendToTopic(bot, chatId, effectiveTopicId, "Failed to start OpenCode instance. Please try again.")
      return { success: false, error: "Failed to create instance" }
    }

    // Wait for instance to be ready
    if (instance.state !== "running") {
      await sendToTopic(bot, chatId, effectiveTopicId, "Starting OpenCode instance...")
      
      // Wait up to 30 seconds for instance to be ready
      const startTime = Date.now()
      while (Date.now() - startTime < 30000) {
        const current = instanceManager.getInstance(instance.config.instanceId)
        if (current?.state === "running" && current.sessionId) {
          break
        }
        if (current?.state === "failed" || current?.state === "crashed") {
          await sendToTopic(bot, chatId, effectiveTopicId, `Failed to start instance: ${current.lastError}`)
          return { success: false, error: current.lastError }
        }
        await new Promise((r) => setTimeout(r, 500))
      }
    }

    // Get the client and send message
    const client = clients.get(instance.config.instanceId)
    const currentInstance = instanceManager.getInstance(instance.config.instanceId)
    
    if (!client || !currentInstance?.sessionId) {
      await sendToTopic(bot, chatId, effectiveTopicId, "Instance not ready. Please try again.")
      return { success: false, error: "Instance not ready" }
    }

    // Record activity
    instanceManager.recordActivity(instance.config.instanceId)

    // Send message asynchronously
    // One-time Telegram-context system prompt: only for freshly bot-created
    // sessions (managed path only — never discovered/TUI/external branches
    // above). Marked injected only after the await resolves (2xx) so a
    // failure retries injection on the next message.
    const injectSystemPrompt = systemPromptPending.has(currentInstance.sessionId)
    const systemOption = injectSystemPrompt
      ? { system: buildTelegramSystemPrompt(topicName) }
      : undefined
    try {
      // Mark this message as coming from Telegram so we don't echo it back
      streamHandler.markMessageFromTelegram(currentInstance.sessionId, text)
      await client.sendMessageAsync(currentInstance.sessionId, text, systemOption)
      if (injectSystemPrompt) {
        systemPromptPending.delete(currentInstance.sessionId)
      }
      // H3: track last accepted prompt for orphan detection on restart.
      lastPromptByTopic.set(effectiveTopicId, { text, sessionId: currentInstance.sessionId, at: Date.now(), hadActive: false })
      startTypingHeartbeat(chatId, effectiveTopicId)
      return { success: true, sessionId: currentInstance.sessionId }
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error)
      await sendToTopic(bot, chatId, effectiveTopicId, `Error: ${errorMsg}`)
      return { success: false, error: errorMsg }
    }
  }

  // Override topic manager's routeMessage to use our custom router
  // We need to monkey-patch this since the original expects ForumMessageContext
  // Task 04: entry is the coalescing wrapper (per-topic ~10s window + busy
  // hold); it flushes through routeMessageToInstance with the JOINED text.
  const originalRouteMessage = topicManager.routeMessage.bind(topicManager)
  topicManager.routeMessage = async (context: ForumMessageContext): Promise<MessageRouteResult> => {
    return routeMessageToInstanceCoalesced(context)
  }

  // Helper to get all active sessions (managed + external + discovered)
  async function getActiveSessions(): Promise<ActiveSessionInfo[]> {
    const sessions: ActiveSessionInfo[] = []
    const knownSessionIds = new Set<string>()
    const knownPorts = new Set<number>()

    // Get managed instances from orchestrator
    const managedInstances = instanceManager.getAllInstances()
    for (const instance of managedInstances) {
      if (instance.state === "running" || instance.state === "starting") {
        if (instance.sessionId) knownSessionIds.add(instance.sessionId)
        knownPorts.add(instance.port)
        
        sessions.push({
          sessionId: instance.sessionId || `pending_${instance.config.instanceId}`,
          name: instance.config.name || `Topic ${instance.config.topicId}`,
          directory: instance.config.workDir,
          topicId: instance.config.topicId,
          isExternal: false,
          isDiscovered: false,
          port: instance.port,
          lastActivity: instance.lastActivityAt,
          status: instance.state === "running" ? "running" : "unknown",
        })
      }
    }

    // Get external instances from API server
    const externalInstances = apiServer.getExternalInstances()
    for (const ext of externalInstances) {
      knownSessionIds.add(ext.sessionId)
      knownPorts.add(ext.opencodePort)
      
      sessions.push({
        sessionId: ext.sessionId,
        name: ext.projectName,
        directory: ext.projectPath,
        topicId: ext.topicId,
        isExternal: true,
        isDiscovered: false,
        port: ext.opencodePort,
        lastActivity: ext.lastActivityAt,
        status: "running", // External instances are assumed running if registered
      })
    }

    // Discover other running OpenCode instances
    try {
      const discovered = await discoverSessions()
      
      for (const disc of discovered) {
        // Skip if we already know about this session or port
        if (knownSessionIds.has(disc.id) || knownPorts.has(disc.instance.port)) {
          continue
        }
        
        // Use directory basename as name, or title if available
        const name = disc.title || disc.directory.split('/').pop() || 'Unknown'
        
        sessions.push({
          sessionId: disc.id,
          name,
          directory: disc.directory,
          topicId: undefined, // Not linked to a topic yet
          isExternal: false,
          isDiscovered: true,
          port: disc.instance.port,
          lastActivity: disc.updatedAt,
          status: "running",
        })
      }
    } catch (error) {
      console.error('[Integration] Error discovering sessions:', error)
    }

    return sessions
  }

  // Helper to connect to an existing session from General topic
  async function connectToSession(chatId: number, sessionIdentifier: string): Promise<ConnectResult> {
    // First, get all sessions
    const sessions = await getActiveSessions()
    
    // Find matching session by name or session ID
    const normalizedId = sessionIdentifier.toLowerCase().trim()
    const matchingSession = sessions.find(s => 
      s.name.toLowerCase() === normalizedId ||
      s.sessionId.toLowerCase().startsWith(normalizedId) ||
      s.directory.toLowerCase().includes(normalizedId) ||
      s.directory.split('/').pop()?.toLowerCase() === normalizedId
    )

    if (!matchingSession) {
      return {
        success: false,
        error: `No session found matching "${sessionIdentifier}".\n\nUse \`/sessions\` to see available sessions.`,
      }
    }

    // If session already has a topic, return that
    if (matchingSession.topicId) {
      const positiveId = String(chatId).replace(/^-100/, "")
      return {
        success: true,
        sessionId: matchingSession.sessionId,
        topicId: matchingSession.topicId,
        topicUrl: `https://t.me/c/${positiveId}/${matchingSession.topicId}`,
      }
    }

    // Create a new topic for this session
    try {
      // Build topic name: <project>-<session title> or just <project> if no title
      const projectName = matchingSession.directory.split('/').pop() || 'project'
      // If name differs from projectName, it's the session title
      const sessionTitle = matchingSession.name !== projectName ? matchingSession.name : null
      const topicName = sessionTitle ? `${projectName}-${sessionTitle}` : projectName
      
      const newTopic = await bot.api.createForumTopic(chatId, topicName)
      const topicId = newTopic.message_thread_id

      // Register the session with the stream handler
      streamHandler.registerSession(matchingSession.sessionId, chatId, topicId, true)

      // Create topic mapping
      topicStore.createMapping(chatId, topicId, topicName, matchingSession.sessionId, {})
      topicStore.updateWorkDir(chatId, topicId, matchingSession.directory)
      topicStore.toggleStreaming(chatId, topicId, true)

      // For discovered sessions, we need to subscribe to SSE events
      if (matchingSession.isDiscovered && matchingSession.port) {
        const client = new OpenCodeClient({
          baseUrl: `http://localhost:${matchingSession.port}`,
        })

        // Subscribe to SSE events
        const abort = client.subscribe(
          (sseEvent: SSEEvent) => {
            console.log(`[Integration] SSE event from discovered session:`, sseEvent.type)
            const evtSessionId = extractSseSessionId(sseEvent)
            if (evtSessionId) markPromptHadActivity(evtSessionId)
            streamHandler.handleEvent(sseEvent)
          },
          (error) => {
            // H1: unrecoverable after client backoff — notify the topic.
            void notifySseLoss(`discovered_${topicId}`, error)
          }
        )

        // Store the subscription for cleanup (using topic ID as key)
        sseSubscriptions.set(`discovered_${topicId}`, abort)
        clients.set(`discovered_${topicId}`, client)
      }

      // Send welcome message
      const sessionType = matchingSession.isDiscovered ? "discovered" : "existing"
      await bot.api.sendMessage(
        chatId,
        `✅ *Connected to ${sessionType} session*\n\n` +
        `*Session:* \`${matchingSession.sessionId.slice(0, 12)}...\`\n` +
        `*Directory:* \`${matchingSession.directory}\`\n` +
        (matchingSession.port ? `*Port:* ${matchingSession.port}\n` : "") +
        `\n_Messages sent here will be forwarded to the OpenCode session._`,
        {
          message_thread_id: topicId,
          parse_mode: "Markdown",
        }
      )

      const positiveId = String(chatId).replace(/^-100/, "")
      return {
        success: true,
        sessionId: matchingSession.sessionId,
        topicId,
        topicUrl: `https://t.me/c/${positiveId}/${topicId}`,
      }
    } catch (error) {
      return {
        success: false,
        error: `Failed to create topic: ${error instanceof Error ? error.message : String(error)}`,
      }
    }
  }

  // Helper to find stale sessions (topics linked to dead sessions)
  async function findStaleSessions(chatId: number): Promise<Array<{
    topicId: number
    topicName: string
    sessionId: string
    directory?: string
    reason: "port_dead" | "session_missing" | "instance_stopped"
  }>> {
    const staleSessions: Array<{
      topicId: number
      topicName: string
      sessionId: string
      directory?: string
      reason: "port_dead" | "session_missing" | "instance_stopped"
    }> = []

    // Get all topic mappings from the store
    const allMappings = topicStore.queryMappings({ chatId })

    for (const mapping of allMappings) {
      // Skip if no session ID or if it's a pending session
      if (!mapping.sessionId || mapping.sessionId.startsWith('pending_')) {
        continue
      }

      // Check if this is a managed instance
      const managedInstance = instanceManager.getInstanceByTopic(mapping.topicId)
      if (managedInstance) {
        // Check if the managed instance is stopped/crashed
        if (managedInstance.state === "stopped" || managedInstance.state === "crashed" || managedInstance.state === "failed") {
          staleSessions.push({
            topicId: mapping.topicId,
            topicName: mapping.topicName,
            sessionId: mapping.sessionId,
            directory: mapping.workDir,
            reason: "instance_stopped",
          })
        }
        continue
      }

      // Check if this is an external instance
      const externalInstance = apiServer.getExternalByTopic(mapping.topicId)
      if (externalInstance) {
        // Check if the external instance is still alive
        const alive = await isPortAlive(externalInstance.opencodePort)
        if (!alive) {
          staleSessions.push({
            topicId: mapping.topicId,
            topicName: mapping.topicName,
            sessionId: mapping.sessionId,
            directory: externalInstance.projectPath,
            reason: "port_dead",
          })
        }
        continue
      }

      // This mapping is not linked to any known instance - it's orphaned
      // Try to find if there's a port stored somewhere we can check
      // For now, mark as session_missing
      staleSessions.push({
        topicId: mapping.topicId,
        topicName: mapping.topicName,
        sessionId: mapping.sessionId,
        directory: mapping.workDir,
        reason: "session_missing",
      })
    }

    return staleSessions
  }

  // Helper to clean up a stale session
  async function cleanupStaleSession(chatId: number, topicId: number): Promise<boolean> {
    try {
      // Clean up SSE subscription if exists
      const discoveredKey = `discovered_${topicId}`
      const abort = sseSubscriptions.get(discoveredKey)
      if (abort) {
        abort()
        sseSubscriptions.delete(discoveredKey)
      }

      const client = clients.get(discoveredKey)
      if (client) {
        client.close()
        clients.delete(discoveredKey)
      }

      // Remove from stream handler
      const mapping = topicStore.getMapping(chatId, topicId)
      if (mapping?.sessionId) {
        streamHandler.unregisterSession(mapping.sessionId)
      }

      // Task 04: destination is gone — drop any buffered burst for the topic.
      clearCoalesceBuffer(topicId)

      // Delete the topic mapping
      topicStore.deleteMapping(chatId, topicId)

      console.log(`[Integration] Cleaned up stale session for topic ${topicId}`)
      return true
    } catch (error) {
      console.error(`[Integration] Error cleaning up stale session:`, error)
      return false
    }
  }

  // Helper to disconnect a session and delete its topic
  async function disconnectSession(chatId: number, topicId: number): Promise<{
    success: boolean
    topicDeleted?: boolean
    error?: string
  }> {
    try {
      // Get the mapping first
      const mapping = topicStore.getMapping(chatId, topicId)
      if (!mapping) {
        return {
          success: false,
          error: "No session mapping found for this topic.",
        }
      }

      // Clean up SSE subscription if exists
      const discoveredKey = `discovered_${topicId}`
      const abort = sseSubscriptions.get(discoveredKey)
      if (abort) {
        abort()
        sseSubscriptions.delete(discoveredKey)
      }

      const client = clients.get(discoveredKey)
      if (client) {
        client.close()
        clients.delete(discoveredKey)
      }

      // Unregister from stream handler
      if (mapping.sessionId) {
        streamHandler.unregisterSession(mapping.sessionId)
      }

      // Task 04: topic is going away — drop any buffered burst for it.
      clearCoalesceBuffer(topicId)

      // Delete the topic mapping
      topicStore.deleteMapping(chatId, topicId)

      // Stop managed instance if exists
      const managedInstance = instanceManager.getInstanceByTopic(topicId)
      if (managedInstance) {
        await instanceManager.stopInstance(managedInstance.config.instanceId)
      }

      // Try to delete the Telegram topic
      let topicDeleted = false
      try {
        await bot.api.deleteForumTopic(chatId, topicId)
        topicDeleted = true
        console.log(`[Integration] Deleted topic ${topicId}`)
      } catch (error) {
        // Topic deletion might fail if it's already deleted or we don't have permission
        console.warn(`[Integration] Could not delete topic ${topicId}:`, error)
      }

      console.log(`[Integration] Disconnected session for topic ${topicId}`)
      return {
        success: true,
        topicDeleted,
      }
    } catch (error) {
      console.error(`[Integration] Error disconnecting session:`, error)
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  // Helper to create a new topic with directory and OpenCode instance
  async function createTopicWithInstance(chatId: number, topicName: string): Promise<CreateTopicResult> {
    try {
      // Create directory in PROJECT_BASE_PATH
      const workDir = `${config.project.basePath}/${topicName}`
      
      // Create the directory
      try {
        await Bun.$`mkdir -p ${workDir}`.quiet()
        console.log(`[Integration] Created directory: ${workDir}`)
      } catch (error) {
        return {
          success: false,
          error: `Failed to create directory: ${error instanceof Error ? error.message : String(error)}`,
        }
      }

      // Create the Telegram forum topic
      let newTopic
      try {
        newTopic = await bot.api.createForumTopic(chatId, topicName)
        console.log(`[Integration] Created topic: "${topicName}" (${newTopic.message_thread_id})`)
      } catch (error) {
        return {
          success: false,
          error: `Failed to create Telegram topic: ${error instanceof Error ? error.message : String(error)}`,
        }
      }

      const topicId = newTopic.message_thread_id

      // Create a placeholder mapping IMMEDIATELY to prevent the forum_topic_created
      // event handler from creating a duplicate mapping with a pending session
      topicStore.createMapping(chatId, topicId, topicName, `pending_${Date.now()}`, {})
      topicStore.updateWorkDir(chatId, topicId, workDir)

      // Start OpenCode instance for this topic
      const instance = await instanceManager.getOrCreateInstance(topicId, workDir, {
        name: topicName,
      })

      if (!instance) {
        return {
          success: false,
          error: "Failed to start OpenCode instance",
        }
      }

      // Wait for instance to be ready (up to 30 seconds)
      // The instance:ready event handler runs asynchronously and sets the sessionId
      const startTime = Date.now()
      let sessionId: string | undefined
      const instanceId = instance.config.instanceId
      
      while (Date.now() - startTime < 30000) {
        const current = instanceManager.getInstance(instanceId)
        if (current?.state === "running" && current.sessionId) {
          sessionId = current.sessionId
          break
        }
        if (current?.state === "failed" || current?.state === "crashed") {
          return {
            success: false,
            error: `Instance failed to start: ${current.lastError}`,
          }
        }
        await new Promise((r) => setTimeout(r, 500))
      }

      if (!sessionId) {
        return {
          success: false,
          error: "Instance did not become ready in time",
        }
      }

      // Update the placeholder mapping with the real session ID
      // We need to delete and recreate because there's no updateSessionId method
      topicStore.deleteMapping(chatId, topicId)
      topicStore.createMapping(chatId, topicId, topicName, sessionId, {})
      topicStore.updateWorkDir(chatId, topicId, workDir)
      topicStore.toggleStreaming(chatId, topicId, true) // Enable streaming by default

      // Send welcome message to the new topic
      await bot.api.sendMessage(
        chatId,
        `✅ *OpenCode session started*\n\n` +
        `*Directory:* \`${workDir}\`\n` +
        `*Session:* \`${sessionId.slice(0, 12)}...\`\n\n` +
        `_Send a message to start coding!_`,
        {
          message_thread_id: topicId,
          parse_mode: "Markdown",
        }
      )

      return {
        success: true,
        topicId,
        sessionId,
        directory: workDir,
      }
    } catch (error) {
      console.error(`[Integration] Error creating topic with instance:`, error)
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  // Helper to list all managed project directories
  async function getManagedProjects(): Promise<ManagedProjectInfo[]> {
    const projects: ManagedProjectInfo[] = []
    const basePath = config.project.basePath

    try {
      // Read the project base directory
      const result = await Bun.$`ls -1 ${basePath} 2>/dev/null`.quiet()
      const dirNames = result.stdout.toString().trim().split('\n').filter(Boolean)

      // Get all active sessions for cross-referencing
      const activeSessions = await getActiveSessions()

      for (const name of dirNames) {
        const fullPath = `${basePath}/${name}`
        
        // Check if it's a directory
        try {
          const isDir = await Bun.$`test -d ${fullPath}`.quiet()
          if (isDir.exitCode !== 0) continue
        } catch {
          continue
        }

        // Check if there's an active session for this directory
        const matchingSession = activeSessions.find(s => 
          s.directory === fullPath || s.directory.endsWith(`/${name}`)
        )

        projects.push({
          name,
          path: fullPath,
          hasActiveSession: !!matchingSession,
          topicId: matchingSession?.topicId,
          sessionId: matchingSession?.sessionId,
        })
      }

      // Sort alphabetically by name
      projects.sort((a, b) => a.name.localeCompare(b.name))
    } catch (error) {
      console.error('[Integration] Error listing managed projects:', error)
    }

    return projects
  }

  // Task 02: abort the in-flight turn for a topic (/cancel + ⏹ button).
  // Resolves the client via the SAME lookup chain as the permission branch
  // below (managed sessionToInstance → clients, then discovered_<topicId>
  // fallback, then topic-instance fallback). Never unregisters the session —
  // it stays usable for the next message. Posts exactly one outcome message
  // via sendToTopic; callers (forum.ts) add no extra reply.
  async function handleCancelRequest(chatId: number, topicId: number): Promise<{ ok: boolean; message: string }> {
    const mapping = topicStore.getMapping(chatId, topicId)
    let sessionId = mapping?.sessionId

    // Resolve the client (same chain as the perm: branch)
    let client: OpenCodeClient | undefined
    if (sessionId && !sessionId.startsWith("pending_")) {
      const instanceId = sessionToInstance.get(sessionId)
      if (instanceId) {
        client = clients.get(instanceId)
      }
      if (!client) {
        const destination = streamHandler.getTelegramDestination(sessionId)
        if (destination) {
          client = clients.get(`discovered_${destination.topicId}`)
        }
      }
    }
    // Fall back to the topic's current instance (a restart/ready race may
    // have superseded the mapped sessionId).
    if (!client) {
      const topicInstance = instanceManager.getInstanceByTopic(topicId)
      if (topicInstance) {
        client = clients.get(topicInstance.config.instanceId)
        if (topicInstance.sessionId) {
          sessionId = topicInstance.sessionId
        }
      }
      if (!client) {
        client = clients.get(`discovered_${topicId}`)
      }
    }

    if (!client || !sessionId || sessionId.startsWith("pending_")) {
      const message = "Nothing to cancel — no active session in this topic."
      await sendToTopic(bot, chatId, topicId, message)
      return { ok: false, message }
    }

    // No-op with a polite notice when nothing is in-flight.
    if (!streamHandler.isProcessing(sessionId)) {
      const message = "Nothing in-flight — the session is idle. Send a message to start a turn."
      await sendToTopic(bot, chatId, topicId, message)
      return { ok: false, message }
    }

    try {
      await client.abortSession(sessionId)
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error)
      console.error(`[Integration] Cancel failed for session ${sessionId} (topic ${topicId}): ${reason.slice(0, 120)}`)
      // Abort failures (e.g. the turn already finished) still end the turn
      // locally so state never sticks — the session stays usable.
      streamHandler.markAborted(sessionId)
      const message = `Cancel failed (${reason.slice(0, 80)}) — session still active, try again or send a new message.`
      await sendToTopic(bot, chatId, topicId, message)
      return { ok: false, message }
    }

    streamHandler.markAborted(sessionId)
    stopTypingHeartbeat(topicId)
    // Task 04: abort-aware boundary — buffered text survives the abort; flush
    // promptly as ONE joined prompt (the next turn) instead of waiting out
    // the window. Permission cards are untouched (stream-handler-owned).
    void flushCoalescedTopic(topicId)
    // P1/S11: the edited card IS the receipt — EDIT the progress card in
    // place and strip its keyboard (prevents double-tap races; a second tap
    // lands on the idle path with "Nothing to cancel"). NEW only when there
    // is no card to edit (e.g. /cancel with no visible progress).
    const progressId = streamHandler.getState(sessionId)?.telegramMessageId
    if (progressId) {
      try {
        await sendCallback(
          chatId,
          topicId,
          `⏹ <b>Cancelled</b> <i>— session still active.</i>`,
          { parseMode: "HTML", editMessageId: progressId, inlineKeyboard: [] }
        )
        return { ok: true, message: "⏹ Cancelled — session still active, send a new message." }
      } catch {
        // Edit failed (card deleted) — fall through to the NEW one-liner.
      }
    }
    const message = "⏹ Cancelled — session still active, send a new message."
    await sendToTopic(bot, chatId, topicId, message)
    return { ok: true, message }
  }

  // Register forum commands FIRST (before text handlers so /commands are processed)
  bot.use(createForumCommands({ 
    topicManager, 
    generalAsControlPlane: true,
    topicStore,
    getActiveSessions,
    connectToSession,
    disconnectSession,
    findStaleSessions,
    cleanupStaleSession,
    createTopicWithInstance,
    getManagedProjects,
    onStreamingToggle: (chatId, topicId, enabled) => {
      // Find the session for this topic and update streaming preference
      const mapping = topicStore.getMapping(chatId, topicId)
      console.log(`[Integration] onStreamingToggle called: chatId=${chatId}, topicId=${topicId}, enabled=${enabled}`)
      console.log(`[Integration] Mapping sessionId: ${mapping?.sessionId}`)
      
      if (mapping?.sessionId) {
        // Also try to find session by looking at all registered sessions
        const destination = streamHandler.getTelegramDestination(mapping.sessionId)
        console.log(`[Integration] Session destination: ${JSON.stringify(destination)}`)
        
        streamHandler.setStreamingEnabled(mapping.sessionId, enabled)
        console.log(`[Integration] Streaming ${enabled ? 'enabled' : 'disabled'} for session ${mapping.sessionId}`)
      } else {
        // Fallback: try to find session by topicId in the sessionToInstance map
        for (const [sessionId, instanceId] of sessionToInstance.entries()) {
          const instance = instanceManager.getInstance(instanceId)
          if (instance?.config.topicId === topicId) {
            streamHandler.setStreamingEnabled(sessionId, enabled)
            console.log(`[Integration] Streaming ${enabled ? 'enabled' : 'disabled'} for session ${sessionId} (fallback)`)
            break
          }
        }
      }
    },
    onCancelRequest: handleCancelRequest,
  }))

  // Register forum handlers
  // General topic is control plane only - use /new to create topics for OpenCode sessions
  bot.use(createForumHandlers({
    topicManager,
    handleGeneralTopic: true,
    generalAsControlPlane: true,  // General topic is control plane only (no OpenCode routing)
    allowedChatIds: config.telegram.chatId ? [config.telegram.chatId] : undefined,
    allowedUserIds: config.telegram.allowedUserIds.length > 0 ? config.telegram.allowedUserIds : undefined,
  }))

  // Handle permission callback queries (inline button presses)
  bot.on("callback_query:data", async (ctx) => {
    const data = ctx.callbackQuery.data

    // Parse permission callback data: perm:<response>:<permissionId>
    if (data.startsWith("perm:")) {
      const parts = data.split(":")
      if (parts.length !== 3) {
        await ctx.answerCallbackQuery({ text: "Invalid callback data" })
        return
      }

      const [, response, permissionId] = parts
      
      // Validate response type
      if (!["once", "always", "reject"].includes(response)) {
        await ctx.answerCallbackQuery({ text: "Invalid response type" })
        return
      }

      // Find the pending permission
      const pending = streamHandler.getPendingPermission(permissionId)
      if (!pending) {
        await ctx.answerCallbackQuery({ text: "Permission request expired or already handled" })
        return
      }

      // Find the client for this session
      // First try managed instances
      let client: OpenCodeClient | undefined
      const instanceId = sessionToInstance.get(pending.permission.sessionID)
      if (instanceId) {
        client = clients.get(instanceId)
      }
      
      // If not found, try discovered/reconnected sessions
      if (!client) {
        // Get the topic ID from the stream handler's session registration
        const destination = streamHandler.getTelegramDestination(pending.permission.sessionID)
        if (destination) {
          const discoveredKey = `discovered_${destination.topicId}`
          client = clients.get(discoveredKey)
          console.log(`[Integration] Looking for discovered client with key ${discoveredKey}: ${client ? 'found' : 'not found'}`)
        }
      }

      // H2: session may have been re-created (restart/ready race) so the old
      // sessionID lookup misses — fall back to the topic's current instance.
      if (!client) {
        const topicInstance = instanceManager.getInstanceByTopic(pending.topicId)
        if (topicInstance) {
          client = clients.get(topicInstance.config.instanceId)
          console.log(
            `[Integration] Permission fallback to topic ${pending.topicId} instance ${topicInstance.config.instanceId}: ${client ? 'found' : 'not found'} ` +
            `(old session ${pending.permission.sessionID}, current ${topicInstance.sessionId ?? 'none'})`
          )
        }
        if (!client) {
          const discoveredKey = `discovered_${pending.topicId}`
          client = clients.get(discoveredKey)
          if (client) {
            console.log(`[Integration] Permission fallback to discovered client ${discoveredKey}`)
          }
        }
      }
      
      if (!client) {
        // H2: never leave a permission stall silent — tell the topic to resend
        // and drop the dead pending entry (its reminder timer is cleared too).
        console.error(`[Integration] No client found for session ${pending.permission.sessionID} (topic ${pending.topicId})`)
        await ctx.answerCallbackQuery({ text: "Session not found — see topic for details", show_alert: true })
        try {
          await sendCallback(
            pending.chatId,
            pending.topicId,
            `⚠️ Permission request expired: the OpenCode session restarted and the pending approval can no longer be answered.\n\n` +
            `Please resend your message.`,
            {
              parseMode: "HTML",
              inlineKeyboard: [
                [{ text: "🔁 Resend last message", callback_data: `retry:${pending.topicId}` }],
              ],
            }
          )
        } catch (notifyError) {
          console.error(`[Integration] Failed to send permission-expired notice:`, notifyError)
        }
        streamHandler.removePendingPermission(permissionId)
        return
      }

      try {
        // Send response to OpenCode
        await client.respondToPermission(
          pending.permission.sessionID,
          permissionId,
          response as "once" | "always" | "reject"
        )

        // Update the message to show it was handled
        const responseText = response === "reject" 
          ? "❌ Permission denied" 
          : response === "always"
            ? "✅ Permission granted (always)"
            : "✅ Permission granted (once)"

        try {
          await ctx.editMessageText(
            `${responseText}\n\n<i>${pending.permission.title}</i>`,
            { parse_mode: "HTML" }
          )
        } catch {
          // Ignore edit errors
        }

        // Clean up
        streamHandler.removePendingPermission(permissionId)

        await ctx.answerCallbackQuery({ text: responseText })
      } catch (error) {
        console.error("[Integration] Failed to respond to permission:", error)
        await ctx.answerCallbackQuery({ 
          text: "Failed to process permission response",
          show_alert: true 
        })
      }

      return
    }

    // P1 (I-1): 🔁 Retry — re-sends the topic's last prompt via the normal
    // send path (server-side text lookup; no prompt text in callback data).
    // Exactly-once: taps while the session is busy are ignored (no
    // duplicate-send), and a completed turn has no tracked prompt left
    // (cleared on idle) so it can never be re-sent — the user gets an
    // explicit notice instead. NEVER auto-resends silently: every tap ends
    // in an ack popup and/or a visible topic notice.
    if (data.startsWith("retry:")) {
      const retryTopicId = parseInt(data.slice("retry:".length), 10)
      const chatId = ctx.callbackQuery.message?.chat.id
      if (!chatId || !Number.isFinite(retryTopicId)) {
        await ctx.answerCallbackQuery({ text: "Invalid retry button" })
        return
      }
      // Busy → duplicate tap: ack only, never re-send.
      if (isTopicSessionBusy(chatId, retryTopicId)) {
        await ctx.answerCallbackQuery({ text: "Already running — tap ignored" })
        return
      }
      const pending = lastPromptByTopic.get(retryTopicId)
      if (!pending) {
        // No tracked prompt: the turn already completed (idle clears the
        // entry) — re-sending would duplicate a finished answer.
        await ctx.answerCallbackQuery({ text: "Already completed — nothing to resend" })
        try {
          await sendToTopic(
            bot,
            chatId,
            retryTopicId,
            `✅ Already completed — no need to resend. Send a new message if you'd like to follow up.`
          )
        } catch (notifyError) {
          console.error(`[Integration] Failed to send retry-completed notice:`, notifyError)
        }
        return
      }
      if (Date.now() - pending.at > 10 * 60 * 1000) {
        // Stale prompt (same 10-min window as the H3 restart notice) —
        // re-sending ancient text would surprise; ask for a manual resend.
        await ctx.answerCallbackQuery({ text: "Too old to resend automatically" })
        try {
          await sendToTopic(
            bot,
            chatId,
            retryTopicId,
            `⏳ That message is too old to resend automatically — please send it again.`
          )
        } catch (notifyError) {
          console.error(`[Integration] Failed to send retry-stale notice:`, notifyError)
        }
        return
      }
      await ctx.answerCallbackQuery({ text: "Resending…" })
      try {
        // Immediate resend through the unchanged router (owns echo guard,
        // system-prompt injection, lastPromptByTopic, typing heartbeat).
        // Bypasses the coalescing window — the tap IS the explicit confirm.
        // Log lengths only, never message text (secrets).
        console.log(`[Integration] Retry tap resending topic ${retryTopicId} (len=${pending.text.length})`)
        await routeMessageToInstance({
          messageId: 0,
          chatId,
          topicId: retryTopicId,
          userId: ctx.from?.id ?? 0,
          text: pending.text,
          isGeneralTopic: false,
          isReply: false,
        })
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error)
        console.error(`[Integration] Retry resend failed for topic ${retryTopicId}: ${reason.slice(0, 120)}`)
        try {
          await sendToTopic(bot, chatId, retryTopicId, `❌ Retry failed (${reason.slice(0, 80)}) — please send your message again.`)
        } catch {
          // Best-effort failure notice.
        }
      }
      return
    }

    // P1 (S12/S14 affordance): 🔄 Restart — P2 (I-5) will make this start a
    // fresh session one-tap. P1 keeps the button honest: instant ack + an
    // explicit NEW notice with the next step (any message starts fresh).
    // Never a dead tap, never silent.
    if (data.startsWith("restart:")) {
      const restartTopicId = parseInt(data.slice("restart:".length), 10)
      const chatId = ctx.callbackQuery.message?.chat.id
      if (!chatId || !Number.isFinite(restartTopicId)) {
        await ctx.answerCallbackQuery({ text: "Invalid button" })
        return
      }
      await ctx.answerCallbackQuery({ text: "Restarting…" })
      try {
        await sendToTopic(
          bot,
          chatId,
          restartTopicId,
          `🔄 <b>Restart requested.</b> Send any message to start a fresh session — history is kept.`,
          "HTML"
        )
      } catch (notifyError) {
        console.error(`[Integration] Failed to send restart notice:`, notifyError)
      }
      return
    }

    // Unknown callback - ignore
    await ctx.answerCallbackQuery()
  })

  // Add status command
  bot.command("status", async (ctx) => {
    const instances = instanceManager.getAllInstances()
    const running = instances.filter((i) => i.state === "running")
    
    let status = `**OpenCode Orchestrator Status**\n\n`
    status += `Running instances: ${running.length}/${config.opencode.maxInstances}\n`
    status += `Active SSE subscriptions: ${sseSubscriptions.size}\n\n`

    if (running.length > 0) {
      status += `**Active Instances:**\n`
      for (const instance of running) {
        const elapsed = instance.startedAt 
          ? Math.round((Date.now() - instance.startedAt.getTime()) / 1000 / 60)
          : 0
        status += `- Topic ${instance.config.topicId}: Port ${instance.port} (${elapsed}m)\n`
      }
    }

    await ctx.reply(status, { parse_mode: "Markdown" })
  })

  // Error handler
  bot.catch((err) => {
    console.error("[Integration] Bot error:", err)
  })

  // Create API server for external instance registration
  const apiPort = parseInt(process.env.API_PORT || "4200")
  apiServer = createApiServer({
    port: apiPort,
    bot,
    config,
    topicStore,
    streamHandler,
    apiKey: process.env.API_KEY,
  })

  console.log("[Integration] Components initialized")

  return {
    bot,
    topicManager,
    instanceManager,
    streamHandler,
    apiServer,

    async start() {
      console.log("[Integration] Starting application...")

      // Recover orchestrator state
      await instanceManager.recover()

      // Start bot (409 here means another poller is polling right now)
      try {
        await bot.start({
          allowed_updates: ["message", "edited_message", "callback_query"],
          onStart: (info) => {
            console.log(`[Integration] Bot started as @${info.username}`)
          },
        })
      } catch (error) {
        if (isTelegramConflictError(error)) {
          console.error(
            `[Integration] Telegram 409 getUpdates conflict — another poller is polling this token. ` +
            `Kill the other instance (see lock: ${BOT_LOCKFILE_PATH}) and restart.`
          )
        }
        throw error
      }
    },

    async stop() {
      console.log("[Integration] Stopping application...")

      // Stop API server
      apiServer.stop()

      // Stop SSE subscriptions
      for (const [id, abort] of sseSubscriptions) {
        abort()
      }
      sseSubscriptions.clear()

      // Close clients
      for (const [id, client] of clients) {
        client.close()
      }
      clients.clear()

      // Clear stream handler
      streamHandler.clear()

      // Stop orchestrator
      await instanceManager.shutdown()

      // Close topic store
      topicStore.close()

      // Stop bot
      await bot.stop()

      console.log("[Integration] Application stopped")
    },

    getInstance(topicId: number) {
      return instanceManager.getInstanceByTopic(topicId)
    },

    getClient(instanceId: string) {
      return clients.get(instanceId)
    },
  }
}
