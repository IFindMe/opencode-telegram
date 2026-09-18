---
name: silent-stop-debug
description: Diagnose the opencode-telegram "acknowledged then silence" failure, where a Telegram prompt gets first streamed tokens but then no completion, error, or progress. Covers the ranked fault map (SSE stream loss with bounded reconnect, permission-approval waits, crash or restart orphaned prompts, final-render failures, session-ID mismatch drops), exact log grep patterns, and the file and line map across src/opencode/client.ts, src/integration.ts, src/opencode/stream-handler.ts, and src/api-server.ts. Use when a prompt goes silent after starting, or when verifying the SSE resilience behavior.
---

# Silent-Stop Debug

Diagnosing "acknowledged then silence" in this repo: user sends a prompt in a
topic, sees initial assistant text, then nothing — no progress, no error.

## Key reframe

The "acknowledgement" (e.g. "ok it will do it") is NOT a bot message — that
string exists nowhere in `src/`. It is the LLM's first streamed tokens, which
means streaming STARTED and the fault lies strictly AFTER `prompt_async` was
accepted. A failed send is LOUD (the bot posts `Error: ...` to the topic), so
silence means: send accepted, completion signal never rendered.

Distilled from `AgentsReport/detective/2026-09-18_silent-stop.md` and
`AgentsReport/reviewer/2026-09-18_silent-stop-fix-review-v2.md` — read those
for full evidence; this file is the working summary. Line numbers below are
approximate; confirm with grep before citing.

## Ranked fault map (current code already has mitigations)

1. **SSE stream drop (was H1, now bounded).** `startSSE`
   (`src/opencode/client.ts`, ~:326-430) reconnects with backoff: 5 retries,
   1s→15s, budget reset on every received event (~:399). If the stream is
   lost for good, the topic gets an honest notice ("...Check /status in
   General — a restart may be needed to restore live updates.",
   `src/integration.ts` ~:350, parity `src/api-server.ts` ~:287). Suspect
   this when silence follows `SSE error for` / `SSE connection lost` with no
   later recovery, or when a zero-event stream exhausts retries over a long
   idle uptime (known residual gap: budget resets on parsed events, not on
   bare TCP connect).
2. **Permission wait (H2).** `handlePermissionUpdated`
   (`src/opencode/stream-handler.ts` ~:736) posts an approve/deny card and
   BLOCKS the session until answered — no timeout, no auto-deny. If the user
   misses it, or resolution fails with "Session not found" (session
   re-created mid-flight), the session stalls forever with no further output.
   Malformed permission envelopes (missing `id`/`sessionID`) now get a
   best-effort notice (~:745-769); a fully sessionId-less envelope still
   drops log-only (~:199-205, needs live-shape data).
3. **Crash/restart orphan (H3).** Crash IS announced ("Instance crashed,
   restarting..."), but the accepted prompt belonged to the old session and
   is never re-sent. Mitigation: prompts are tracked per topic
   (`lastPromptByTopic`, `src/integration.ts` ~:108; activity marked via
   `extractSseSessionId`/`markPromptHadActivity` ~:360-380 and wired into all
   subscribe wrappers); after `instance:ready` with a new session, the topic
   gets a resend nudge if the prior prompt had activity (~:592-597). Edge: a
   prompt accepted seconds before an instant crash (zero events) yields only
   the crash line, no nudge — not silent, but tell the user to resend.
4. **Final-render failure (H4, contributor).** Completion arrived but the
   final edit failed (over-length after Markdown→HTML expansion, unparseable
   entities) and fell into a log-only branch, leaving a frozen progress
   message that looks like "ack then silence". Check message length and
   `Final edit failed` / parse-error lines.
5. **Session mismatch drop (H5, contributor).** `handleEvent` drops events
   for unregistered sessions with only a log line
   (`src/opencode/stream-handler.ts` ~:209-210) — stale mapping after
   remap/restart, or TUI-reconnect registering a new ID without unregistering
   the old one.

Eliminated as primaries (do not chase first): send failure (loud + retried),
idle-timeout race (30-min default, activity reset per event), Telegram 429 on
the final answer (explicitly retried), wrong-session-from-health-list.

## Exact log greps (run around the failure window)

1. Send accepted? `Sent message|sendMessageAsync|prompt_async` — if logged
   OK, send-failure is out.
2. Stream life? `SSE event` — stops after `message.part.updated` with no
   `session.idle`/`session.error` ⇒ drop (1) or permission wait (2). Ends
   with `SSE error for|SSE connection lost` ⇒ (1) confirmed.
   `permission.updated` with no later `permission.replied` ⇒ (2) confirmed.
   `Process exited unexpectedly|Health check failed while running|Instance .*
   crashed` ⇒ (3).
3. Mismatch? `not registered|has no sessionID` ⇒ (5); compare the session ID
   against `Instance .* ready with session` lines.
4. Render? `Rate limited|message is too long|can't parse entities|Final edit
   failed|message to edit not found` at completion time ⇒ (4).
5. Brackets? `Instance .* ready with session|Updating topic name|Session
   stopped due to inactivity` — ready/crash/idle markers around the silence
   separate (3) from idle expiry.

Then: send `/status` in General, record `Active SSE subscriptions` plus
instance state; send one more message in the stuck topic and note whether it
recovers (transient) or errors loudly (stale session).

## File:line map

- `src/opencode/client.ts` — `subscribe` (~:299), `startSSE` with
  reconnect/backoff (~:326-430, `maxRetries = 5`, event-reset ~:399).
- `src/integration.ts` — prompt tracking (`lastPromptByTopic` ~:108);
  unrecoverable-SSE notice (~:342-350); session-ID extraction + activity
  marking (~:353-380); `instance:ready` resubscribe + resend nudge (~:560-620);
  send paths recording prompts (~:719, :796, :895, :975); permission-resolve
  lookup with "Session not found" (~:1500-1523 area).
- `src/opencode/stream-handler.ts` — event routing + `not registered` drop
  (~:180-245); `handleSessionIdle` (~:478); `session.error` loud path
  (~:623-661); `handlePermissionUpdated` + malformed-envelope notices (~:736-769).
- `src/api-server.ts` — external-instance SSE parity: subscribe (~:268),
  log-only error + shared remedy text (~:279-288).
