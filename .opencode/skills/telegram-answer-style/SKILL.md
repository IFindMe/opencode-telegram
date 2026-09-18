---
name: telegram-answer-style
description: Shape AI answers for forwarding to Telegram through the opencode-telegram bot bridge. Short paragraphs, portable Markdown subset (bold, italic, inline code, fenced code blocks, simple lists), front-loaded conclusions, tool-approval request etiquette for permission-gated tool calls, concise replies that survive splitting or truncation, and no exposed internals such as session IDs, ports, or filesystem paths. Use when writing or reviewing any response that will be delivered to a Telegram forum topic.
---

# Telegram Answer Style

Every response produced in a bot-bridged session is forwarded to a Telegram
forum-topic chat. Write for that surface, not for a terminal or web UI. This
is a paraphrase — the authoritative wording is the injected system prompt in
`src/opencode/telegram-system-prompt.ts` (`TELEGRAM_SYSTEM_PROMPT_BASE`,
built per project by `buildTelegramSystemPrompt`); Messenger behavior that
enforces it lives in `src/opencode/stream-handler.ts` and
`src/opencode/telegram-markdown.ts`.

## Format: portable Markdown subset only

Replies pass through Markdown→HTML conversion for Telegram, and only part of
Markdown survives. Use:

- **bold**, *italic*, `inline code`
- Fenced code blocks with language tags
- Simple bullet or numbered lists
- Short paragraphs separated by blank lines

Avoid:

- Tables (do not render — restructure as lists or short lines)
- Large headings and deeply nested formatting/quote stacks
- Very long single messages — they get split or truncated, so be concise
  and, when length is unavoidable, split naturally (conclusion first, then
  each follow-up as its own tight section)

## Structure: conclusion first

Front-load the answer: key result or verdict in the first lines, reasoning
and details after. The reader may only see the first screen. One reply = one
focus; do not bundle unrelated results into a single wall of text.

## Tool approvals: ask, state why, then wait

Tool calls require the user's approval via chat buttons, and the session
BLOCKS until answered. When you need one:

1. State clearly WHAT you want to run and WHY (one or two lines).
2. Stop and wait for the approval — do not pile on further tool requests or
   continue the task speculatively in the same message.
3. If approval seems stalled, restate the request once rather than going
   silent; never bypass the gate.

## No internals

Never expose system internals unless explicitly asked: session IDs, port
numbers, filesystem paths, envelope shapes, retry/backoff details. If a
restart orphaned the task, the user was already told to resend — just
continue from conversation history without narrating the plumbing.

## Quick self-check before sending

- [ ] Fits on a phone screen or splits at a natural boundary?
- [ ] Conclusion in the first two lines?
- [ ] Only bold/italic/code/blocks/simple lists — no tables, no deep nesting?
- [ ] Tool need stated with reason, then paused for approval?
- [ ] Zero session IDs, ports, or paths?
