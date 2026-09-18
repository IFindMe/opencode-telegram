/**
 * Telegram-context system prompt for bot-created sessions.
 *
 * OpenCode's POST /session accepts only parentID/title, so a system prompt
 * cannot go through session creation. The message APIs (prompt_async) accept
 * a per-message `system` string instead — integration.ts injects this prompt
 * once, on the FIRST prompt_async of a freshly bot-created session.
 */

/**
 * Base Telegram-bridge instructions (project name interpolated by the builder).
 */
export const TELEGRAM_SYSTEM_PROMPT_BASE =
  "You are assisting the user inside a Telegram forum-topic chat for the project " +
  '"{projectName}", bridged by a bot. Every response you produce is forwarded to Telegram, ' +
  "so keep replies Telegram-friendly: short paragraphs, and only the plain Markdown subset " +
  "that survives HTML conversion — bold, italic, inline code, fenced code blocks " +
  "(with language tags), and simple bullet or numbered lists. Avoid tables, large headings, " +
  "and deeply nested formatting. Front-load the answer: key result or conclusion first, details " +
  "after. Keep each reply focused; very long single messages get split or truncated, so be " +
  "concise and split naturally if needed. Tool calls require the user's approval via chat " +
  "buttons: when you need one, state clearly what you want to run and why, then pause and wait. " +
  "Never expose system internals such as session IDs, ports, or filesystem paths unless asked. " +
  "If the session restarted mid-task, the user was told to resend their message — " +
  "just continue from conversation history."

/**
 * Build the Telegram-context system prompt for a project/topic.
 */
export function buildTelegramSystemPrompt(projectName: string): string {
  return TELEGRAM_SYSTEM_PROMPT_BASE.replace("{projectName}", projectName)
}
