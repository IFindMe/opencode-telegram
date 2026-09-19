/**
 * Configuration Management
 * 
 * Loads and validates configuration from environment variables.
 */

import type { ManagerConfig, PortPoolConfig } from "./types/orchestrator"
import type { TopicManagerConfig } from "./types/forum"

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Telegram bot configuration
 */
export interface TelegramConfig {
  /** Bot token from @BotFather */
  botToken: string
  
  /** Supergroup chat ID (negative number) */
  chatId: number
  
  /** Allowed user IDs (empty = all users) */
  allowedUserIds: number[]
  
  /** Whether to handle the General topic */
  handleGeneralTopic: boolean
}

/**
 * OpenCode configuration
 */
export interface OpenCodeConfig {
  /** Path to opencode binary */
  binaryPath: string
  
  /** Maximum concurrent instances */
  maxInstances: number
  
  /** Idle timeout in milliseconds */
  idleTimeoutMs: number
  
  /** Port range start */
  portStart: number
  
  /** Port range size */
  portPoolSize: number
  
  /** Health check interval in milliseconds */
  healthCheckIntervalMs: number
  
  /** Startup timeout in milliseconds */
  startupTimeoutMs: number

  /** Minimum interval between Telegram progress edits in ms (default: 1000, clamped 500-5000 via STREAM_UPDATE_INTERVAL_MS) */
  streamUpdateIntervalMs: number

  /** Bot-wide floor between Telegram API calls in ms (default: 40 ≈ 25 msg/s via TELEGRAM_SEND_INTERVAL_MS) */
  sendIntervalMs: number

  /** Permission types auto-answered "once" without asking (default: read-only; unknown kinds always ask; override: PERMISSIONS_AUTO_ALLOW) */
  permissionsAutoAllow: string[]
}

/**
 * Storage configuration
 */
export interface StorageConfig {
  /** Path to orchestrator SQLite database */
  orchestratorDbPath: string
  
  /** Path to topic store SQLite database */
  topicDbPath: string
}

/**
 * Project configuration
 */
export interface ProjectConfig {
  /** Base path for projects (topics will create subdirectories here) */
  basePath: string
  
  /** Whether to auto-create project directories */
  autoCreateDirs: boolean
}

/**
 * Full application configuration
 */
export interface AppConfig {
  telegram: TelegramConfig
  opencode: OpenCodeConfig
  storage: StorageConfig
  project: ProjectConfig
}

// =============================================================================
// Default Values
// =============================================================================

const DEFAULT_CONFIG: AppConfig = {
  telegram: {
    botToken: "",
    chatId: 0,
    allowedUserIds: [],
    handleGeneralTopic: true,
  },
  opencode: {
    binaryPath: "opencode",
    maxInstances: 10,
    idleTimeoutMs: 30 * 60 * 1000, // 30 minutes
    portStart: 4100,
    portPoolSize: 100,
    healthCheckIntervalMs: 30_000,
    startupTimeoutMs: 60_000,
    streamUpdateIntervalMs: 1000, // 1s Telegram progress-edit floor (override: STREAM_UPDATE_INTERVAL_MS)
    sendIntervalMs: 40, // bot-wide pacing ≈25 msg/s (override: TELEGRAM_SEND_INTERVAL_MS)
    permissionsAutoAllow: ["read", "glob", "grep", "list", "lsp"], // read-only auto-allow; write/exec/network/external_directory ask (override: PERMISSIONS_AUTO_ALLOW)
  },
  storage: {
    orchestratorDbPath: "./data/orchestrator.db",
    topicDbPath: "./data/topics.db",
  },
  project: {
    basePath: `${process.env.HOME || "/tmp"}/oc-bot`,
    autoCreateDirs: true,
  },
}

// =============================================================================
// Configuration Loading
// =============================================================================

/**
 * Load configuration from environment variables
 */
export function loadConfig(): AppConfig {
  const config: AppConfig = {
    telegram: {
      botToken: requireEnv("TELEGRAM_BOT_TOKEN"),
      chatId: parseIntEnv("TELEGRAM_CHAT_ID", 0),
      allowedUserIds: parseIntArrayEnv("TELEGRAM_ALLOWED_USERS"),
      handleGeneralTopic: parseBoolEnv("HANDLE_GENERAL_TOPIC", true),
    },
    opencode: {
      binaryPath: getEnv("OPENCODE_PATH", DEFAULT_CONFIG.opencode.binaryPath),
      maxInstances: parseIntEnv("OPENCODE_MAX_INSTANCES", DEFAULT_CONFIG.opencode.maxInstances),
      idleTimeoutMs: parseIntEnv("OPENCODE_IDLE_TIMEOUT_MS", DEFAULT_CONFIG.opencode.idleTimeoutMs),
      portStart: parseIntEnv("OPENCODE_PORT_START", DEFAULT_CONFIG.opencode.portStart),
      portPoolSize: parseIntEnv("OPENCODE_PORT_POOL_SIZE", DEFAULT_CONFIG.opencode.portPoolSize),
      healthCheckIntervalMs: parseIntEnv("OPENCODE_HEALTH_CHECK_INTERVAL_MS", DEFAULT_CONFIG.opencode.healthCheckIntervalMs),
      startupTimeoutMs: parseIntEnv("OPENCODE_STARTUP_TIMEOUT_MS", DEFAULT_CONFIG.opencode.startupTimeoutMs),
      streamUpdateIntervalMs: Math.min(5000, Math.max(500, parseIntEnv("STREAM_UPDATE_INTERVAL_MS", DEFAULT_CONFIG.opencode.streamUpdateIntervalMs))),
      sendIntervalMs: Math.min(1000, Math.max(10, parseIntEnv("TELEGRAM_SEND_INTERVAL_MS", DEFAULT_CONFIG.opencode.sendIntervalMs))),
      permissionsAutoAllow: parseStringListEnv("PERMISSIONS_AUTO_ALLOW", DEFAULT_CONFIG.opencode.permissionsAutoAllow),
    },
    storage: {
      orchestratorDbPath: getEnv("ORCHESTRATOR_DB_PATH", DEFAULT_CONFIG.storage.orchestratorDbPath),
      topicDbPath: getEnv("TOPIC_DB_PATH", DEFAULT_CONFIG.storage.topicDbPath),
    },
    project: {
      basePath: getEnv("PROJECT_BASE_PATH", DEFAULT_CONFIG.project.basePath),
      autoCreateDirs: parseBoolEnv("AUTO_CREATE_PROJECT_DIRS", DEFAULT_CONFIG.project.autoCreateDirs),
    },
  }

  return config
}

/**
 * Validate configuration
 */
export function validateConfig(config: AppConfig): { valid: boolean; errors: string[] } {
  const errors: string[] = []

  // Telegram validation
  if (!config.telegram.botToken) {
    errors.push("TELEGRAM_BOT_TOKEN is required")
  }
  if (!config.telegram.chatId) {
    errors.push("TELEGRAM_CHAT_ID is required")
  }

  // OpenCode validation
  if (config.opencode.maxInstances < 1) {
    errors.push("OPENCODE_MAX_INSTANCES must be at least 1")
  }
  if (config.opencode.portPoolSize < config.opencode.maxInstances) {
    errors.push("OPENCODE_PORT_POOL_SIZE must be >= OPENCODE_MAX_INSTANCES")
  }

  return {
    valid: errors.length === 0,
    errors,
  }
}

// =============================================================================
// Configuration Converters
// =============================================================================

/**
 * Convert AppConfig to ManagerConfig for the orchestrator
 */
export function toManagerConfig(config: AppConfig): Partial<ManagerConfig> {
  return {
    maxInstances: config.opencode.maxInstances,
    portPool: {
      startPort: config.opencode.portStart,
      poolSize: config.opencode.portPoolSize,
    },
    healthCheckIntervalMs: config.opencode.healthCheckIntervalMs,
    startupTimeoutMs: config.opencode.startupTimeoutMs,
    defaultIdleTimeoutMs: config.opencode.idleTimeoutMs,
    statePath: config.storage.orchestratorDbPath,
    opencodePath: config.opencode.binaryPath,
  }
}

/**
 * Convert AppConfig to TopicManagerConfig
 */
export function toTopicManagerConfig(config: AppConfig): TopicManagerConfig {
  return {
    databasePath: config.storage.topicDbPath,
    autoCreateSessions: true,
    handleGeneralTopic: config.telegram.handleGeneralTopic,
  }
}

// =============================================================================
// Environment Helpers
// =============================================================================

function getEnv(key: string, defaultValue: string): string {
  return process.env[key] ?? defaultValue
}

function requireEnv(key: string): string {
  const value = process.env[key]
  if (!value) {
    throw new Error(`Required environment variable ${key} is not set`)
  }
  return value
}

function parseIntEnv(key: string, defaultValue: number): number {
  const value = process.env[key]
  if (!value) return defaultValue
  const parsed = parseInt(value, 10)
  return isNaN(parsed) ? defaultValue : parsed
}

function parseBoolEnv(key: string, defaultValue: boolean): boolean {
  const value = process.env[key]?.toLowerCase()
  if (!value) return defaultValue
  return value === "true" || value === "1" || value === "yes"
}

function parseIntArrayEnv(key: string): number[] {
  const value = process.env[key]
  if (!value) return []
  return value
    .split(",")
    .map((s) => parseInt(s.trim(), 10))
    .filter((n) => !isNaN(n))
}

/**
 * Parse a comma-separated string list (total, side-effect-free).
 * Unset → copy of the default; set-but-empty → [] (deny all).
 * Entries are trimmed + lowercased for case-insensitive matching.
 */
function parseStringListEnv(key: string, defaultValue: string[]): string[] {
  const value = process.env[key]
  if (value === undefined) return [...defaultValue]
  return value
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter((s) => s.length > 0)
}

// =============================================================================
// Configuration Display
// =============================================================================

/**
 * Print configuration (with sensitive values masked)
 */
export function printConfig(config: AppConfig): void {
  console.log("\n=== Configuration ===")
  console.log("\nTelegram:")
  console.log(`  Bot Token: ${maskToken(config.telegram.botToken)}`)
  console.log(`  Chat ID: ${config.telegram.chatId}`)
  console.log(`  Allowed Users: ${config.telegram.allowedUserIds.length > 0 ? config.telegram.allowedUserIds.join(", ") : "(all)"}`)
  console.log(`  Handle General Topic: ${config.telegram.handleGeneralTopic}`)
  
  console.log("\nOpenCode:")
  console.log(`  Binary Path: ${config.opencode.binaryPath}`)
  console.log(`  Max Instances: ${config.opencode.maxInstances}`)
  console.log(`  Idle Timeout: ${config.opencode.idleTimeoutMs / 1000 / 60} minutes`)
  console.log(`  Port Range: ${config.opencode.portStart}-${config.opencode.portStart + config.opencode.portPoolSize - 1}`)
  console.log(`  Permissions Auto-allow: ${config.opencode.permissionsAutoAllow.length > 0 ? config.opencode.permissionsAutoAllow.join(", ") : "(none — all ask)"}`)
  
  console.log("\nStorage:")
  console.log(`  Orchestrator DB: ${config.storage.orchestratorDbPath}`)
  console.log(`  Topic DB: ${config.storage.topicDbPath}`)
  
  console.log("\nProject:")
  console.log(`  Base Path: ${config.project.basePath}`)
  console.log(`  Auto-create Dirs: ${config.project.autoCreateDirs}`)
  console.log("")
}

function maskToken(token: string): string {
  if (!token) return "(not set)"
  if (token.length < 10) return "****"
  return token.slice(0, 5) + "..." + token.slice(-4)
}
