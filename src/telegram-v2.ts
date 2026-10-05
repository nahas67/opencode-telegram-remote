/**
 * Telegram notifications + remote control for OpenCode V2.
 *
 * Originally ported from Tommertomtom/opencoder-telegram-plugin (V1 plugin API)
 * to the OpenCode V2 plugin contract: default-exports a definition with `id` and
 * `setup(ctx)`, and subscribes to the public server event stream through
 * `ctx.event.subscribe()`.
 *
 * Stage 1 + 2 of REMOTE-CONTROL-DESIGN.md are implemented here: the durable
 * state model (Â§3), the command surface (Â§4), inline-keyboard interaction (Â§5),
 * the per-session turn state machine (Â§6), the permission flow (Â§7), the
 * security model (Â§8), the error matrix (Â§9), the redesigned notification (Â§10),
 * the audit formats (Â§11), the cost guardrails (Â§12) and the standing decisions
 * in §14. Execution-capable paths (free-text `prompt`, `/perm` allow) stay
 * behind `mode === "control"`; there is no second factor by owner request.
 */
import { randomBytes } from "node:crypto";
import { exec as execCb } from "node:child_process";
import {
  appendFileSync,
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { homedir, tmpdir } from "node:os";
import { promisify } from "node:util";

const execAsync = promisify(execCb);
import { basename, dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { config as loadEnv } from "dotenv";
import { Bot } from "grammy";

// --- configuration ---------------------------------------------------------

const PLUGIN_DIR = dirname(fileURLToPath(import.meta.url));

// Append-only delivery audit. OpenCode does not surface plugin console output,
// so this file is the only way to confirm a notification actually reached
// Telegram rather than silently failing. Failures here never block delivery.
function audit(line: string): void {
  try {
    appendFileSync(
      resolve(PLUGIN_DIR, "sent.log"),
      `${new Date().toISOString()} ${line}\n`,
    );
  } catch {
    /* auditing must never break delivery */
  }
}

// Candidate .env locations, most specific first. dotenv never overrides a
// variable that is already set, so the first candidate defining a value wins.
// Resolution is anchored to this file rather than process.cwd() so the plugin
// works no matter which directory OpenCode was started from.
function envCandidates(): string[] {
  return [
    process.env.TELEGRAM_ENV_FILE,
    resolve(PLUGIN_DIR, ".env"),
    resolve(PLUGIN_DIR, "../../.env"),
    resolve(process.cwd(), ".env"),
  ].filter((path): path is string => Boolean(path));
}

function envInt(name: string, fallback: number): number {
  const raw = (process.env[name] ?? "").trim();
  if (raw === "") return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

interface PluginConfig {
  token: string;
  /**
   * Optional second bot used ONLY for buttonless notifications (turn finished,
   * agent idle, etc). Separation matters: this token can send messages but
   * cannot answer a command, so leaking it does not grant code execution.
   * Control replies, permission buttons and setMyCommands stay on `token`.
   */
  notifyToken?: string;
  /**
   * Optional Whisper-compatible transcription endpoint (OpenAI
   * `/audio/transcriptions` shape). Without both URL and key, voice messages
   * get an honest "not configured" reply instead of fake transcription.
   */
  whisperUrl?: string;
  whisperKey?: string;
  allowed: number[];
  chatId?: number;
  // Â§8 "Idle TTL - binding expires after inactivity". Generous default so an
  // overnight phone does not silently lose its binding.
  bindingTtlMs: number;
  // Â§12 optional daily prompt cap; 0 disables it.
  dailyPromptCap: number;
  // Â§8 "Per-command allowlist option". Undefined means "every command".
  allowedCommands?: string[];
}

function loadConfig(): PluginConfig {
  for (const path of envCandidates()) {
    if (existsSync(path)) {
      loadEnv({ path });
      console.log(`[telegram-notify] loaded environment from ${path}`);
    }
  }

  const token = (process.env.TELEGRAM_BOT_TOKEN ?? "").trim();
  if (!token) throw new Error("Missing TELEGRAM_BOT_TOKEN");

  const allowed = (process.env.TELEGRAM_ALLOWED_USER_IDS ?? "")
    .split(",")
    .map((id) => id.trim())
    .filter((id) => id !== "")
    .map((id) => Number.parseInt(id, 10))
    .filter((id) => !Number.isNaN(id));

  if (allowed.length === 0) {
    throw new Error("Missing or invalid TELEGRAM_ALLOWED_USER_IDS");
  }
  if (allowed.includes(0)) {
    console.warn(
      "[telegram-notify] TELEGRAM_ALLOWED_USER_IDS still contains the placeholder 0 - " +
        "the bot will ignore every message until a real Telegram user ID is set.",
    );
  }

  const chatIdRaw = (process.env.TELEGRAM_CHAT_ID ?? "").trim();
  const chatId = chatIdRaw ? Number.parseInt(chatIdRaw, 10) : Number.NaN;

  const commandsRaw = (process.env.TELEGRAM_ALLOWED_COMMANDS ?? "").trim();
  const allowedCommands =
    commandsRaw === ""
      ? undefined
      : commandsRaw
          .split(",")
          .map((entry) => entry.trim().toLowerCase())
          .filter((entry) => entry !== "");

  const notifyTokenRaw = (process.env.TELEGRAM_NOTIFY_TOKEN ?? "").trim();
  // Identical tokens would double every message, so treat that as "not set".
  const notifyToken =
    notifyTokenRaw === "" || notifyTokenRaw === token ? undefined : notifyTokenRaw;

  return {
    token,
    notifyToken,
    whisperUrl: (process.env.TELEGRAM_WHISPER_URL ?? "").trim() || undefined,
    whisperKey: (process.env.TELEGRAM_WHISPER_KEY ?? "").trim() || undefined,
    allowed,
    chatId: Number.isNaN(chatId) ? undefined : chatId,
    bindingTtlMs: envInt("TELEGRAM_BINDING_TTL_MIN", 24 * 60) * 60_000,
    dailyPromptCap: envInt("TELEGRAM_DAILY_PROMPT_CAP", 0),
    allowedCommands,
  };
}

// --- limits (Â§9 chunking, Â§12 caps, Â§14.3/5/6) -----------------------------

const TELEGRAM_MAX = 4096;
const SEND_QUEUE_CAP = 25;
const PROMPT_CHAR_CAP = 4000;

// How many sessions /tasks hydrates titles for. Bounds the session.get fan-out
// on machines with many sessions; display already shows fewer.
const LIST_LIMIT = 25;
const HEARTBEAT_MS = 120_000;
const HEARTBEAT_MAX = 4;
const PERMISSION_TTL_MS = 10 * 60_000;
const ACTION_TTL_MS = 10 * 60_000;
const SWEEP_MS = 60_000;
const CALLBACK_PREFIX = "rc:";

// --- notification state ----------------------------------------------------

const sessionTitles = new Map<string, string>();
let activeChatId: number | null = null;
const seenChats = new Set<number>();

// Idempotency guard. Aborting the subscription signal does not reliably stop
// the SDK's async iterator, so a replaced instance can keep handling events for
// a while and every completion gets notified twice. Server events carry a
// stable `id`, so de-duplicating on it makes handling idempotent no matter how
// many iterators are attached.
const handledEventIds = new Set<string>();

function alreadyHandled(id: string): boolean {
  if (handledEventIds.has(id)) return true;
  handledEventIds.add(id);
  // Bounded FIFO trim: Set iteration order is insertion order.
  if (handledEventIds.size > 1000) {
    const oldest = handledEventIds.values().next().value;
    if (oldest !== undefined) handledEventIds.delete(oldest);
  }
  return false;
}

// --- event helpers ---------------------------------------------------------

// V2 wire format (verified against a live /api/event capture):
//   { id, created, type, location, data }
// V1 used `properties` instead of `data`, so accept both.
function props(event: any): any {
  return event?.data ?? event?.properties ?? event ?? {};
}

function sessionIdOf(p: any): string | undefined {
  const id = p?.info?.id ?? p?.sessionID ?? p?.id;
  return typeof id === "string" && id.trim() !== "" ? id : undefined;
}

function titleOf(p: any): string | undefined {
  const title = p?.info?.title ?? p?.title;
  return typeof title === "string" && title.trim() !== "" ? title : undefined;
}

function directoryOf(p: any): string | undefined {
  const directory = p?.info?.directory ?? p?.directory ?? p?.location?.directory;
  return typeof directory === "string" && directory.trim() !== "" ? directory : undefined;
}

// V1 had a dedicated `question.asked` event carrying `questions: [{header,
// question}]`. V2 removed it from the public API and routes questions through
// the permission domain instead, so accept either shape.
function questionTextsOf(p: any): string[] {
  const candidates = [p?.questions, p?.metadata?.questions, p?.metadata?.question];
  for (const value of candidates) {
    if (!Array.isArray(value) || value.length === 0) continue;
    const texts = value
      .map((item: any) =>
        typeof item === "string"
          ? item
          : `${item?.header ? `${item.header}: ` : ""}${item?.question ?? item?.text ?? item?.message ?? ""}`,
      )
      .map((text: string) => text.trim())
      .filter((text: string) => text !== "");
    if (texts.length > 0) return texts;
  }
  return [];
}

// "Task finished" signalling, established by auditing the live event stream
// (see sent.log `event-type` entries). On OpenCode v2.0.16 the only real
// completion signal is `session.execution.succeeded`:
//   - `session.idle`, shown in the official plugin docs, is never published.
//   - `session.step.ended` only ever carries finish="tool-calls", i.e. the
//     agent loop is continuing, so it must not notify.
// The V1 and terminal-finish cases are kept as harmless compatibility paths.
function isIdleEvent(type: string, p: any): boolean {
  if (type === "session.execution.succeeded") return true;
  if (type === "session.idle") return true;
  if (type === "session.status" && p?.status?.type === "idle") return true;
  if (type === "session.step.ended") {
    const finish = p?.finish ?? p?.rawFinish;
    return typeof finish === "string" && finish !== "" && !/tool[-_]?calls?/i.test(finish);
  }
  return false;
}

// --- small helpers ---------------------------------------------------------

// ASCII only: these strings end up in sent.log, which is read from terminals.
function shortId(id: string): string {
  return id.length <= 10 ? id : `${id.slice(0, 10)}..`;
}

function projectLabel(directory?: string): string {
  if (!directory) return "?";
  const parts = directory.replace(/\\/g, "/").split("/").filter(Boolean);
  return parts[parts.length - 1] ?? directory;
}

function fmtDuration(ms: number): string {
  if (ms < 1000) return "0s";
  const total = Math.floor(ms / 1000);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  if (minutes > 0) return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
  return `${seconds}s`;
}

function errorText(error: unknown): string {
  const message = (error as Error)?.message ?? String(error);
  return message.length > 300 ? `${message.slice(0, 300)}...` : message;
}

// §9: chunks over 4096 chars are split on line boundaries and numbered.
function splitForTelegram(text: string, limit = TELEGRAM_MAX): string[] {
  if (text.length <= limit) return [text];
  // Reserve room for the (n/m) prefix so numbered chunks stay within the cap.
  const effective = Math.max(Math.floor(limit / 2), limit - 12);
  const parts: string[] = [];
  let rest = text;
  while (rest.length > limit) {
    let cut = rest.lastIndexOf("\n", effective);
    // A single line longer than the cap has no boundary to cut on.
    if (cut < Math.floor(limit / 2)) cut = limit;
    parts.push(rest.slice(0, cut));
    rest = rest.slice(cut).replace(/^\n/, "");
  }
  if (rest !== "") parts.push(rest);
  const total = parts.length;
  return parts.map((part, index) => `(${index + 1}/${total}) ${part}`);
}

// Only used to honour Telegram's own retry_after; never to wait for work.
function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}

// The V2 plugin client returns bare values, but the generated HTTP client wraps
// responses in { data, error }. Accept both so one call site works either way and
// an admission rejection surfaces as a throwable with the server's own message.
function unwrap<T>(result: any, op: string): T {
  if (result && typeof result === "object") {
    const error = result.error;
    if (error) {
      const message =
        typeof error === "object"
          ? (error.message ?? JSON.stringify(error))
          : String(error);
      throw new Error(`${op}: ${message}`);
    }
    const keys = Object.keys(result);
    if ("data" in result && (keys.length <= 2 || "error" in result)) {
      return result.data as T;
    }
  }
  return result as T;
}

// --- durable state (Â§3) ----------------------------------------------------

const KEY_BINDINGS = "bindings";
const KEY_SESSIONS = "sessions";
const KEY_SETTINGS = "settings";
const KEY_TURNS = "turns";

type Mode = "notify" | "control";

interface Binding {
  sessionID: string;
  label: string;
  lastUsed: number;
  projectDir?: string;
}

interface BindingMap {
  [chatKey: string]: Binding;
}

interface SessionMeta {
  title: string;
  lastUsed: number;
  createdAt: number;
  directory?: string;
}

interface SessionMap {
  [sessionID: string]: SessionMeta;
}

interface AlertSettings {
  completed: boolean;
  failed: boolean;
  waiting: boolean;
  progress: boolean;
}

interface Settings {
  mode: Mode;
  allowExec: boolean;
  /** Inject new messages into a running turn ("steer") instead of queueing. */
  steerBusyTurns?: boolean;
  /**
   * Attach Open/Diff/Stop buttons to finished-turn alerts. Off by default to
   * keep alerts compact; buttons only work on the polling control bot.
   */
  finishedButtons?: boolean;
  alerts?: Partial<AlertSettings>;
  logBodies: boolean;
  dailyPromptCap: number;
}

interface TurnRecord {
  startedAt: number;
  chatKey: string;
  remote: boolean;
}

interface TurnMap {
  [sessionID: string]: TurnRecord;
}

interface Store {
  get<T>(key: string): Promise<T | undefined>;
  set(key: string, value: unknown): Promise<void>;
  remove(key: string): Promise<void>;
}

// Fallback used only when ctx.storage exposes no known accessor pair. It keeps
// the surface usable (and auditable) but loses state on reload, which is the
// failure mode Â§3 exists to remove - hence the loud audit line.
function memoryStore(): Store {
  const map = new Map<string, unknown>();
  return {
    async get<T>(key: string): Promise<T | undefined> {
      return map.get(key) as T | undefined;
    },
    async set(key: string, value: unknown): Promise<void> {
      map.set(key, value);
    },
    async remove(key: string): Promise<void> {
      map.delete(key);
    },
  };
}

// The exact accessor names on ctx.storage are not documented in the spec, and
// the installed @opencode-ai/plugin types still describe the V1 hook contract,
// so the accessor pair is probed once at setup and the winner is audited.
function resolveStore(ctx: any): { store: Store; kind: string } {
  const raw = ctx?.storage;
  const candidates: Array<[string, string, string]> = [
    ["get", "set", "remove"],
    ["getItem", "setItem", "removeItem"],
    ["read", "write", "delete"],
  ];
  for (const [reader, writer, remover] of candidates) {
    if (typeof raw?.[reader] !== "function" || typeof raw?.[writer] !== "function") {
      continue;
    }
    return {
      kind: `${reader}/${writer}`,
      store: {
        async get<T>(key: string): Promise<T | undefined> {
          const value = await raw[reader](key);
          return value === null ? undefined : (value as T);
        },
        async set(key: string, value: unknown): Promise<void> {
          await raw[writer](key, value);
        },
        async remove(key: string): Promise<void> {
          if (typeof raw[remover] === "function") await raw[remover](key);
        },
      },
    };
  }
  return { store: memoryStore(), kind: "memory-fallback" };
}

// --- singleton guard -------------------------------------------------------

// OpenCode invokes setup() once per location, and this machine has dozens active
// at once (projects, worktrees, alpha fleet lanes). Without a guard each
// location starts its own grammy long-poll, but Telegram permits only one
// getUpdates consumer per bot: the instances terminate each other with HTTP 409
// and every notification would be sent once per location. The event stream is
// global, so exactly one instance is correct.
const SINGLETON = Symbol.for("telegram-notify.v2.singleton");

// Bump whenever the polling/notification logic changes. On reload OpenCode does
// not tear the old instance down (reference counts churn but never reach zero),
// so without this the previous code would keep owning the poller and freshly
// deployed logic would silently never run. Â§13 makes this a per-stage ritual.
const CODE_GENERATION = 54;

interface Singleton {
  refs: number;
  generation: number;
  stop: () => void;
  /**
   * Cleared when a newer instance takes over. `controller.abort()` does not
   * terminate the ctx.event.subscribe() iterator, so a superseded instance keeps
   * receiving events; without this flag it keeps sending too, and every reload
   * would leave another ghost behind (duplicated notifications, 409 conflicts).
   */
  active: boolean;
  /** Set once, so a stale instance logs suppression exactly one time. */
  reportedStale?: boolean;
}

// The state this module instance owns, if any. Used to detect that we have been
// superseded and must stop sending.
let ownedState: Singleton | undefined;

function sharedSingleton(): Singleton | undefined {
  return (globalThis as Record<symbol, Singleton | undefined>)[SINGLETON];
}

function claimSingleton(generation: number, stop: () => void): Singleton {
  const state: Singleton = { refs: 1, generation, stop, active: true };
  (globalThis as Record<symbol, Singleton | undefined>)[SINGLETON] = state;
  return state;
}

function releaseSingleton(state: Singleton): void {
  state.refs -= 1;
  audit(`release refs=${state.refs}`);
  // A newer generation may already own the poller; a late cleanup from the
  // replaced instance must not tear that one down.
  if (sharedSingleton() !== state) return;
  if (state.refs > 0) return;
  state.active = false;
  state.stop();
  delete (globalThis as Record<symbol, Singleton | undefined>)[SINGLETON];
  console.log("[telegram-notify] stopped");
  audit("stopped");
}

// --- cross-isolate ownership lock -------------------------------------------
//
// OpenCode evaluates this plugin once per location, and each evaluation gets its
// OWN isolate (its own globalThis), so Symbol.for() cannot deduplicate them -
// every isolate believed it was the owner and every isolate sent notifications.
// The only shared medium is the filesystem, so ownership is arbitrated with a
// lock file: created exclusively ('wx'), heartbeated, and stolen via atomic
// rename + verify once the heartbeat goes stale.

const LOCK_FILE = resolve(PLUGIN_DIR, "owner.lock");
const LOCK_HEARTBEAT_MS = 5_000;
const LOCK_STALE_MS = 20_000;
const LOCK_POLL_MS = 3_000;
const OWNER_TOKEN = randomBytes(8).toString("hex");

/** True only in the single isolate allowed to poll and send. */
let crossOwner = false;
let lockHeartbeat: ReturnType<typeof setInterval> | undefined;

interface LockInfo {
  token: string;
  ts: number;
}

function readLock(): LockInfo | undefined {
  try {
    // Strip a BOM/whitespace: a lock file that exists but cannot be parsed must
    // be reported as unreadable, not as "absent".
    const raw = readFileSync(LOCK_FILE, "utf8").replace(/^\uFEFF/, "").trim();
    const parsed = JSON.parse(raw) as LockInfo;
    if (typeof parsed?.token !== "string" || typeof parsed?.ts !== "number") return undefined;
    return parsed;
  } catch {
    return undefined;
  }
}

function lockPayload(): string {
  return JSON.stringify({ token: OWNER_TOKEN, ts: Date.now(), generation: CODE_GENERATION });
}

/** Atomic exclusive create: exactly one caller can win. */
function tryCreateLock(): boolean {
  let fd: number | undefined;
  try {
    fd = openSync(LOCK_FILE, "wx");
    writeFileSync(LOCK_FILE, lockPayload());
    return true;
  } catch {
    return false;
  } finally {
    if (fd !== undefined) {
      try {
        closeSync(fd);
      } catch {
        /* ignore */
      }
    }
  }
}

/**
 * Replace a stale lock.
 *
 * rename() is atomic but check-then-act is not: every racing isolate renames,
 * then reads back its own token and concludes it won - a later rename silently
 * invalidates an earlier "winner". Measured: 5 concurrent stealers all reported
 * success. A guard file (itself acquired with exclusive create) serialises the
 * read-steal-write sequence so exactly one can win.
 */
const GUARD_FILE = `${LOCK_FILE}.guard`;
const GUARD_STALE_MS = 5_000;

function acquireGuard(): boolean {
  try {
    // A crashed stealer must not wedge the guard forever.
    try {
      if (Date.now() - statSync(GUARD_FILE).mtimeMs > GUARD_STALE_MS) {
        unlinkSync(GUARD_FILE);
      }
    } catch {
      /* no guard present, or unreadable: fall through to the exclusive create */
    }
    closeSync(openSync(GUARD_FILE, "wx"));
    return true;
  } catch {
    return false;
  }
}

function releaseGuard(): void {
  try {
    unlinkSync(GUARD_FILE);
  } catch {
    /* ignore */
  }
}

function stealLock(): boolean {
  if (!acquireGuard()) return false;
  try {
    const current = readLock();
    // Re-check under the guard: a previous winner may have just refreshed it.
    if (current !== undefined && Date.now() - current.ts <= LOCK_STALE_MS) return false;
    const staging = `${LOCK_FILE}.steal.${OWNER_TOKEN}`;
    try {
      writeFileSync(staging, lockPayload());
      renameSync(staging, LOCK_FILE);
    } catch {
      try {
        unlinkSync(staging);
      } catch {
        /* ignore */
      }
      return false;
    }
    return readLock()?.token === OWNER_TOKEN;
  } finally {
    releaseGuard();
  }
}

function heartbeat(): void {
  // Conditional write: blindly overwriting the lock lets a superseded owner
  // resurrect itself over the real owner's fresh heartbeat (split brain).
  // Only the current holder refreshes; everyone else goes through steal/create.
  try {
    if (readLock()?.token !== OWNER_TOKEN) return;
    writeFileSync(LOCK_FILE, lockPayload());
  } catch {
    /* the watcher will notice and re-elect */
  }
}

function releaseLock(): void {
  if (readLock()?.token !== OWNER_TOKEN) return;
  try {
    unlinkSync(LOCK_FILE);
    audit("lock released");
  } catch {
    /* ignore */
  }
}

const HELP_TEXT = [
  "OpenCode remote control - just type to talk to the bound session.",
  "",
  "Start here",
  "/tasks - pick a session (tap to bind)",
  "/use <number> - bind this chat",
  "/new [title] - start a fresh session here",
  "/mode on - allow remote prompts (/mode off to stop)",
  "",
  "While working",
  "/stop - interrupt the running turn",
  "/queue - waiting prompts, tap to withdraw",
  "/btw <prompt> - run now if idle, first in line if busy",
  "/perm - approve or deny waiting requests",
  "/status - what is bound, running, waiting",
  "/diff [file] - what changed",
  "/read [n] - recent replies",
  "/messages [n] - your messages, revert or fork one",
  "/attach [path] - send a project file with your next message",
  "/ls [path] - browse project files",
  "",
  "Tuning",
  "/projects - workspaces with session counts",
  "/active - sessions running right now",
  "/refresh [filter] - reload the session list",
  "/model [provider/id] - show models, or switch",
  "/agent [name] - show agents, or switch",
  "/context - tokens and cost",
  "/compact - shrink session context",
  "/commands - custom commands",
  "/skills - skills catalog",
  "/skill <name> - attach a skill",
  "/worktree - switch git worktrees",
  "/fork [msg_id] - branch the session",
  "/forms - open questions",
  "/answer <id> key=value ... - answer one",
  "/rename <title> - rename the session",
  "/forget - delete the session (asks first)",
  "/live [off] - live status card",
  "/steer on|off - inject into a running turn",
  "/buttons on|off - buttons on finished alerts",
  "/alerts <completed|failed|waiting|progress> on|off",
  "/health - server and plugin status",
  "/task in 30m|at 14:30|every 1h <prompt> - schedule",
  "/tasklist - scheduled tasks",
  "/mcps - MCP servers on/off",
  "/unbind - clear the binding",
  "/cancel - drop queue and pending buttons",
  "/whoami - your id, binding, mode",
  "",
  "Alerts: this bot covers bound sessions (compact in notify mode, full in control mode); the notify bot covers every session.",
].join("\n");

// --- plugin ----------------------------------------------------------------

export default {
  id: "local.telegram-notify",

  async setup(ctx: any) {
    const debug = (process.env.TELEGRAM_DEBUG_EVENTS ?? "") === "1";
    const seenTypes = new Set<string>();
    // Record each distinct step-end finish value once. Completion detection
    // depends on these values, so they must be auditable from sent.log rather
    // than inferred.
    const seenFinishes = new Set<string>();

    // Another location may already own the poller and subscription. Join it when it
    // runs the same code generation; otherwise it is a leftover from before a
    // reload, so stop it and take ownership of the newer logic.
    const existing = sharedSingleton();
    if (existing && existing.generation === CODE_GENERATION) {
      existing.refs += 1;
      audit(`attach refs=${existing.refs} dir=${ctx.location?.directory ?? "unknown"}`);
      return () => releaseSingleton(existing);
    }
    if (existing) {
      // Mark it dead before stopping: abort() leaves its event iterator running,
      // and this is the only thing that silences it.
      existing.active = false;
      existing.stop();
      // Deliberately NOT deleting the singleton here. Deleting first opens a
      // window in which globalThis[SINGLETON] is undefined, so any location
      // loading concurrently claims its own ownership instead of attaching -
      // that produced two concurrent pollers (409s) and doubled notifications.
      // claimSingleton() overwrites the entry, so no delete is needed.
      audit(`takeover gen=${CODE_GENERATION} (replaced gen=${existing.generation})`);
    }

    let config: PluginConfig;
    try {
      config = loadConfig();
    } catch (error) {
      console.error(`[telegram-notify] disabled: ${(error as Error).message}`);
      return;
    }

    const bot = new Bot(config.token);
// Second, send-only bot for notifications. Never polled, so it can never
// contend for getUpdates, and it has no path to executing anything.
const notifyBot = config.notifyToken === undefined ? undefined : new Bot(config.notifyToken);
if (notifyBot !== undefined) {
  console.log("[telegram-notify] separate notification bot configured (send-only)");
  // Fail loudly at startup rather than silently degrading every notification
  // into a control-bot fallback.
  void notifyBot.api.getMe().then(
    (me) => audit(`notify-bot ok @${me.username} id=${me.id}`),
    (error: unknown) =>
      audit(`notify-bot FAILED error=${JSON.stringify(String((error as Error)?.message ?? error))}`),
  );
}
    if (config.chatId) activeChatId = config.chatId;

    // --- state -------------------------------------------------------------

    const { store, kind: storeKind } = resolveStore(ctx);
    let bindings: BindingMap = {};
    let sessions: SessionMap = {};
    let settings: Settings = {
      mode: "notify",
      allowExec: false,
      steerBusyTurns: true,
      finishedButtons: false,
      alerts: { completed: true, failed: true, waiting: true, progress: true },
      logBodies: false,
      dailyPromptCap: config.dailyPromptCap,
    };
    // Turn records are durable so a reload can tell the user a remote turn is
    // orphaned instead of pretending it finished (Â§9).
    let turnRecords: TurnMap = {};

    async function persist(key: string, value: unknown): Promise<void> {
      try {
        await store.set(key, value);
      } catch (error) {
        audit(`err op=storage key=${key} error=${JSON.stringify(errorText(error))}`);
      }
    }

    async function loadState(): Promise<void> {
      try {
        bindings = (await store.get<BindingMap>(KEY_BINDINGS)) ?? {};
        sessions = (await store.get<SessionMap>(KEY_SESSIONS)) ?? {};
        const stored = await store.get<Partial<Settings>>(KEY_SETTINGS);
        settings = { ...settings, ...(stored ?? {}) };
        turnRecords = (await store.get<TurnMap>(KEY_TURNS)) ?? {};
        const savedQueues = (await store.get<Record<string, QueuedPrompt[]>>(KEY_QUEUE)) ?? {};
        let restored = 0;
        for (const [sessionID, queue] of Object.entries(savedQueues)) {
          if (Array.isArray(queue) && queue.length > 0) {
            promptQueues.set(sessionID, queue.slice(0, QUEUE_CAP));
            restored += queue.length;
          }
        }
        if (restored > 0) audit(`queue-restored prompts=${restored}`);
        scheduledTasks = (await store.get<Record<string, ScheduledTask>>(KEY_SCHED)) ?? {};
        // A reload orphaned any in-flight run markers; the tick clears them.
      } catch (error) {
        audit(`err op=storage key=load error=${JSON.stringify(errorText(error))}`);
      }
    }

    await loadState();
    // PIN support was fully removed: delete any stored auth record so an old
    // hash does not linger in storage.
    try {
      await store.remove("auth");
    } catch {
      /* best effort */
    }
    audit(
      `state mode=${settings.mode} allowExec=${settings.allowExec} ` +
        `store=${storeKind}`,
    );

    function alertEnabled(kind: keyof AlertSettings): boolean {
      const defaults: AlertSettings = { completed: true, failed: true, waiting: true, progress: true };
      return { ...defaults, ...(settings.alerts ?? {}) }[kind];
    }

    // --- turn state (Â§6) ---------------------------------------------------

    interface LiveTurn {
      sessionID: string;
      chatKey: string;
      chatId: number;
      startedAt: number;
      heartbeats: number;
      ackMessageId?: number;
      timer?: ReturnType<typeof setInterval>;
    }

    interface QueuedPrompt {
      id: string;
      text: string;
      chatKey: string;
      chatId: number;
    }

    interface PendingPermission {
      token: string;
      sessionID: string;
      permissionID: string;
      chatKey: string;
      chatId: number;
      title: string;
      createdAt: number;
      expiresAt: number;
    }

    interface PendingAction {
      token: string;
      kind:
        | "open"
        | "diff"
        | "stop"
        | "bind"
        | "allow-once"
        | "always"
        | "deny"
        | "model"
        | "agent"
        | "run-command"
        | "run-skill"
        | "forget-confirm"
        | "forget-cancel"
        | "ls-open"
        | "task-delete"
        | "msg-revert"
        | "msg-revert-confirm"
        | "msg-revert-cancel"
        | "msg-fork"
        | "queue-withdraw"
        | "mcp-toggle";
      chatKey: string;
      chatId: number;
      sessionID: string;
      permissionToken?: string;
      /** Free-form payload for actions that need more than a session id. */
      data?: string;
      createdAt: number;
    }

    const liveTurns = new Map<string, LiveTurn>();
    // FIFO queue per session, bounded so one chat cannot flood it. Each entry
    // has an id so individual prompts can be withdrawn from /queue. Persisted
    // so a reload does not silently drop waiting prompts.
    const QUEUE_CAP = 5;
    const KEY_QUEUE = "promptQueues";
    const promptQueues = new Map<string, QueuedPrompt[]>();

    async function persistQueues(): Promise<void> {
      const snapshot: Record<string, QueuedPrompt[]> = {};
      for (const [sessionID, queue] of promptQueues) snapshot[sessionID] = queue;
      await persist(KEY_QUEUE, snapshot);
    }
    const pendingPermissions = new Map<string, PendingPermission>();
    // --- Tier 3: file attachments -------------------------------------------
// Files staged per chat and attached to the NEXT prompt (Session prompt body
// accepts files:[{uri,name}]). Telegram payloads are downloaded to a temp dir;
// nothing is written into the user's project.
const stagedFiles = new Map<string, Array<{ uri: string; name: string; at: number }>>();

function attachmentDir(): string {
  const dir = join(tmpdir(), "opencode-telegram-attach");
  mkdirSync(dir, { recursive: true });
  return dir;
}

// The cached OpenAPI schema accepts file URIs. Windows absolute paths need the
// three-slash form (file:///C:/...); POSIX absolute paths already contain the
// third slash after file://.
function toFileUri(path: string): string {
  const normalized = path.replace(/\\/g, "/");
  if (/^[A-Za-z]:\//.test(normalized)) return `file:///${normalized}`;
  if (normalized.startsWith("/")) return `file://${normalized}`;
  return `file:///${normalized}`;
}

// Delete a downloaded temp copy. Project files staged via /attach point into
// the user's project and must never be touched - only paths under our own
// temp dir are removed.
function deleteTempCopy(uri: string): void {
  const local = uri.startsWith("file://") ? uri.slice("file://".length) : uri;
  const normalized = local.replace(/\//g, sep);
  if (!normalized.startsWith(attachmentDir() + sep)) return;
  try {
    unlinkSync(normalized);
  } catch {
    /* best effort */
  }
}

function stageFiles(chatKey: string, files: Array<{ uri: string; name: string }>): void {
  const now = Date.now();
  const merged = [...(stagedFiles.get(chatKey) ?? []), ...files.map((f) => ({ ...f, at: now }))];
  // Bound the batch so one message cannot flood the prompt.
  stagedFiles.set(chatKey, merged.slice(-10));
}

function takeStagedFiles(chatKey: string): Array<{ uri: string; name: string }> {
  const files = stagedFiles.get(chatKey) ?? [];
  stagedFiles.delete(chatKey);
  for (const file of files) deleteTempCopy(file.uri);
  return files.map(({ uri, name }) => ({ uri, name }));
}

const pendingActions = new Map<string, PendingAction>();
    // The /ls handler lives inside the command router's scope, but the callback
    // handler is a sibling, so publish it here for ls-open buttons to reach.
    let lsNav:
      | ((chatId: number, userId: number, chatKey: string, path: string) => Promise<void>)
      | undefined;
    // Same story for the session-directory lookup used by /tasks, /ls and
    // /attach: defined alongside the router, so publish it at setup scope.
    let sessionRoot: ((sessionID: string) => Promise<string | undefined>) | undefined;
    const sessionChains = new Map<string, Promise<void>>();
    const lastListings = new Map<string, string[]>();
    const dailyCounters = new Map<string, { day: string; count: number }>();
    const blockedChats = new Set<number>();
    // An explicit /stop makes the server emit a completion event afterwards;
    // remember it briefly so the user does not also get a "Finished" notice.
    const interruptedAt = new Map<string, number>();

    function chatKeyOf(chatId: number, userId: number): string {
      // Â§14.7 key by chat+user so a second whitelist entry cannot inherit state.
      return `${chatId}:${userId}`;
    }

    // Â§6 rule 6: serialize per session so a fast turn cannot interleave with the
    // next one. Used by the event stream; command handling mutates the same maps
    // but is already serialized by grammy per chat.
    function enqueueSession<T>(sessionID: string, work: () => Promise<T>): Promise<T> {
      const previous = sessionChains.get(sessionID) ?? Promise.resolve();
      const next = previous.then(work, work);
      sessionChains.set(
        sessionID,
        next.then(
          () => {},
          () => {},
        ),
      );
      return next;
    }

    // --- sending -----------------------------------------------------------

    type SendResult = { ok: boolean; messageId?: number };

    // Messages a message can only be edited by the bot that sent it, so remember
// which of the two owns each notify-bot message. Bounded: only recent edits
// matter.
const notifyOwned = new Set<string>();
let lastNonOwnerAudit = 0;
function msgKey(chatId: number, messageId: number): string {
  return `${chatId}:${messageId}`;
}
function markNotifyOwned(chatId: number, messageId: number): void {
  notifyOwned.add(msgKey(chatId, messageId));
  if (notifyOwned.size > 200) {
    const oldest = notifyOwned.values().next().value;
    if (oldest !== undefined) notifyOwned.delete(oldest);
  }
}

async function sendNow(
      chatId: number,
      text: string,
      // grammy types reply_markup strictly; keyboards are built above and are
      // always InlineKeyboardMarkup, so any keeps the call site readable.
      keyboard?: any,
      // "notify" = unsolicited alert, must only ever leave via the notify bot.
      // "control" = a reply to something the owner typed, so it must stay on the
      // bot they are talking to.
      route: "control" | "notify" = "control",
    ): Promise<SendResult> {
      if (blockedChats.has(chatId)) return { ok: false };
      // A superseded instance still receives events (abort does not close the
      // iterator). It must not send: that is what produced duplicate
      // notifications and competing getUpdates requests.
      if (ownedState !== undefined && ownedState.active === false) {
        if (!ownedState.reportedStale) {
          ownedState.reportedStale = true;
          audit(`stale-instance suppressed gen=${CODE_GENERATION} dir=${ctx.location?.directory ?? "?"}`);
        }
        return { ok: false };
      }
      // Only the lock holder sends. Every isolate receives events, so without
      // this gate each one would deliver its own copy of every notification.
      // Drops are audited (rate-limited): silent drops once hid a dead owner
      // for hours.
      if (!crossOwner) {
        const now = Date.now();
        if (now - lastNonOwnerAudit > 60_000) {
          lastNonOwnerAudit = now;
          audit(`send-suppressed-not-owner gen=${CODE_GENERATION} dir=${ctx.location?.directory ?? "?"}`);
        }
        return { ok: false };
      }
      // Only the control bot polls, so callback_query (permission buttons) can
      // only ever reach it: anything with a keyboard must go out on `bot`.
      const viaNotify = notifyBot !== undefined && route === "notify" && keyboard === undefined;
      for (let attempt = 0; attempt < 3; attempt += 1) {
        const useNotify = viaNotify;
        const api = useNotify ? notifyBot!.api : bot.api;
        try {
          const sent = await api.sendMessage(
            chatId,
            text,
            keyboard ? { reply_markup: keyboard } : undefined,
          );
          if (useNotify) markNotifyOwned(chatId, sent.message_id);
          audit(
            `sent chat=${chatId} msg=${sent.message_id} via=${useNotify ? "notify" : "control"}` +
              ` text=${JSON.stringify(text.slice(0, 120))}`,
          );
          return { ok: true, messageId: sent.message_id };
        } catch (error) {
          const failure = error as {
            error_code?: number;
            parameters?: { retry_after?: number };
            message?: string;
          };
          if (useNotify && failure?.error_code !== 429) {
            // Alerts must never fall back to the control bot: the whole point
            // of the split is that @opencode89bot stays silent. Drop and audit
            // so a broken notify bot is visible instead of silently diverting.
            // 429 is excluded: that is transient and handled by the backoff
            // branch below, still against the notify bot.
            audit(`notify-FAILED chat=${chatId} dropped error=${failure?.message ?? String(error)}`);
            return { ok: false };
          }
          if (failure?.error_code === 403) {
            blockedChats.add(chatId);
            audit(`FAILED chat=${chatId} error=${failure.message ?? "forbidden"}`);
            audit(`err op=send chat=${chatId} error="bot blocked by user, sending suspended"`);
            console.warn(
              `[telegram-notify] chat ${chatId} blocked the bot; sending suspended for this chat`,
            );
            return { ok: false };
          }
          if (failure?.error_code === 429 && attempt < 2) {
            const wait = Math.min((failure.parameters?.retry_after ?? 5) * 1000, 15_000);
            audit(`err op=send chat=${chatId} error="429 rate limited, backing off ${wait}ms"`);
            await delay(wait);
            continue;
          }
          console.error("[telegram-notify] send failed:", error);
          audit(`FAILED chat=${chatId} error=${failure?.message ?? String(error)}`);
          return { ok: false };
        }
      }
      return { ok: false };
    }

    // Â§9: Telegram 429 handling requires a bounded serial queue rather than
    // firing every message at once. Depth is capped and overflow is audited.
    const sendQueues = new Map<
      number,
      Array<{
        text: string;
        keyboard?: any;
        route: "control" | "notify";
        resolve: (r: SendResult) => void;
      }>
    >();
    const sending = new Set<number>();

    function sendToChat(
      chatId: number,
      text: string,
      keyboard?: any,
      route: "control" | "notify" = "control",
    ): Promise<SendResult> {
      return new Promise<SendResult>((resolveItem) => {
        const queue = sendQueues.get(chatId) ?? [];
        sendQueues.set(chatId, queue);
        if (queue.length >= SEND_QUEUE_CAP) {
          audit(`err op=send chat=${chatId} error="send queue overflow, message dropped"`);
          resolveItem({ ok: false });
          return;
        }
        queue.push({ text, keyboard, route, resolve: resolveItem });
        void pump(chatId);
      });
    }

    async function pump(chatId: number): Promise<void> {
      if (sending.has(chatId)) return;
      sending.add(chatId);
      try {
        for (;;) {
          const queue = sendQueues.get(chatId);
          const item = queue?.shift();
          if (!item) {
            sendQueues.delete(chatId);
            return;
          }
          let last: SendResult = { ok: false };
          const parts = splitForTelegram(item.text);
          for (let index = 0; index < parts.length; index += 1) {
            // Buttons belong on the final chunk only; repeating them would
            // create duplicate tappable actions for one logical message.
            const keyboard = index === parts.length - 1 ? item.keyboard : undefined;
            last = await sendNow(chatId, parts[index]!, keyboard, item.route);
            if (!last.ok) break;
          }
          item.resolve(last);
        }
      } finally {
        sending.delete(chatId);
      }
    }

    async function replyTo(chatId: number, text: string, keyboard?: any): Promise<void> {
      await sendToChat(chatId, text, keyboard);
    }

    async function notify(text: string, keyboard?: any): Promise<void> {
      if (activeChatId === null) {
        console.warn("[telegram-notify] no active chat yet - message dropped");
        audit(`dropped no-active-chat text=${JSON.stringify(text.slice(0, 80))}`);
        return;
      }
      await sendToChat(activeChatId, text, keyboard, "notify");
    }

    async function editSent(chatId: number, messageId: number, text: string): Promise<void> {
      try {
        // Telegram only lets the owning bot edit its own message.
        const owner = notifyOwned.has(msgKey(chatId, messageId)) ? notifyBot : bot;
        await owner!.api.editMessageText(chatId, messageId, text);
      } catch (error) {
        audit(`err op=edit chat=${chatId} msg=${messageId} error=${JSON.stringify(errorText(error))}`);
      }
    }

    // --- buttons (Â§5) ------------------------------------------------------

    function newToken(): string {
      return randomBytes(8).toString("hex");
    }

    function registerAction(
      action: Omit<PendingAction, "token" | "createdAt">,
    ): PendingAction {
      const token = newToken();
      const full: PendingAction = { ...action, token, createdAt: Date.now() };
      pendingActions.set(token, full);
      return full;
    }

    function button(text: string, action: Omit<PendingAction, "token" | "createdAt">): {
      text: string;
      callback_data: string;
    } {
      const full = registerAction(action);
      return { text, callback_data: `${CALLBACK_PREFIX}${full.token}` };
    }

    // --- bindings (Â§3 routing rule, Â§8 idle TTL) ---------------------------

    function bindingFor(chatKey: string): Binding | undefined {
      const binding = bindings[chatKey];
      if (!binding) return undefined;
      if (Date.now() - binding.lastUsed > config.bindingTtlMs) {
        // §8 idle TTL: an unattended phone must not still be able to act.
        delete bindings[chatKey];
        void persist(KEY_BINDINGS, bindings);
        audit(`cmd cmd=binding-expired session=${binding.sessionID} key=${chatKey}`);
        return undefined;
      }
      return binding;
    }

    // Chats currently bound to a session, derived from the chatKey format
    // `${chatId}:${userId}`. Completion alerts go only to these chats so the
    // phone is not noised by sessions nobody selected.
    function boundChatIds(sessionID: string): number[] {
      const ids: number[] = [];
      for (const [chatKey, binding] of Object.entries(bindings)) {
        if (binding.sessionID !== sessionID) continue;
        if (Date.now() - binding.lastUsed > config.bindingTtlMs) continue;
        const chatId = Number.parseInt(chatKey.split(":")[0] ?? "", 10);
        if (!Number.isNaN(chatId) && !ids.includes(chatId)) ids.push(chatId);
      }
      return ids;
    }

    async function setBinding(
      chatKey: string,
      sessionID: string,
      label: string,
      projectDir?: string,
    ): Promise<void> {
      bindings[chatKey] = {
        sessionID,
        label,
        lastUsed: Date.now(),
        ...(projectDir ? { projectDir } : {}),
      };
      await persist(KEY_BINDINGS, bindings);
    }

    async function clearBinding(chatKey: string): Promise<void> {
      delete bindings[chatKey];
      await persist(KEY_BINDINGS, bindings);
    }

    function rememberSession(
      sessionID: string,
      title: string,
      directory?: string,
      createdAt?: number,
    ): void {
      const previous = sessions[sessionID];
      const changed =
        previous === undefined ||
        previous.title !== title ||
        previous.directory !== directory;
      sessions[sessionID] = {
        title,
        directory: directory ?? previous?.directory,
        createdAt: createdAt ?? previous?.createdAt ?? Date.now(),
        lastUsed: Date.now(),
      };
      sessionTitles.set(sessionID, title);
      // Session metadata arrives on nearly every event; persisting the whole map
      // each time would turn a chatty session into a disk write storm.
      if (changed || Date.now() - (previous?.lastUsed ?? 0) > 60_000) {
        void persist(KEY_SESSIONS, sessions);
      }
    }

    // --- auth --------------------------------------------------------------
    // PIN support was fully removed (owner request): no second factor exists.
    // Remaining gates are the Telegram allowlist, private-chat restriction,
    // and control-mode switch. Stored PIN secrets, if any, are purged below.


    // --- session API shims --------------------------------------------------

    async function callSessions<T>(op: string, args: Record<string, unknown>): Promise<T> {
      const fn = ctx.session?.[op];
      if (typeof fn !== "function") throw new Error(`session.${op} unavailable`);
      return unwrap<T>(await fn.call(ctx.session, args), `session.${op}`);
    }

    // session.message.list is the documented name; the HTTP surface calls the
    // same endpoint /session/{id}/message, so accept the bare method too.
    async function callMessageList(sessionID: string, limit: number): Promise<any[]> {
      const session: any = ctx.session;
      const fn =
        typeof session?.message?.list === "function"
          ? session.message.list.bind(session.message)
          : typeof session?.message === "function"
            ? session.message.bind(session)
            : undefined;
      if (fn) {
        const result = unwrap<any>(await fn({ sessionID, limit }), "session.message.list");
        return Array.isArray(result) ? result : [];
      }
      // This build has no message accessor in context; fall back to HTTP.
      const body: any = await apiGet(
        `/api/session/${sessionID}/message?limit=${Math.max(limit, 10)}&order=desc`,
      );
      const list = Array.isArray(body) ? body : Array.isArray(body?.data) ? body.data : [];
      audit(`messages-http session=${sessionID} count=${list.length}`);
      return list;
    }

    async function callPermission(
      kind: "list" | "reply",
      args: Record<string, unknown>,
    ): Promise<any> {
      const scoped: any = ctx.session?.permission;
      const root: any = ctx.permission;
      const fn =
        typeof scoped?.[kind] === "function"
          ? scoped[kind].bind(scoped)
          : typeof root?.[kind] === "function"
            ? root[kind].bind(root)
            : undefined;
      if (!fn) throw new Error(`session.permission.${kind} unavailable`);
      return unwrap<any>(await fn(args), `permission.${kind}`);
    }

    async function fetchSession(sessionID: string): Promise<any | undefined> {
      try {
        return await callSessions<any>("get", { sessionID });
      } catch {
        // Â§9: unauthorized for this location must not leak, and a missing
        // session should clear the binding rather than spin.
        return undefined;
      }
    }

    // --- session listing (Â§4 /tasks, decision Â§14.4) -----------------------

    interface TaskRow {
      id: string;
      title: string;
      directory?: string;
      projectID?: string;
      project?: string;
      updated: number;
      created: number;
    }

    // /tasks query language: free-text filter plus explicit sort/project tokens.
    // Examples: `/tasks`, `/tasks sort:new`, `/tasks project:shop`, `/tasks foo sort:new`.
    function parseTaskQuery(raw: string): { filter: string; sort: "updated" | "created"; project: string } {
      let sort: "updated" | "created" = "updated";
      let project = "";
      const filters: string[] = [];
      for (const token of raw.trim().split(/\s+/).filter((part) => part !== "")) {
        const sortMatch = /^sort:(new|newest|created|updated)$/i.exec(token);
        if (sortMatch) {
          sort = /^(new|newest|created)$/i.test(sortMatch[1]!) ? "created" : "updated";
          continue;
        }
        const projectMatch = /^project:(.+)$/i.exec(token);
        if (projectMatch) {
          project = projectMatch[1]!.trim();
          continue;
        }
        filters.push(token);
      }
      return { filter: filters.join(" "), sort, project };
    }

    // session.list does NOT include titles. Verified against the live server:
    // list items expose id, projectID, model, cost, tokens, time and location,
    // while session.get is what carries `title`. Fetching a title per session
    // for all ~50 sessions would be wasteful, so titles are fetched only for the
    // rows actually displayed and cached for the process lifetime.
    const titleCache = new Map<string, string>();

    async function hydrateTitles(rows: TaskRow[]): Promise<void> {
      const CONCURRENCY = 5;
      const pending = rows.filter((row) => !titleCache.has(row.id));
      for (let i = 0; i < pending.length; i += CONCURRENCY) {
        await Promise.all(
          pending.slice(i, i + CONCURRENCY).map(async (row) => {
            try {
              const info = await callSessions<any>("get", { sessionID: row.id });
              const title = typeof info?.title === "string" ? info.title.trim() : "";
              if (title !== "") titleCache.set(row.id, title);
            } catch {
              // Leave uncached; the row renders as "(untitled)".
            }
          }),
        );
      }
      for (const row of rows) {
        const cached = titleCache.get(row.id);
        if (cached !== undefined) row.title = cached;
      }
    }

    // ctx.session has no list() on this build, so /tasks goes over the local HTTP
    // API. Two wrinkles: the server password lives in service.json, and the
    // ephemeral port is not exposed anywhere except `opencode pair`, so the URL
    // is discovered once and then cached on disk.
    const urlCacheFile = resolve(PLUGIN_DIR, "opencode-url.txt");

    async function serverBaseUrl(forceRediscovery = false): Promise<string> {
      const cached =
        !forceRediscovery && existsSync(urlCacheFile)
          ? readFileSafe(urlCacheFile).trim().replace(/\/+$/, "")
          : "";
      if (cached !== "") return cached;
      const { stdout } = await execAsync("opencode pair", {
        timeout: 30_000,
        maxBuffer: 4 * 1024 * 1024,
        windowsHide: true,
      });
      const match = /^\s*URLs\s+((?:https?:\/\/\S+\s*)+)$/m.exec(stdout);
      const url = match?.[1]?.trim().split(/\s+/)[0] ?? "";
      if (!/^https?:\/\//.test(url)) throw new Error("could not read server URL from `opencode pair`");
      try {
        writeFileSync(urlCacheFile, url, "utf8");
        audit(`server-url ${forceRediscovery ? "rediscovered" : "discovered"} ${url}`);
      } catch {
        /* cache is an optimisation, not a requirement */
      }
      return url.replace(/\/+$/, "");
    }

    function readFileSafe(path: string): string {
      try {
        return readFileSync(path, "utf8");
      } catch {
        return "";
      }
    }

    // Shared local-API auth. The password lives in service.json; the user is
    // always `opencode` for the background service.
    async function serverAuthHeader(): Promise<string> {
      const password = (() => {
        try {
          return JSON.parse(
            readFileSafe(join(homedir(), ".config", "opencode", "service.json")),
          ).password as unknown;
        } catch {
          return undefined;
        }
      })();
      if (typeof password !== "string" || password === "") {
        throw new Error("no server password in service.json");
      }
      return `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}`;
    }

    // GET against the local server with the same stale-URL self-heal used for
    // sessions: retry once against a freshly discovered base URL.
    async function apiGet(path: string): Promise<any> {
      const authorization = await serverAuthHeader();
      for (let attempt = 0; attempt < 2; attempt += 1) {
        const base = await serverBaseUrl(attempt === 1);
        try {
          const res = await fetch(`${base}${path}`, {
            headers: { authorization },
            signal: AbortSignal.timeout(20_000),
          });
          if (!res.ok) throw new Error(`${path} returned HTTP ${res.status}`);
          return await res.json();
        } catch (error) {
          audit(
            `api-FAILED attempt=${attempt} path=${path} base=${base}` +
              ` error=${JSON.stringify(errorText(error))}`,
          );
          if (attempt === 1) throw error;
        }
      }
      throw new Error(`${path} unavailable`);
    }

    // Global list: every session on this server, with titles already hydrated
    // (Session.Info carries `title`), which avoids an N x session.get fan-out.
    async function listSessionsGlobal(): Promise<any[]> {
      const body: any = await apiGet("/api/session");
      if (!Array.isArray(body?.data)) throw new Error("unexpected session list response");
      audit(`sessions-http count=${body.data.length}`);
      return body.data;
    }

    // POST against the local server with the same stale-URL self-heal.
    async function apiPost(path: string, body: unknown): Promise<any> {
      const authorization = await serverAuthHeader();
      for (let attempt = 0; attempt < 2; attempt += 1) {
        const base = await serverBaseUrl(attempt === 1);
        try {
          const res = await fetch(`${base}${path}`, {
            method: "POST",
            headers: { authorization, "content-type": "application/json" },
            body: JSON.stringify(body ?? {}),
            signal: AbortSignal.timeout(20_000),
          });
          if (!res.ok) throw new Error(`${path} returned HTTP ${res.status}`);
          if (res.status === 204) return undefined;
          return await res.json();
        } catch (error) {
          audit(
            `api-FAILED attempt=${attempt} path=${path} base=${base}` +
              ` error=${JSON.stringify(errorText(error))}`,
          );
          if (attempt === 1) throw error;
        }
      }
      throw new Error(`${path} unavailable`);
    }

    // Runtime-supported project list: GET /api/project returns Project[] with
    // id, name/canonical directory, and time metadata.
    async function listProjectsGlobal(): Promise<any[]> {
      const body: any = await apiGet("/api/project");
      const list = Array.isArray(body) ? body : Array.isArray(body?.data) ? body.data : [];
      audit(`projects-http count=${list.length}`);
      return list;
    }

    // Runtime-supported live-session map: GET /api/session/active returns
    // {data: {ses_...: {type: "running"}}}.
    async function listActiveSessions(): Promise<Set<string>> {
      const body: any = await apiGet("/api/session/active");
      const data = body?.data;
      const ids = data && typeof data === "object" ? Object.keys(data) : [];
      audit(`sessions-active count=${ids.length}`);
      return new Set(ids);
    }

    // Fallback when the HTTP route is unavailable: the CLI resolves the server
    // itself but only sees the current project.
    async function listSessionsViaCli(cwd: string): Promise<any[]> {
      const { stdout } = await execAsync("opencode session list --format json", {
        cwd,
        timeout: 30_000,
        maxBuffer: 8 * 1024 * 1024,
        windowsHide: true,
      });
      const parsed = JSON.parse(stdout);
      const list = Array.isArray(parsed) ? parsed : Array.isArray(parsed?.data) ? parsed.data : [];
      audit(`sessions-cli cwd=${cwd} count=${list.length}`);
      return list;
    }

    async function listTasks(chatKey: string, rawQuery: string): Promise<TaskRow[]> {
      const binding = bindingFor(chatKey);
      const cwd =
        (binding === undefined || sessionRoot === undefined
          ? undefined
          : await sessionRoot(binding.sessionID)) ?? process.cwd();
      const query = parseTaskQuery(rawQuery);
      let list: any[];
      let projects: any[] = [];
      try {
        // Sessions and projects are independent reads; projects are best-effort
        // because directory labels still identify the workspace without them.
        const [sessions, projectList] = await Promise.all([
          listSessionsGlobal(),
          listProjectsGlobal().catch((error: unknown) => {
            audit(`projects-http-FAILED error=${JSON.stringify(errorText(error))}`);
            return [] as any[];
          }),
        ]);
        list = sessions;
        projects = projectList;
      } catch (error) {
        audit(`sessions-http-FAILED error=${JSON.stringify(errorText(error))}`);
        list = await listSessionsViaCli(cwd);
      }
      const projectById = new Map<string, any>();
      for (const project of projects) {
        if (project && typeof project.id === "string") projectById.set(project.id, project);
      }
      const rows: TaskRow[] = list
        .filter((item: any) => item && typeof item.id === "string")
        .map((item: any) => {
          const id = String(item.id);
          const directory =
            typeof item?.directory === "string"
              ? item.directory
              : typeof item?.location?.directory === "string"
                ? item.location.directory
                : undefined;
          const projectID = typeof item?.projectID === "string" ? item.projectID : undefined;
          const project = projectID !== undefined ? projectById.get(projectID) : undefined;
          const projectName =
            typeof project?.name === "string" && project.name.trim() !== ""
              ? project.name
              : typeof project?.canonical === "string" && project.canonical.trim() !== ""
                ? projectLabel(project.canonical)
                : projectLabel(directory);
          return {
            id,
            // The CLI already returns titles; the cache only covers older shapes.
            title: String(item.title ?? titleCache.get(id) ?? "(untitled)"),
            // CLI returns a flat `directory`; the HTTP API nests location.directory.
            directory,
            projectID,
            project: projectName,
            updated: Number(item?.time?.updated ?? item?.time?.created ?? 0),
            created: Number(item?.time?.created ?? item?.time?.updated ?? 0),
          };
        })
        .sort((a, b) =>
          query.sort === "created" ? b.created - a.created : b.updated - a.updated,
        )
        .slice(0, LIST_LIMIT);

      // Titles must be resolved before filtering, otherwise a title filter
      // silently matches nothing.
      await hydrateTitles(rows);

      const needle = query.filter.toLowerCase();
      const projectNeedle = query.project.toLowerCase();
      return rows.filter((row) => {
        if (projectNeedle !== "") {
          const haystack = `${row.project ?? ""} ${row.projectID ?? ""} ${projectLabel(row.directory)}`.toLowerCase();
          if (!haystack.includes(projectNeedle)) return false;
        }
        if (needle === "") return true;
        return (
          row.title.toLowerCase().includes(needle) ||
          row.id.includes(needle) ||
          projectLabel(row.directory).toLowerCase().includes(needle) ||
          (row.project ?? "").toLowerCase().includes(needle)
        );
      });
    }

    async function handleTasks(
      chatId: number,
      userId: number,
      chatKey: string,
      filter: string,
    ): Promise<void> {
      const query = parseTaskQuery(filter);
      audit(
        `cmd chat=${chatId} user=${userId} cmd=/tasks filter=${query.filter === "" ? "<none>" : query.filter}` +
          ` sort=${query.sort}${query.project === "" ? "" : ` project=${query.project}`}`,
      );
      let rows: TaskRow[];
      try {
        rows = await listTasks(chatKey, filter);
      } catch (error) {
        audit(`err op=session.list error=${JSON.stringify(errorText(error))}`);
        await replyTo(chatId, "Could not list sessions from here.");
        return;
      }
      lastListings.set(chatKey, rows.map((row) => row.id));
      if (rows.length === 0) {
        await replyTo(chatId, filter === "" ? "No sessions found." : `No session matches "${filter}".`);
        return;
      }
      const bound = bindingFor(chatKey);
      // Group by workspace while preserving the requested timeline order.
      const groups = new Map<string, TaskRow[]>();
      for (const row of rows.slice(0, 20)) {
        const key = row.project ?? projectLabel(row.directory);
        const group = groups.get(key) ?? [];
        group.push(row);
        groups.set(key, group);
      }
      const lines: string[] = [];
      let index = 0;
      for (const [project, group] of groups) {
        lines.push(`[${project}]`);
        for (const row of group) {
          index += 1;
          const mark = bound?.sessionID === row.id ? "* " : `${index}. `;
          lines.push(`${mark}${shortId(row.id)}  ${row.title.slice(0, 60)}`);
        }
      }
      const keyboardRows = rows.slice(0, 5).map((row) => [
        button(`Use ${row.title.slice(0, 24) || shortId(row.id)}`, {
          kind: "bind",
          chatKey,
          chatId,
          sessionID: row.id,
        }),
      ]);
      await replyTo(
        chatId,
        `Sessions${filter === "" ? "" : ` matching "${filter}"`} (sort: ${query.sort}):\n${lines.join("\n")}\n\n` +
          "Bind with /use <number>, /use <id>, or the buttons below.\n" +
          "Try `/tasks sort:new`, `/tasks project:<name>`, or /projects.",
        { inline_keyboard: keyboardRows },
      );
    }

    async function handleProjects(
      chatId: number,
      userId: number,
      chatKey: string,
      filter: string,
    ): Promise<void> {
      const needle = filter.trim().toLowerCase();
      audit(`cmd chat=${chatId} user=${userId} cmd=/projects filter=${needle === "" ? "<none>" : needle}`);
      let projects: any[];
      let sessions: any[];
      try {
        [projects, sessions] = await Promise.all([listProjectsGlobal(), listSessionsGlobal()]);
      } catch (error) {
        audit(`err op=project.list error=${JSON.stringify(errorText(error))}`);
        await replyTo(chatId, "Could not list projects from here.");
        return;
      }
      const counts = new Map<string, number>();
      for (const session of sessions) {
        if (session && typeof session.projectID === "string") {
          counts.set(session.projectID, (counts.get(session.projectID) ?? 0) + 1);
        }
      }
      const rows = projects
        .filter((project: any) => project && typeof project.id === "string")
        .map((project: any) => ({
          id: String(project.id),
          name:
            typeof project.name === "string" && project.name.trim() !== ""
              ? project.name
              : projectLabel(typeof project.canonical === "string" ? project.canonical : undefined),
          directory: typeof project.canonical === "string" ? project.canonical : undefined,
          sessions: counts.get(String(project.id)) ?? 0,
        }))
        .filter(
          (row) =>
            needle === "" ||
            row.name.toLowerCase().includes(needle) ||
            row.id.toLowerCase().includes(needle) ||
            projectLabel(row.directory).toLowerCase().includes(needle),
        )
        .sort((a, b) => b.sessions - a.sessions || a.name.localeCompare(b.name))
        .slice(0, 20);
      if (rows.length === 0) {
        await replyTo(chatId, needle === "" ? "No projects found." : `No project matches "${filter}".`);
        return;
      }
      await replyTo(
        chatId,
        `Projects:\n${rows
          .map((row, index) => `${index + 1}. ${row.name}  [${projectLabel(row.directory)}]  ${row.sessions} session${row.sessions === 1 ? "" : "s"}`)
          .join("\n")}\n\nUse /tasks project:<name> to list that workspace.`,
      );
    }

    async function handleActive(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/active`);
      let active: Set<string>;
      let sessions: any[];
      try {
        [active, sessions] = await Promise.all([listActiveSessions(), listSessionsGlobal()]);
      } catch (error) {
        audit(`err op=session.active error=${JSON.stringify(errorText(error))}`);
        await replyTo(chatId, "Could not list live sessions from here.");
        return;
      }
      const rows = sessions
        .filter((item: any) => item && typeof item.id === "string" && active.has(String(item.id)))
        .map((item: any) => ({
          id: String(item.id),
          title: String(item.title ?? titleCache.get(String(item.id)) ?? "(untitled)"),
          directory:
            typeof item?.directory === "string"
              ? item.directory
              : typeof item?.location?.directory === "string"
                ? item.location.directory
                : undefined,
        }))
        .slice(0, 20);
      if (rows.length === 0) {
        await replyTo(chatId, "No live sessions reported by this server.");
        return;
      }
      const bound = bindingFor(chatKey);
      const keyboardRows = rows.slice(0, 5).map((row) => [
        button(`Use ${row.title.slice(0, 24) || shortId(row.id)}`, {
          kind: "bind",
          chatKey,
          chatId,
          sessionID: row.id,
        }),
      ]);
      await replyTo(
        chatId,
        `Live sessions:\n${rows
          .map((row, index) => {
            const mark = bound?.sessionID === row.id ? "* " : `${index + 1}. `;
            return `${mark}${shortId(row.id)}  ${row.title.slice(0, 60)}  [${projectLabel(row.directory)}]`;
          })
          .join("\n")}\n\nBind with /use <number>, /use <id>, or the buttons below.`,
        { inline_keyboard: keyboardRows },
      );
    }

    async function handleRefresh(
      chatId: number,
      userId: number,
      chatKey: string,
      filter: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/refresh filter=${filter === "" ? "<none>" : filter}`);
      // Titles are cached for the process lifetime; refresh forces the next
      // /tasks view to re-read them instead of reusing stale names.
      titleCache.clear();
      await handleTasks(chatId, userId, chatKey, filter);
    }

    async function doFork(
      chatId: number,
      chatKey: string,
      sessionID: string,
      before?: string,
    ): Promise<boolean> {
      let forked: any;
      try {
        forked = await apiPost(
          `/api/session/${sessionID}/fork`,
          before === undefined ? {} : { before },
        );
      } catch (error) {
        audit(`err op=session.fork session=${sessionID} error=${JSON.stringify(errorText(error))}`);
        await replyTo(chatId, "Could not fork this session from here.");
        return false;
      }
      const info = forked?.data ?? forked;
      const forkID = sessionIdOf(info) ?? String(info?.id ?? "");
      if (forkID === "") {
        await replyTo(chatId, "Fork created but the server returned no id. Send /tasks.");
        return false;
      }
      await bindTo(chatId, chatKey, forkID);
      audit(`cmd chat=${chatId} fork=${forkID} source=${sessionID} result=ok`);
      return true;
    }

    async function handleFork(
      chatId: number,
      userId: number,
      chatKey: string,
      arg: string,
    ): Promise<void> {
      const want = arg.trim();
      audit(`cmd chat=${chatId} user=${userId} cmd=/fork arg=${want === "" ? "<latest>" : want}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      let body: Record<string, unknown> = {};
      if (want !== "") {
        const before = want.replace(/^before:/i, "").trim();
        if (!/^msg_[A-Za-z0-9]+$/.test(before)) {
          await replyTo(chatId, "Usage: /fork [msg_<id>] or /fork [before:msg_<id>].");
          return;
        }
        body = { before };
      }
      await doFork(chatId, chatKey, binding.sessionID, (body.before as string | undefined));
    }

    async function handleMessages(
      chatId: number,
      userId: number,
      chatKey: string,
      countText: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/messages n=${countText === "" ? "5" : countText}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const parsed = Number.parseInt(countText, 10);
      const count = Number.isNaN(parsed) ? 5 : Math.min(Math.max(parsed, 1), 10);
      let messages: any[];
      try {
        const body: any = await apiGet(`/api/session/${binding.sessionID}/message?limit=40&order=desc`);
        messages = Array.isArray(body) ? body : Array.isArray(body?.data) ? body.data : [];
      } catch (error) {
        await replyTo(chatId, `Could not read messages: ${errorText(error)}`);
        return;
      }
      const users = messages
        .filter((m: any) => m?.type === "user" || m?.info?.role === "user")
        .slice(0, count);
      if (users.length === 0) {
        await replyTo(chatId, "No user messages in this session yet.");
        return;
      }
      const keyboardRows: Array<Array<{ text: string; callback_data: string }>> = [];
      const lines = users.map((m: any, index: number) => {
        const id = String(m?.id ?? "?");
        const text = String(m?.text ?? "").replace(/\s+/g, " ").slice(0, 100);
        keyboardRows.push([
          button(`Revert @${index + 1}`, {
            kind: "msg-revert",
            chatKey,
            chatId,
            sessionID: binding.sessionID,
            data: id,
          }),
          button(`Fork @${index + 1}`, {
            kind: "msg-fork",
            chatKey,
            chatId,
            sessionID: binding.sessionID,
            data: id,
          }),
        ]);
        return `${index + 1}. ${text === "" ? "(attachment only)" : text}`;
      });
      await replyTo(
        chatId,
        `Your messages (newest first):\n${lines.join("\n")}\n\nRevert rolls back to that message and discards everything after. Fork branches from it.`,
        { inline_keyboard: keyboardRows },
      );
    }

    async function listForms(sessionID: string): Promise<any[]> {
      const body: any = await apiGet(`/api/session/${sessionID}/form`);
      const list = Array.isArray(body) ? body : Array.isArray(body?.data) ? body.data : [];
      audit(`forms-http session=${sessionID} count=${list.length}`);
      return list;
    }

    function formFieldSummary(field: any): string {
      const key = String(field?.key ?? "?");
      const type = String(field?.type ?? "?");
      const title = typeof field?.title === "string" && field.title.trim() !== "" ? ` ${field.title}` : "";
      if (type === "multiselect" && Array.isArray(field?.options)) {
        const options = field.options
          .slice(0, 8)
          .map((option: any) => String(option?.value ?? option?.label ?? option))
          .join("|");
        return `${key}(${type})${title}: ${options}`;
      }
      if (type === "external") return `${key}(${type})${title}: open in OpenCode`;
      return `${key}(${type})${title}`;
    }

    async function handleForms(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/forms`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      let forms: any[];
      try {
        forms = await listForms(binding.sessionID);
      } catch (error) {
        audit(`err op=form.list session=${binding.sessionID} error=${JSON.stringify(errorText(error))}`);
        await replyTo(chatId, "Could not list questions/forms from here.");
        return;
      }
      if (forms.length === 0) {
        await replyTo(chatId, "No open questions/forms for the bound session.");
        return;
      }
      const lines = forms.slice(0, 5).map((form: any) => {
        const id = String(form?.id ?? "?");
        const title = String(form?.title ?? "question");
        const fields = Array.isArray(form?.fields) ? form.fields.map(formFieldSummary).join("; ") : "";
        return `${shortId(id)}  ${title.slice(0, 60)}${fields === "" ? "" : `\n  ${fields.slice(0, 300)}`}`;
      });
      await replyTo(
        chatId,
        `Open questions:\n${lines.join("\n")}\n\nAnswer with /answer <formId> key=value ...`,
      );
    }

    function coerceFormValue(field: any, raw: string): unknown {
      const type = String(field?.type ?? "string");
      if (type === "boolean") {
        const normalized = raw.trim().toLowerCase();
        if (["yes", "y", "true", "1", "on"].includes(normalized)) return true;
        if (["no", "n", "false", "0", "off"].includes(normalized)) return false;
        throw new Error(`expected yes/no for ${String(field?.key ?? "field")}`);
      }
      if (type === "integer") {
        const value = Number.parseInt(raw, 10);
        if (!Number.isInteger(value)) throw new Error(`expected an integer for ${String(field?.key ?? "field")}`);
        return value;
      }
      if (type === "number") {
        const value = Number(raw);
        if (!Number.isFinite(value)) throw new Error(`expected a number for ${String(field?.key ?? "field")}`);
        return value;
      }
      if (type === "multiselect") {
        return raw
          .split(",")
          .map((part) => part.trim())
          .filter((part) => part !== "");
      }
      return raw;
    }

    async function handleAnswer(
      chatId: number,
      userId: number,
      chatKey: string,
      arg: string,
    ): Promise<void> {
      // Audit keys only; form answers can contain sensitive values.
      const parts = arg.trim().split(/\s+/).filter((part) => part !== "");
      const formRef = parts[0] ?? "";
      audit(`cmd chat=${chatId} user=${userId} cmd=/answer form=${formRef === "" ? "<none>" : shortId(formRef)} keys=${parts.length > 1 ? parts.length - 1 : 0}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      if (formRef === "" || parts.length < 2) {
        await replyTo(chatId, "Usage: /answer <formId> key=value [key=value ...].");
        return;
      }
      let forms: any[];
      try {
        forms = await listForms(binding.sessionID);
      } catch (error) {
        await replyTo(chatId, `Could not read open questions: ${errorText(error)}`);
        return;
      }
      const form =
        forms.find((item: any) => String(item?.id ?? "") === formRef) ??
        forms.find((item: any) => String(item?.id ?? "").startsWith(formRef));
      if (!form) {
        await replyTo(chatId, `No open question matches "${formRef}". Send /forms.`);
        return;
      }
      const fields = Array.isArray(form?.fields) ? form.fields : [];
      const answer: Record<string, unknown> = {};
      try {
        for (const pair of parts.slice(1)) {
          const equals = pair.indexOf("=");
          if (equals <= 0) throw new Error(`expected key=value, got "${pair.slice(0, 40)}"`);
          const key = pair.slice(0, equals);
          const raw = pair.slice(equals + 1);
          const field = fields.find((item: any) => String(item?.key ?? "") === key);
          if (!field) throw new Error(`unknown field "${key}"`);
          answer[key] = coerceFormValue(field, raw);
        }
      } catch (error) {
        await replyTo(chatId, `Could not parse that answer: ${errorText(error)}`);
        return;
      }
      try {
        await apiPost(`/api/session/${binding.sessionID}/form/${form.id}/reply`, { answer });
      } catch (error) {
        audit(`err op=form.reply session=${binding.sessionID} form=${shortId(String(form.id))} error=${JSON.stringify(errorText(error))}`);
        await replyTo(chatId, `Could not send that answer: ${errorText(error)}`);
        return;
      }
      audit(`cmd chat=${chatId} form=${shortId(String(form.id))} result=ok`);
      await replyTo(chatId, `Answer sent for ${String(form?.title ?? "question").slice(0, 80)}.`);
    }

    // --- scheduled tasks ----------------------------------------------------
    // Cron-like prompts: one-shot (`/task in 30m ...`, `/task at 14:30 ...`)
    // or recurring (`/task every 1h ...`). Each run gets a fresh session in
    // the task's project; results go to the owner's chat on both bots.
    // Guardrails: max 10 tasks, min 5-minute interval, no overlapping runs of
    // the same task. Persisted so tasks survive reloads.

    const KEY_SCHED = "scheduledTasks";
    const SCHED_MAX_TASKS = 10;
    const SCHED_MIN_INTERVAL_MS = 5 * 60_000;
    const SCHED_TICK_MS = 30_000;
    // A run that never finishes blocks its recurring task forever via the
    // overlap guard, so interrupt it and let the next interval fire fresh.
    const SCHED_MAX_RUN_MS = 2 * 3_600_000;

    interface ScheduledTask {
      id: string;
      text: string;
      kind: "once" | "every";
      projectDir: string;
      projectLabel: string;
      model?: string;
      agent?: string;
      chatKey: string;
      chatId: number;
      nextAt: number;
      everyMs?: number;
      runningSessionID?: string;
      lastRunAt?: number;
      failCount: number;
    }

    let scheduledTasks: Record<string, ScheduledTask> = {};
    // sessionID -> taskID for runs currently in flight.
    const scheduledRuns = new Map<string, string>();
    let scheduler: ReturnType<typeof setInterval> | undefined;

    async function persistTasks(): Promise<void> {
      await persist(KEY_SCHED, scheduledTasks);
    }

    function describeTask(task: ScheduledTask): string {
      const when =
        task.kind === "once"
          ? `once at ${new Date(task.nextAt).toLocaleString()}`
          : `every ${fmtDuration(task.everyMs ?? 0)} (next: ${fmtDuration(Math.max(0, task.nextAt - Date.now()))})`;
      const running = task.runningSessionID !== undefined ? " [running]" : "";
      return `${task.id}  ${when}${running}\n  ${task.projectLabel}: ${task.text.slice(0, 80)}`;
    }

    async function runScheduledTask(task: ScheduledTask): Promise<void> {
      if (task.runningSessionID !== undefined && liveTurns.has(task.runningSessionID)) {
        audit(`sched task=${task.id} result=skipped-overlap`);
        return;
      }
      let info: any;
      try {
        info = await callSessions<any>("create", {
          title: `[scheduled] ${task.text.slice(0, 50)}`,
          location: { directory: task.projectDir },
          ...(task.model ? { model: task.model } : {}),
          ...(task.agent ? { agent: task.agent } : {}),
        });
      } catch (error) {
        task.failCount += 1;
        await persistTasks();
        audit(`sched task=${task.id} error=${JSON.stringify(errorText(error))}`);
        await sendToChat(task.chatId, `Scheduled task ${task.id} failed to start: ${errorText(error)}`);
        return;
      }
      const sessionID = sessionIdOf(info) ?? String(info?.id ?? "");
      if (sessionID === "") {
        audit(`sched task=${task.id} error="no session id returned"`);
        return;
      }
      task.runningSessionID = sessionID;
      task.lastRunAt = Date.now();
      scheduledRuns.set(sessionID, task.id);
      await persistTasks();
      audit(`sched task=${task.id} session=${sessionID} result=started`);
      await admitPrompt(sessionID, task.text, task.chatId, task.chatKey, true);
    }

    async function schedulerTick(): Promise<void> {
      if (!crossOwner) return;
      const now = Date.now();
      for (const task of Object.values(scheduledTasks)) {
        if (task.nextAt > now) continue;
        // Timed-out run: interrupt the stuck session so the next interval can
        // fire instead of being blocked by the overlap guard forever.
        if (task.runningSessionID !== undefined) {
          const turn = liveTurns.get(task.runningSessionID);
          if (turn !== undefined && now - turn.startedAt > SCHED_MAX_RUN_MS) {
            audit(`sched task=${task.id} result=timeout-interrupt session=${task.runningSessionID}`);
            try {
              await callSessions("interrupt", { sessionID: task.runningSessionID });
            } catch (error) {
              audit(`sched task=${task.id} error=${JSON.stringify(errorText(error))}`);
            }
            await finishTurn(turn, "interrupted");
            continue;
          }
          if (turn !== undefined) continue;
        }
        // Stale running marker (e.g. server restarted mid-run): clear and run.
        if (task.runningSessionID !== undefined) {
          audit(`sched task=${task.id} result=stale-run-cleared`);
          scheduledRuns.delete(task.runningSessionID);
          task.runningSessionID = undefined;
        }
        await runScheduledTask(task);
      }
    }

    function startScheduler(): void {
      if (scheduler !== undefined) return;
      scheduler = setInterval(() => {
        void schedulerTick().catch((error: unknown) =>
          audit(`err op=sched error=${JSON.stringify(errorText(error))}`),
        );
      }, SCHED_TICK_MS);
    }

    function stopScheduler(): void {
      if (scheduler !== undefined) {
        clearInterval(scheduler);
        scheduler = undefined;
      }
    }

    function parseTaskSchedule(arg: string): { kind: "once" | "every"; at: number; everyMs?: number; text: string } | undefined {
      const onceIn = /^in\s+(\d+)\s*(m|min|mins|h|hr|hrs)\s+([\s\S]+)$/i.exec(arg);
      if (onceIn) {
        const n = Number.parseInt(onceIn[1]!, 10);
        const ms = /h/i.test(onceIn[2]!) ? n * 3_600_000 : n * 60_000;
        return { kind: "once", at: Date.now() + ms, text: onceIn[3]!.trim() };
      }
      const atMatch = /^at\s+(\d{1,2}):(\d{2})\s+([\s\S]+)$/.exec(arg);
      if (atMatch) {
        const target = new Date();
        target.setHours(Number.parseInt(atMatch[1]!, 10), Number.parseInt(atMatch[2]!, 10), 0, 0);
        if (target.getTime() <= Date.now()) target.setDate(target.getDate() + 1);
        return { kind: "once", at: target.getTime(), text: atMatch[3]!.trim() };
      }
      const every = /^every\s+(\d+)\s*(m|min|mins|h|hr|hrs)\s+([\s\S]+)$/i.exec(arg);
      if (every) {
        const n = Number.parseInt(every[1]!, 10);
        const ms = /h/i.test(every[2]!) ? n * 3_600_000 : n * 60_000;
        if (ms < SCHED_MIN_INTERVAL_MS || every[3]!.trim() === "") return undefined;
        return { kind: "every", at: Date.now() + ms, everyMs: ms, text: every[3]!.trim() };
      }
      return undefined;
    }

    async function handleMcps(chatId: number, userId: number, chatKey: string): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/mcps`);
      let servers: any[];
      try {
        const raw = unwrap<any>(await ctx.mcp.list(), "mcp.list");
        servers = Array.isArray(raw) ? raw : Array.isArray(raw?.data) ? raw.data : [];
      } catch (error) {
        await replyTo(chatId, `MCP list is unavailable on this build (${errorText(error)}).`);
        return;
      }
      if (servers.length === 0) {
        await replyTo(chatId, "No MCP servers registered.");
        return;
      }
      const rows = servers.filter((s) => typeof s?.name === "string").slice(0, 12);
      const keyboardRows = rows.map((s) => {
        const name = String(s.name);
        const enabled = String(s?.status?.status ?? s?.status ?? "").toLowerCase() !== "disabled";
        return [
          button(`${enabled ? "Disable" : "Enable"} ${name}`.slice(0, 40), {
            kind: "mcp-toggle",
            chatKey,
            chatId,
            sessionID: "",
            data: `${enabled ? "off" : "on"}:${name}`,
          }),
        ];
      });
      await replyTo(
        chatId,
        `MCP servers:\n${rows
          .map((s) => {
            const status = String(s?.status?.status ?? s?.status ?? "?");
            return `- ${s.name}  [${status}]`;
          })
          .join("\n")}`,
        { inline_keyboard: keyboardRows },
      );
    }

    async function handleTask(
      chatId: number,
      userId: number,
      chatKey: string,
      arg: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/task arg=${arg === "" ? "<none>" : arg.slice(0, 40)}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const parsed = parseTaskSchedule(arg.trim());
      if (!parsed || parsed.text === "") {
        await replyTo(
          chatId,
          "Usage:\n/task in 30m <prompt>\n/task at 14:30 <prompt>\n/task every 1h <prompt>\n(minimum interval 5m)",
        );
        return;
      }
      if (Object.keys(scheduledTasks).length >= SCHED_MAX_TASKS) {
        await replyTo(chatId, `Task limit reached (${SCHED_MAX_TASKS}). Delete one with /tasklist first.`);
        return;
      }
      const root = (sessionRoot !== undefined ? await sessionRoot(binding.sessionID) : undefined) ?? binding.projectDir;
      if (!root) {
        await replyTo(chatId, "Could not determine this session's project directory.");
        return;
      }
      const id = `task_${Date.now().toString(36)}`;
      const task: ScheduledTask = {
        id,
        text: parsed.text,
        kind: parsed.kind,
        projectDir: root,
        projectLabel: projectLabel(root),
        chatKey,
        chatId,
        nextAt: parsed.at,
        everyMs: parsed.everyMs,
        failCount: 0,
      };
      scheduledTasks[id] = task;
      await persistTasks();
      audit(`sched task=${id} kind=${parsed.kind} result=created`);
      await replyTo(chatId, `Task ${id} scheduled.\n${describeTask(task)}`);
    }

    async function handleTaskList(chatId: number, userId: number, chatKey: string): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/tasklist`);
      const rows = Object.values(scheduledTasks);
      if (rows.length === 0) {
        await replyTo(chatId, "No scheduled tasks. Create one with /task in 30m <prompt>.");
        return;
      }
      const keyboardRows = rows.slice(0, 8).map((task) => [
        button(`Delete ${task.id}`, {
          kind: "task-delete",
          chatKey,
          chatId,
          sessionID: "",
          data: task.id,
        }),
      ]);
      await replyTo(
        chatId,
        `Scheduled tasks:\n${rows.map(describeTask).join("\n\n")}`,
        { inline_keyboard: keyboardRows },
      );
    }

    // --- turn lifecycle (Â§6) ----------------------------------------------

    function today(): string {
      return new Date().toISOString().slice(0, 10);
    }

    function dailyExceeded(chatKey: string): boolean {
      if (settings.dailyPromptCap <= 0) return false;
      const counter = dailyCounters.get(chatKey);
      if (!counter || counter.day !== today()) return false;
      return counter.count >= settings.dailyPromptCap;
    }

    function countPrompt(chatKey: string): void {
      const counter = dailyCounters.get(chatKey);
      if (!counter || counter.day !== today()) {
        dailyCounters.set(chatKey, { day: today(), count: 1 });
        return;
      }
      counter.count += 1;
    }

    function bodyFingerprint(text: string): string {
      // Â§11/Â§14.6: never log prompt bodies, only a short digest for correlation.
      // FNV-1a 32 bit - not a security primitive, only an id to join log lines.
      let hash = 0x811c9dc5;
      for (let index = 0; index < text.length; index += 1) {
        hash ^= text.charCodeAt(index);
        hash = Math.imul(hash, 0x01000193) >>> 0;
      }
      return hash.toString(16).padStart(8, "0");
    }

    function beginTurn(
      sessionID: string,
      chatKey: string,
      chatId: number,
      remote: boolean,
    ): Promise<void> {
      const turn: LiveTurn = {
        sessionID,
        chatKey,
        chatId,
        startedAt: Date.now(),
        heartbeats: 0,
      };
      liveTurns.set(sessionID, turn);
      turnRecords[sessionID] = { startedAt: turn.startedAt, chatKey, remote };
      void persist(KEY_TURNS, turnRecords);
      return startHeartbeat(turn);
    }

    // Â§14.3: heartbeats every 120 s, at most 4 per turn, edited into the ack
    // message instead of spamming a new one.
    async function startHeartbeat(turn: LiveTurn): Promise<void> {
      const ack = await sendToChat(turn.chatId, "sent, working...");
      if (ack.ok) turn.ackMessageId = ack.messageId;
      turn.timer = setInterval(() => {
        if (turn.heartbeats >= HEARTBEAT_MAX) {
          if (turn.timer) clearInterval(turn.timer);
          turn.timer = undefined;
          return;
        }
        turn.heartbeats += 1;
        if (!alertEnabled("progress")) return;
        const elapsed = fmtDuration(Date.now() - turn.startedAt);
        if (turn.ackMessageId !== undefined) {
          void editSent(
            turn.chatId,
            turn.ackMessageId,
            `still working... ${elapsed} elapsed (${turn.heartbeats}/${HEARTBEAT_MAX})`,
          );
        } else {
          void sendToChat(turn.chatId, `still working... ${elapsed} elapsed`, undefined, "notify");
        }
      }, HEARTBEAT_MS);
      turn.timer.unref?.();
    }

    async function admitPrompt(
      sessionID: string,
      text: string,
      chatId: number,
      chatKey: string,
      remote: boolean,
    ): Promise<void> {
      const fingerprint = bodyFingerprint(text);
      // Declared outside the try so the audit line below can report it.
      const steering = liveTurns.has(sessionID) && settings.steerBusyTurns === true;
      try {
        // Returns an admission receipt (Session.Inbox.User), not a completed
        // turn: completion is detected from session.execution.succeeded.
        // The V2 prompt body carries `text`, not `parts`. `delivery: "steer"`
        // injects the message into an already-running turn
        // (Session.Inbox.Delivery = steer|queue); otherwise it is left unset so
        // the server's default queueing applies and our own single-slot queue
        // remains the fallback.
        const promptPayload: Record<string, unknown> = { sessionID, text };
      // Tier 3: consume anything staged via Telegram upload or /attach.
      const staged = takeStagedFiles(chatKey);
      if (staged.length > 0) {
        promptPayload.files = staged;
        audit(`attach-sent chat=${chatId} session=${sessionID} count=${staged.length}`);
      }
        if (steering) promptPayload.delivery = "steer";
        await callSessions("prompt", promptPayload);
      } catch (error) {
        // Â§9: surface the server's reason verbatim, never retry automatically.
        const message = errorText(error);
        audit(`err op=prompt session=${sessionID} error=${JSON.stringify(message)}`);
        await replyTo(chatId, `prompt rejected: ${message}`);
        return;
      }
      audit(
        `prompt chat=${chatId} session=${sessionID} chars=${text.length} hash=${fingerprint}` +
          `${steering ? " delivery=steer" : ""}` +
          `${settings.logBodies ? ` body=${JSON.stringify(text.slice(0, 200))}` : ""}`,
      );
      await beginTurn(sessionID, chatKey, chatId, remote);
    }

    function queueDepth(sessionID: string): number {
      return promptQueues.get(sessionID)?.length ?? 0;
    }

    async function flushQueue(sessionID: string): Promise<void> {
      const queue = promptQueues.get(sessionID);
      const queued = queue?.shift();
      if (queue !== undefined && queue.length === 0) promptQueues.delete(sessionID);
      await persistQueues();
      if (!queued) return;
      const binding = bindingFor(queued.chatKey);
      if (!binding || binding.sessionID !== sessionID) {
        await replyTo(queued.chatId, "Queued prompt dropped: the chat is no longer bound to that session.");
        return;
      }
      audit(`prompt chat=${queued.chatId} session=${sessionID} chars=${queued.text.length} queued=1 remaining=${queue?.length ?? 0}`);
      await replyTo(queued.chatId, "Sending the queued prompt now.");
      await admitPrompt(sessionID, queued.text, queued.chatId, queued.chatKey, true);
    }

    async function handleQueue(chatId: number, userId: number, chatKey: string): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/queue`);
      const binding = bindingFor(chatKey);
      if (!binding) {
        await replyTo(chatId, "No session bound. Send /tasks, then /use <index>.");
        return;
      }
      const queue = promptQueues.get(binding.sessionID) ?? [];
      if (queue.length === 0) {
        await replyTo(chatId, "Queue is empty.");
        return;
      }
      const keyboardRows = queue.slice(0, 8).map((item, index) => [
        button(`Withdraw #${index + 1}`, {
          kind: "queue-withdraw",
          chatKey,
          chatId,
          sessionID: binding.sessionID,
          data: item.id,
        }),
      ]);
      await replyTo(
        chatId,
        `Queued prompts:\n${queue
          .map((item, index) => `#${index + 1}. ${item.text.replace(/\s+/g, " ").slice(0, 80)}`)
          .join("\n")}`,
        { inline_keyboard: keyboardRows },
      );
    }

    async function handleBtw(
      chatId: number,
      userId: number,
      chatKey: string,
      text: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/btw chars=${text.length}`);
      if (settings.mode !== "control") {
        await replyTo(chatId, "Mode is notify - remote prompts are disabled. Send /mode control to enable them.");
        return;
      }
      const binding = bindingFor(chatKey);
      if (!binding) {
        await replyTo(chatId, "No session bound to this chat. Send /tasks, then /use <index>.");
        return;
      }
      if (text.trim() === "") {
        await replyTo(chatId, "Usage: /btw <prompt> - runs now if idle, first in queue otherwise.");
        return;
      }
      if (text.length > PROMPT_CHAR_CAP) {
        await replyTo(chatId, `Prompt too long: ${text.length} chars, cap is ${PROMPT_CHAR_CAP}.`);
        return;
      }
      if (dailyExceeded(chatKey)) {
        await replyTo(chatId, `Daily prompt cap reached (${settings.dailyPromptCap}). Resets at UTC midnight.`);
        return;
      }
      countPrompt(chatKey);
      if (!liveTurns.has(binding.sessionID)) {
        await admitPrompt(binding.sessionID, text, chatId, chatKey, true);
        return;
      }
      const queue = promptQueues.get(binding.sessionID) ?? [];
      if (queue.length >= QUEUE_CAP) {
        await replyTo(chatId, `Queue is full (${QUEUE_CAP}). Withdraw one with /queue first.`);
        return;
      }
      queue.unshift({ id: newToken(), text, chatKey, chatId });
      promptQueues.set(binding.sessionID, queue);
      await persistQueues();
      audit(`prompt chat=${chatId} session=${binding.sessionID} queued=1 position=front`);
      await replyTo(chatId, "Inserted at the front of the queue - it runs right after the current turn.");
    }

    async function finishTurn(
      turn: LiveTurn,
      outcome: "succeeded" | "failed" | "interrupted",
      detail?: string,
    ): Promise<void> {
      if (turn.timer) clearInterval(turn.timer);
      turn.timer = undefined;
      liveTurns.delete(turn.sessionID);
      delete turnRecords[turn.sessionID];
      void persist(KEY_TURNS, turnRecords);
      const elapsed = Date.now() - turn.startedAt;
      audit(
        `turn session=${turn.sessionID} state=running->idle ms=${elapsed} outcome=${outcome}` +
          `${detail ? ` detail=${JSON.stringify(detail.slice(0, 160))}` : ""}`,
      );

      if (turn.ackMessageId !== undefined && outcome === "succeeded") {
        await editSent(turn.chatId, turn.ackMessageId, `done in ${fmtDuration(elapsed)}.`);
      }

      if (outcome === "succeeded") {
        const texts = await lastAssistantTexts(turn.sessionID, 1, turn.startedAt);
        const body = texts.length > 0 ? texts[0] : "(no new reply this turn)";
        await sendToChat(turn.chatId, `Done in ${fmtDuration(elapsed)}.\n\n${body}`);
      } else if (outcome === "failed") {
        await sendToChat(turn.chatId, `Turn failed: ${detail ?? "unknown error"}`);
      }

      // Scheduled runs also mirror a one-line alert on the notify bot and
      // advance the task (once tasks are deleted, recurring ones rescheduled).
      const scheduledId = scheduledRuns.get(turn.sessionID);
      if (scheduledId !== undefined) {
        const task = scheduledTasks[scheduledId];
        scheduledRuns.delete(turn.sessionID);
        if (task !== undefined) {
          await sendToChat(
            turn.chatId,
            `[scheduled] ${task.id}: ${outcome} in ${fmtDuration(elapsed)}.`,
            undefined,
            "notify",
          );
          if (task.kind === "once" || outcome === "interrupted") {
            delete scheduledTasks[task.id];
            audit(`sched task=${task.id} result=completed-removed outcome=${outcome}`);
          } else {
            task.runningSessionID = undefined;
            task.nextAt = Date.now() + (task.everyMs ?? SCHED_MIN_INTERVAL_MS);
            audit(`sched task=${task.id} result=rescheduled outcome=${outcome}`);
          }
          await persistTasks();
        }
      }

      // An explicit /stop means the user does not want the agent restarted.
      if (outcome === "interrupted") {
        interruptedAt.set(turn.sessionID, Date.now());
        return;
      }
      await flushQueue(turn.sessionID);
    }

    async function lastAssistantTexts(
      sessionID: string,
      count: number,
      // Only text written after this timestamp counts. Without it, a turn that
      // produces no text (e.g. a fast failure) would resurface the previous
      // turn's reply and read as a repeated old message.
      since?: number,
    ): Promise<string[]> {
      try {
        // Fetch a wide window: a tool-heavy turn can easily put 100+
        // reasoning/tool messages between text parts. Verified live: the
        // newest text sat ~100 messages back behind pure tool traffic.
        const messages = await callMessageList(sessionID, Math.max(count * 20, 100));
        // Two shapes: the context accessor nests role/parts under `info`, while
        // the HTTP endpoint uses flat `type` with a `content` array.
        const assistants = messages.filter(
          (m: any) => m?.info?.role === "assistant" || m?.role === "assistant" || m?.type === "assistant",
        );
        return assistants
          .map((m: any) => ({
            // Both shapes carry timing; sort by it so "latest" is correct
            // regardless of the order the endpoint returned.
            at: Number(m?.time?.completed ?? m?.time?.created ?? 0),
            text: (Array.isArray(m?.parts) ? m.parts : Array.isArray(m?.content) ? m.content : [])
              .filter((part: any) => part?.type === "text" && typeof part.text === "string")
              .map((part: any) => part.text.trim())
              .filter((text: string) => text !== "")
              .join("\n\n"),
          }))
          // Take the latest message that actually contains text: the tail of a
          // turn is often reasoning/tool parts with no visible text. When the
          // turn's start is known, ignore anything written before it so a
          // textless turn cannot echo the previous turn's reply.
          .filter((item) => item.text !== "" && (since === undefined || item.at === 0 || item.at >= since))
          .sort((a, b) => a.at - b.at)
          .slice(-count)
          .map((item) => item.text);
      } catch (error) {
        audit(`err op=message.list session=${sessionID} error=${JSON.stringify(errorText(error))}`);
        return [];
      }
    }

    async function handlePrompt(
      chatId: number,
      userId: number,
      chatKey: string,
      text: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=prompt chars=${text.length}`);
      if (settings.mode !== "control") {
        await replyTo(
          chatId,
          "Mode is notify - remote prompts are disabled. Send /mode control to enable them.",
        );
        return;
      }
      const binding = bindingFor(chatKey);
      if (!binding) {
        await replyTo(chatId, "No session bound to this chat. Send /tasks, then /use <index>.");
        return;
      }
      if (text.length > PROMPT_CHAR_CAP) {
        await replyTo(
          chatId,
          `Prompt too long: ${text.length} chars, cap is ${PROMPT_CHAR_CAP}. Split it into two messages.`,
        );
        return;
      }
      if (dailyExceeded(chatKey)) {
        await replyTo(
          chatId,
          `Daily prompt cap reached (${settings.dailyPromptCap}). Resets at UTC midnight.`,
        );
        return;
      }
      countPrompt(chatKey);

      const live = liveTurns.get(binding.sessionID);
      if (live) {
        const queue = promptQueues.get(binding.sessionID) ?? [];
        if (queue.length >= QUEUE_CAP) {
          await replyTo(chatId, `Queue is full (${QUEUE_CAP}). Withdraw one with /queue first.`);
          return;
        }
        queue.push({ id: newToken(), text, chatKey, chatId });
        promptQueues.set(binding.sessionID, queue);
        await persistQueues();
        audit(`prompt chat=${chatId} session=${binding.sessionID} queued=1 position=${queue.length}`);
        await replyTo(
          chatId,
          `Queued at position ${queue.length} - it runs when the current turn finishes. Manage with /queue.`,
        );
        return;
      }
      await admitPrompt(binding.sessionID, text, chatId, chatKey, true);
    }

    // --- permission flow (Â§7) ---------------------------------------------

    function permissionTitleOf(p: any): string {
      const title = p?.title ?? p?.message ?? p?.metadata?.message ?? p?.metadata?.title;
      return typeof title === "string" && title.trim() !== "" ? title : "tool call needs approval";
    }

    function permissionResourcesOf(p: any): string {
      const raw = p?.resources ?? p?.metadata?.resources ?? p?.pattern;
      const list = Array.isArray(raw) ? raw : typeof raw === "string" ? [raw] : [];
      return list
        .filter((item: unknown) => typeof item === "string")
        .slice(0, 5)
        .join("\n");
    }

    // The permission "type" is the tool/domain that triggered the request; the
    // live payload naming could not be verified from the spec, so exec-like
    // types are matched by name and anything unknown is treated as non-exec.
    function isExecPermission(p: any): boolean {
      const type = String(p?.type ?? p?.action ?? "").toLowerCase();
      return /bash|exec|shell|command|powershell|cmd|terminal/.test(type);
    }

    function permissionIdOf(p: any): string | undefined {
      const id = p?.id ?? p?.permissionID ?? p?.permission?.id;
      return typeof id === "string" && id !== "" ? id : undefined;
    }

    async function replyPermission(
      pending: PendingPermission,
      response: "once" | "always" | "reject",
      userId: number,
      decisionLabel: string,
    ): Promise<boolean> {
      try {
        await callPermission("reply", {
          sessionID: pending.sessionID,
          requestID: pending.permissionID,
          response,
        });
      } catch (error) {
        // Â§9: an expired request must be reported, then pending re-listed.
        const message = errorText(error);
        audit(`err op=permission.reply session=${pending.sessionID} error=${JSON.stringify(message)}`);
        await replyTo(pending.chatId, `That request is no longer answerable: ${message}`);
        return false;
      }
      pendingPermissions.delete(pending.token);
      audit(
        `perm session=${pending.sessionID} request=${shortId(pending.permissionID)} decision=${decisionLabel} user=${userId}`,
      );
      await replyTo(pending.chatId, `Permission ${decisionLabel}.`);
      return true;
    }

    function pruneExpiredPermissions(): void {
      const now = Date.now();
      for (const pending of [...pendingPermissions.values()]) {
        if (pending.expiresAt > now) continue;
        pendingPermissions.delete(pending.token);
        // Â§14.5: unanswered requests are denied on expiry - default-deny is the
        // only acceptable failure mode for code execution.
        audit(
          `perm session=${pending.sessionID} request=${shortId(pending.permissionID)} decision=deny-expired user=-`,
        );
        void callPermission("reply", {
          sessionID: pending.sessionID,
          requestID: pending.permissionID,
          response: "reject",
        }).catch((error: unknown) =>
          audit(
            `err op=permission.reply session=${pending.sessionID} error=${JSON.stringify(errorText(error))}`,
          ),
        );
        void replyTo(pending.chatId, "Permission request expired and was denied.");
      }
      for (const [token, action] of [...pendingActions.entries()]) {
        if (now - action.createdAt > ACTION_TTL_MS) pendingActions.delete(token);
      }
      // Staged files nobody ever sent: drop entries older than an hour so
      // abandoned uploads neither linger in memory nor on disk.
      let swept = 0;
      for (const [chatKey, files] of [...stagedFiles.entries()]) {
        const fresh = files.filter((f) => now - f.at <= 3_600_000);
        for (const stale of files) {
          if (now - stale.at > 3_600_000) {
            deleteTempCopy(stale.uri);
            swept += 1;
          }
        }
        if (fresh.length === 0) stagedFiles.delete(chatKey);
        else stagedFiles.set(chatKey, fresh);
      }
      if (swept > 0) audit(`sweep staged-expired files=${swept}`);
    }

    function registerPendingPermission(
      sessionID: string,
      permissionID: string,
      chatKey: string,
      chatId: number,
      title: string,
    ): PendingPermission {
      const pending: PendingPermission = {
        token: newToken(),
        sessionID,
        permissionID,
        chatKey,
        chatId,
        title,
        createdAt: Date.now(),
        expiresAt: Date.now() + PERMISSION_TTL_MS,
      };
      pendingPermissions.set(pending.token, pending);
      return pending;
    }

    function permissionKeyboard(pending: PendingPermission) {
      const base = { chatKey: pending.chatKey, chatId: pending.chatId, sessionID: pending.sessionID };
      return {
        inline_keyboard: [
          [
            button("Allow once", { ...base, kind: "allow-once", permissionToken: pending.token }),
            button("Always", { ...base, kind: "always", permissionToken: pending.token }),
          ],
          [button("Deny", { ...base, kind: "deny", permissionToken: pending.token })],
        ],
      };
    }

    async function onPermissionAsked(sessionID: string, payload: any): Promise<void> {
      const permissionID = permissionIdOf(payload);
      const turn = sessionID ? liveTurns.get(sessionID) : undefined;

      if (!permissionID || !turn || settings.mode !== "control") {
        // Preserve the pre-existing notify-only behaviour for questions raised
        // outside a remote turn.
        const questions = questionTextsOf(payload);
        if (questions.length > 0) {
          if (!alertEnabled("waiting")) {
            audit(`waiting-suppressed session=${sessionID}`);
            return;
          }
          const title = sessionTitles.get(sessionID) ?? sessionID;
          const body = questions.map((text, i) => `${i + 1}. ${text}`).join("\n");
          await notify(`[?] ${title}` + String.fromCharCode(10) + String.fromCharCode(10) + `Questions:${String.fromCharCode(10)}${body}`);
        }
        return;
      }

      if (isExecPermission(payload) && !settings.allowExec) {
        audit(
          `perm session=${sessionID} request=${shortId(permissionID)} decision=deny-noexec user=-`,
        );
        await callPermission("reply", {
          sessionID,
          permissionID,
          response: "reject",
        }).catch((error: unknown) =>
          audit(
            `err op=permission.reply session=${sessionID} error=${JSON.stringify(errorText(error))}`,
          ),
        );
        await sendToChat(
          turn.chatId,
          `Denied ${String(payload?.type ?? "exec")} automatically: allowExec is false. ` +
            "Use /perm to review, or set allowExec true once you trust the session.",
        );
        return;
      }

      const pending = registerPendingPermission(
        sessionID,
        permissionID,
        turn.chatKey,
        turn.chatId,
        permissionTitleOf(payload),
      );
      audit(
        `perm session=${sessionID} request=${shortId(permissionID)} decision=pending user=- ttl=600000`,
      );
      const resources = permissionResourcesOf(payload);
      const detail = [
        `Approval needed: ${pending.title}`,
        `Type: ${String(payload?.type ?? "unknown")}`,
        `Session: ${shortId(sessionID)}`,
        ...(resources === "" ? [] : [`Resources:\n${resources}`]),
        "",
        "Unanswered requests are denied automatically after 10 minutes.",
      ].join("\n");
      if (!alertEnabled("waiting")) {
        audit(`waiting-suppressed session=${sessionID} request=${shortId(permissionID)}`);
        return;
      }
      await sendToChat(turn.chatId, detail, permissionKeyboard(pending));
    }

    async function handlePermList(chatId: number, userId: number, chatKey: string): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/perm`);
      const binding = bindingFor(chatKey);
      const rows: Array<{ id: string; sessionID: string; title: string; pending?: PendingPermission }> = [];
      try {
        const raw = await callPermission("list", binding ? { sessionID: binding.sessionID } : {});
        const list = Array.isArray(raw) ? raw : [];
        for (const item of list) {
          const permissionID = permissionIdOf(item);
          const sessionID = String(item?.sessionID ?? binding?.sessionID ?? "");
          if (!permissionID || sessionID === "") continue;
          rows.push({
            id: permissionID,
            sessionID,
            title: permissionTitleOf(item),
            pending: [...pendingPermissions.values()].find(
              (p) => p.chatKey === chatKey && p.permissionID === permissionID && p.sessionID === sessionID,
            ),
          });
        }
      } catch (error) {
        audit(`err op=permission.list error=${JSON.stringify(errorText(error))}`);
      }
      // Requests this process pushed to Telegram must appear even when the list
      // endpoint does not report them - otherwise /perm claims "none" while a
      // request is genuinely blocking the agent.
      const known = new Set(rows.map((row) => `${row.sessionID}:${row.id}`));
      for (const pending of pendingPermissions.values()) {
        if (pending.chatKey !== chatKey) continue;
        if (binding && pending.sessionID !== binding.sessionID) continue;
        if (known.has(`${pending.sessionID}:${pending.permissionID}`)) continue;
        rows.push({
          id: pending.permissionID,
          sessionID: pending.sessionID,
          title: pending.title,
          pending,
        });
      }
      if (rows.length === 0) {
        await replyTo(
          chatId,
          binding
            ? "No pending permission requests for the bound session."
            : "No pending permission requests. Bind a session with /use <index> to scope this.",
        );
        return;
      }
      // Only requests this process actually owns get buttons: an untracked id
      // has no permission id to reply with, so it is listed as text only.
      const keyboardRows: Array<Array<{ text: string; callback_data: string }>> = [];
      for (const row of rows.slice(0, 5)) {
        const pending = row.pending;
        if (!pending) continue;
        const base = { chatKey, chatId, sessionID: row.sessionID, permissionToken: pending.token };
        keyboardRows.push([
          button("Allow once", { ...base, kind: "allow-once" }),
          button("Always", { ...base, kind: "always" }),
        ]);
        keyboardRows.push([button("Deny", { ...base, kind: "deny" })]);
      }
      const lines = rows
        .slice(0, 10)
        .map(
          (row, index) =>
            `${index + 1}. ${shortId(row.id)}  ${row.title.slice(0, 50)}  (${shortId(row.sessionID)})`,
        );
      await replyTo(
        chatId,
        `Pending permissions:\n${lines.join("\n")}\n\nAnswer with /perm <id> yes|no|always` +
          (keyboardRows.length > 0 ? ", or the buttons." : "."),
        keyboardRows.length > 0 ? { inline_keyboard: keyboardRows } : undefined,
      );
    }

    async function handlePermAnswer(
      chatId: number,
      userId: number,
      chatKey: string,
      argument: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/perm arg=${argument}`);
      const [target, verdict] = argument.split(/\s+/);
      const normalized = String(verdict ?? "").toLowerCase();
      const response = normalized === "yes" || normalized === "y" ? "once"
        : normalized === "always" ? "always"
        : normalized === "no" || normalized === "n" ? "reject"
        : undefined;
      if (!target || !response) {
        await replyTo(chatId, "Usage: /perm <id> yes|no|always");
        return;
      }
      // Accept the row number shown by /perm, a permission id, or a unique prefix.
      // Only this chat's pending requests are answerable here.
      const ordered = [...pendingPermissions.values()]
        .filter((p) => p.chatKey === chatKey)
        .sort((a, b) => a.createdAt - b.createdAt);
      const pending = /^\d+$/.test(target)
        ? ordered[Number.parseInt(target, 10) - 1]
        : ordered.find(
            (p) => p.permissionID === target || p.permissionID.startsWith(target),
          );
      const binding = bindingFor(chatKey);
      const sessionID = pending?.sessionID ?? binding?.sessionID;
      const permissionID = pending?.permissionID ?? target;
      if (!sessionID) {
        await replyTo(chatId, "No session bound and no matching pending request. Use /perm first.");
        return;
      }
      if (/^\d+$/.test(target) && !pending) {
        await replyTo(chatId, `No pending request at position ${target}. Send /perm to re-list.`);
        return;
      }
      const record: PendingPermission = pending ?? {
        token: "",
        sessionID,
        permissionID,
        chatKey,
        chatId,
        title: "requested inline",
        createdAt: Date.now(),
        expiresAt: Date.now() + PERMISSION_TTL_MS,
      };
      const label = response === "once" ? "allow-once" : response === "always" ? "always" : "deny";
      await replyPermission(record, response, userId, label);
    }

    // --- notification redesign (Â§10) ---------------------------------------

    // Short real summary for notify mode: the agent's own last words, cut to a
    // phone-glanceable size on a line boundary.
    function summarizeBody(text: string, maxLines = 8, maxChars = 600): string {
      const kept: string[] = [];
      let used = 0;
      for (const line of text.split("\n")) {
        const trimmed = line.trim();
        if (trimmed === "") continue;
        if (kept.length >= maxLines || used + trimmed.length > maxChars) break;
        kept.push(trimmed);
        used += trimmed.length;
      }
      const summary = kept.join("\n");
      return summary.length < text.trim().length ? `${summary}\n...` : summary;
    }

    // Turn durations for the notify card: record when each session's execution
    // starts so the finished alert can show time-to-finish. Bounded and cleared
    // on completion.
    const executionStartedAt = new Map<string, number>();

    async function notifyFinished(
      sessionID: string,
      outcome: "succeeded" | "failed",
      detail?: string,
      // Control bot: bound chats only, so one conversation holds the summary
      // plus follow-up commands. Notify bot: one global chat, all sessions.
      controlChatIds?: number[],
      notifyChatId?: number | null,
    ): Promise<void> {
      if (outcome === "succeeded" && !alertEnabled("completed")) {
        audit(`completed-suppressed session=${sessionID}`);
        return;
      }
      if (outcome === "failed" && !alertEnabled("failed")) {
        audit(`failed-suppressed session=${sessionID}`);
        return;
      }
      const info = await fetchSession(sessionID);
      const title = titleOf(info) ?? sessionTitles.get(sessionID) ?? sessionID;
      const finishedAt = Date.now();
      const startedAt = executionStartedAt.get(sessionID);
      executionStartedAt.delete(sessionID);
      // <time to get finish>: measured turn duration when a start event was
      // seen, otherwise the finish clock time.
      const elapsed =
        startedAt !== undefined
          ? fmtDuration(Math.max(0, finishedAt - startedAt))
          : `${String(new Date(finishedAt).getHours()).padStart(2, "0")}:${String(new Date(finishedAt).getMinutes()).padStart(2, "0")}`;
      // Notify-bot card: fixed metadata format for every session.
      const directory = directoryOf(info);
      const model = info?.model ? `${info.model.providerID}/${info.model.id}` : "?";
      const cost = Number(info?.cost ?? 0);
      const files = Number(info?.summary?.files ?? 0);
      const facts = [
        directory === undefined ? "?" : projectLabel(directory),
        model,
        `$${cost.toFixed(4)}`,
        `${files} file${files === 1 ? "" : "s"}`,
        elapsed,
      ].join("  |  ");
      const card =
        outcome === "succeeded"
          ? [`[done] ${title}`, facts, `session ${shortId(sessionID)}`]
          : [
              `[fail] ${title}`,
              facts,
              `session ${shortId(sessionID)}`,
              ...(detail ? [`Error: ${detail.slice(0, 400)}`] : []),
            ];
      // Control mode gets the full agent reply; notify mode gets a short
      // real summary: the agent's own last words, truncated for the phone.
      // No metadata card - just what was actually done.
      const texts = await lastAssistantTexts(sessionID, 1, startedAt);
      const body =
        texts.length > 0
          ? texts[0]
          : startedAt === undefined
            ? "(no assistant text returned)"
            : "(no new reply this turn)";
      const summary =
        outcome === "succeeded"
          ? [`${title}`, "", summarizeBody(body)]
          : [`${title}`, ...(detail ? [`Error: ${detail.slice(0, 400)}`] : [])];
      const lines =
        outcome === "succeeded" && settings.mode === "control"
          ? [`${title}`, "", body]
          : outcome === "succeeded"
            ? summary
            : settings.mode === "control"
              ? [...summary, "", body]
              : summary;
      // Buttons are optional and off by default to keep alerts compact;
      // actions use /use + /stop.
      const keyboard =
        outcome === "succeeded" && settings.finishedButtons === true
          ? {
              inline_keyboard: [
                [
                  button("Open", { kind: "open", chatKey: "", chatId: activeChatId ?? 0, sessionID }),
                  button("Diff", { kind: "diff", chatKey: "", chatId: activeChatId ?? 0, sessionID }),
                  button("Stop", { kind: "stop", chatKey: "", chatId: activeChatId ?? 0, sessionID }),
                ],
              ],
            }
          : undefined;
      const targets =
        controlChatIds !== undefined && controlChatIds.length > 0
          ? controlChatIds
          : activeChatId === null
            ? []
            : [activeChatId];
      if (targets.length === 0 && (notifyChatId === undefined || notifyChatId === null)) {
        audit(`dropped no-bound-chat session=${sessionID}`);
        return;
      }
      for (const chatId of targets) {
        // Completion summaries go out on the control bot: that is where commands
        // live, so the summary and the follow-up (/use, /stop, /diff) stay in one
        // conversation.
        await sendToChat(chatId, lines.join("\n"), keyboard);
      }
      // The notify bot is a separate alert channel: every session, fixed card
      // format, never a keyboard.
      if (notifyChatId !== undefined && notifyChatId !== null) {
        await sendToChat(notifyChatId, card.join("\n"), undefined, "notify");
      }
    }

    // --- commands ----------------------------------------------------------

    function commandAllowed(name: string): boolean {
      const allowlist = config.allowedCommands;
      if (!allowlist) return true;
      return allowlist.includes(name);
    }

    async function requireBinding(chatId: number, chatKey: string): Promise<Binding | undefined> {
      const binding = bindingFor(chatKey);
      if (!binding) {
        await replyTo(chatId, "No session bound. Send /tasks, then /use <index>.");
        return undefined;
      }
      const info = await fetchSession(binding.sessionID);
      if (!info) {
        // Â§9: session gone -> clear the binding and point back at /tasks.
        await clearBinding(chatKey);
        await replyTo(
          chatId,
          `Session ${shortId(binding.sessionID)} is not accessible from here (removed or belongs to another location). Binding cleared. Send /tasks.`,
        );
        return undefined;
      }
      rememberSession(
        binding.sessionID,
        titleOf(info) ?? binding.label,
        directoryOf(info) ?? binding.projectDir,
        Number(info?.time?.created ?? Date.now()),
      );
      binding.lastUsed = Date.now();
      await persist(KEY_BINDINGS, bindings);
      return binding;
    }

    async function handleUse(
      chatId: number,
      userId: number,
      chatKey: string,
      argument: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/use arg=${argument}`);
      const listed = lastListings.get(chatKey) ?? [];
      let sessionID: string | undefined;
      if (/^\d+$/.test(argument)) {
        const index = Number.parseInt(argument, 10);
        sessionID = listed[index - 1];
        if (!sessionID) {
          await replyTo(chatId, `No session at index ${argument}. Send /tasks first.`);
          return;
        }
      } else if (argument !== "") {
        const rows = await listTasks(chatKey, argument).catch(() => [] as TaskRow[]);
        sessionID =
          rows.find((row) => row.id === argument)?.id ??
          rows.find((row) => row.id.startsWith(argument))?.id;
        if (!sessionID) {
          await replyTo(chatId, `No session matches "${argument}". Send /tasks to list them.`);
          return;
        }
      } else {
        await replyTo(chatId, "Usage: /use <index|id>");
        return;
      }
      await bindTo(chatId, chatKey, sessionID);
    }

    async function bindTo(chatId: number, chatKey: string, sessionID: string): Promise<void> {
      const info = await fetchSession(sessionID);
      if (!info) {
        await replyTo(
          chatId,
          `Session ${shortId(sessionID)} is not accessible from here. Send /tasks for the visible list.`,
        );
        return;
      }
      const title = titleOf(info) ?? sessionID;
      rememberSession(
        sessionID,
        title,
        directoryOf(info),
        Number(info?.time?.created ?? Date.now()),
      );
      await setBinding(chatKey, sessionID, title, directoryOf(info));
      audit(`cmd chat=${chatId} cmd=bind session=${sessionID}`);
      const turn = liveTurns.get(sessionID);
      const orphan = !turn && turnRecords[sessionID];
      await replyTo(
        chatId,
        [
          `Bound to ${title}`,
          `Session: ${shortId(sessionID)}   Project: ${projectLabel(directoryOf(info))}`,
          turn ? `A turn is already running (${fmtDuration(Date.now() - turn.startedAt)}).` : "",
          orphan
            ? `[warn] A remote turn was in flight when the plugin reloaded - its result is unknown.`
            : "",
        ]
          .filter((line) => line !== "")
          .join("\n"),
      );
    }

    async function handleNew(
      chatId: number,
      userId: number,
      chatKey: string,
      title: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/new title=${title === "" ? "<auto>" : title}`);
      let info: any;
      try {
        info = await callSessions<any>("create", title === "" ? {} : { title });
      } catch (error) {
        const message = errorText(error);
        audit(`err op=session.create error=${JSON.stringify(message)}`);
        await replyTo(chatId, `Could not create a session: ${message}`);
        return;
      }
      const sessionID = sessionIdOf(info) ?? String(info?.id ?? "");
      if (sessionID === "") {
        await replyTo(chatId, "Session created but the server returned no id.");
        return;
      }
      await bindTo(chatId, chatKey, sessionID);
    }

    async function handleStatus(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/status`);
      const binding = bindingFor(chatKey);
      const settingsLine = `Mode: ${settings.mode}   allowExec: ${settings.allowExec}`;
      if (!binding) {
        await replyTo(chatId, `No session bound.\n${settingsLine}\n\nSend /tasks to pick one.`);
        return;
      }
      const info = await fetchSession(binding.sessionID);
      const lines = [
        `Session: ${titleOf(info) ?? binding.label}`,
        `Id: ${shortId(binding.sessionID)}`,
        `Project: ${projectLabel(directoryOf(info) ?? binding.projectDir)}`,
        `Bound: ${fmtDuration(Date.now() - binding.lastUsed)} ago`,
        settingsLine,
      ];
      const turn = liveTurns.get(binding.sessionID);
      if (turn) {
        lines.push(`Turn: running for ${fmtDuration(Date.now() - turn.startedAt)} (heartbeats ${turn.heartbeats}/${HEARTBEAT_MAX})`);
      } else if (turnRecords[binding.sessionID]) {
        lines.push(`Turn: [warn] orphaned by a plugin reload - the result is unknown.`);
      } else {
        lines.push("Turn: idle");
      }
      const queuedCount = queueDepth(binding.sessionID);
      if (queuedCount > 0) lines.push(`Queued prompts: ${queuedCount} waiting (see /queue).`);
      const pendingCount = [...pendingPermissions.values()].filter(
        (p) => p.sessionID === binding.sessionID,
      ).length;
      lines.push(`Pending permissions: ${pendingCount}`);
      const files = Number(info?.summary?.files ?? 0);
      if (files > 0) lines.push(`Files changed (session total): ${files}`);
      await replyTo(chatId, lines.join("\n"));
    }

    async function handleStop(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/stop`);
      const binding = bindingFor(chatKey);
      if (!binding) {
        await replyTo(chatId, "No session bound.");
        return;
      }
      try {
        await callSessions("interrupt", { sessionID: binding.sessionID });
      } catch (error) {
        const message = errorText(error);
        audit(`err op=session.interrupt session=${binding.sessionID} error=${JSON.stringify(message)}`);
        await replyTo(chatId, `Could not interrupt: ${message}`);
        return;
      }
      const turn = liveTurns.get(binding.sessionID);
      if (turn) {
        const dropped = queueDepth(binding.sessionID);
        promptQueues.delete(binding.sessionID);
        await persistQueues();
        await finishTurn(turn, "interrupted");
        await replyTo(chatId, dropped > 0 ? `Interrupted. ${dropped} queued prompt(s) dropped - send again if still wanted.` : "Interrupted.");
        return;
      }
      await replyTo(chatId, "Interrupt sent.");
    }

    async function handleDiff(
      chatId: number,
      userId: number,
      chatKey: string,
      fileFilter: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/diff file=${fileFilter === "" ? "<all>" : fileFilter}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      let diffs: any[];
      try {
        diffs = await callSessions<any[]>("diff", { sessionID: binding.sessionID });
      } catch (error) {
        const message = errorText(error);
        audit(`err op=session.diff session=${binding.sessionID} error=${JSON.stringify(message)}`);
        await replyTo(chatId, "Diff is not accessible from here.");
        return;
      }
      const list = Array.isArray(diffs) ? diffs : [];
      if (list.length === 0) {
        await replyTo(chatId, "No changes recorded for this session.");
        return;
      }
      if (fileFilter === "") {
        const ordered = [...list].sort(
          (a: any, b: any) =>
            Number(b?.additions ?? 0) + Number(b?.deletions ?? 0) -
            (Number(a?.additions ?? 0) + Number(a?.deletions ?? 0)),
        );
        const summary = ordered
          .slice(0, 40)
          .map(
            (d: any) =>
              `+${Number(d?.additions ?? 0)}/-${Number(d?.deletions ?? 0)}  ${String(d?.file ?? "?")}`,
          );
        const totals = list.reduce(
          (acc: { a: number; d: number }, d: any) => ({
            a: acc.a + Number(d?.additions ?? 0),
            d: acc.d + Number(d?.deletions ?? 0),
          }),
          { a: 0, d: 0 },
        );
        await replyTo(
          chatId,
          `${list.length} file(s) changed, +${totals.a}/-${totals.d}\n${summary.join("\n")}\n\n` +
            "Send /diff <file> for the current content of one file.",
        );
        return;
      }
      const needle = fileFilter.toLowerCase();
      const matches = list.filter((d: any) =>
        String(d?.file ?? "").toLowerCase().includes(needle),
      );
      if (matches.length === 0) {
        await replyTo(chatId, `No changed file matches "${fileFilter}".`);
        return;
      }
      const sections = matches.slice(0, 3).map((d: any) => {
        // FileDiff.Info carries a unified `patch`, not file content. Keep phone
        // output readable by showing hunk headers plus the first changed lines.
        const patch = typeof d?.patch === "string" ? d.patch : "";
        const lines = patch.split("\n");
        const shown = lines.slice(0, 80).join("\n");
        const preview =
          shown.length > 2500
            ? `${shown.slice(0, 2500)}\n... (truncated)`
            : lines.length > 80
              ? `${shown}\n... (truncated, ${lines.length - 80} more lines)`
              : shown;
        return [
          `${String(d?.file ?? "?")}  +${Number(d?.additions ?? 0)}/-${Number(d?.deletions ?? 0)}  ${String(d?.status ?? "")}`.trim(),
          preview === "" ? "(no patch text)" : preview,
        ].join("\n");
      });
      await replyTo(chatId, sections.join("\n\n---\n\n"));
    }

    async function handleRead(
      chatId: number,
      userId: number,
      chatKey: string,
      countText: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/read n=${countText === "" ? "1" : countText}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const parsed = Number.parseInt(countText, 10);
      const count = Number.isNaN(parsed) ? 1 : Math.min(Math.max(parsed, 1), 5);
      const texts = await lastAssistantTexts(binding.sessionID, count);
      if (texts.length === 0) {
        await replyTo(chatId, "No assistant text found in the recent messages.");
        return;
      }
      await replyTo(
        chatId,
        texts
          .map((text, index) => `[${index + 1}/${texts.length}]\n${text}`)
          .join("\n\n"),
      );
    }

    async function handleMode(
      chatId: number,
      userId: number,
      chatKey: string,
      argument: string,
    ): Promise<void> {
      const requested = (argument.trim().toLowerCase().split(/\s+/)[0] ?? "");
      audit(`cmd chat=${chatId} user=${userId} cmd=/mode arg=${requested === "" ? "<show>" : requested}`);
      if (requested === "") {
        await replyTo(
          chatId,
          `Mode: ${settings.mode}\nallowExec: ${settings.allowExec}\n` +
            (settings.mode === "control"
              ? "Free text is sent to the bound session as a prompt."
              : "Free text is ignored until /mode control or /mode on."),
        );
        return;
      }
      if (requested === "notify" || requested === "off" || requested === "disable") {
        settings.mode = "notify";
        await persist(KEY_SETTINGS, settings);
        audit(`cmd chat=${chatId} user=${userId} cmd=/mode set=notify`);
        await replyTo(chatId, "Mode set to notify. Remote prompts are disabled; notifications continue.");
        return;
      }
      if (requested === "stop") {
        settings.mode = "notify";
        await persist(KEY_SETTINGS, settings);
        await clearBinding(chatKey);
        audit(`cmd chat=${chatId} user=${userId} cmd=/mode set=stop-unbound`);
        await replyTo(
          chatId,
          "Mode set to notify and this chat was unbound from its task/project. " +
            "Notifications continue; send /use to bind again, then /mode on.",
        );
        return;
      }
      if (requested !== "control" && requested !== "on" && requested !== "enable") {
        await replyTo(chatId, "Usage: /mode notify|control (or /mode off|on|stop).");
        return;
      }
      settings.mode = "control";
      await persist(KEY_SETTINGS, settings);
      await replyTo(
        chatId,
        "Mode set to control. Free text is sent to the bound session as a prompt.\n" +
          "allowExec is false: exec-type permission requests are denied automatically.",
      );
      audit(`cmd chat=${chatId} user=${userId} cmd=/mode set=control`);
    }

    async function handleWhoami(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/whoami`);
      const binding = bindingFor(chatKey);
      await replyTo(
        chatId,
        [
          `Telegram id: ${userId}`,
          `Chat id: ${chatId}`,
          `State key: ${chatKey}`,
          `Mode: ${settings.mode}`,
          `Binding: ${binding ? `${binding.label} (${shortId(binding.sessionID)})` : "none"}`,
          `allowExec: ${settings.allowExec}`,
          `Prompts today: ${dailyCounters.get(chatKey)?.day === today() ? dailyCounters.get(chatKey)?.count : 0}` +
            (settings.dailyPromptCap > 0 ? `/${settings.dailyPromptCap}` : ""),
          `Storage: ${storeKind}`,
        ].join("\n"),
      );
    }

    async function handleCancel(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/cancel`);
      const binding = bindingFor(chatKey);
      let dropped = 0;
      if (binding) {
        dropped += queueDepth(binding.sessionID);
        promptQueues.delete(binding.sessionID);
        await persistQueues();
      }
      for (const [token, action] of [...pendingActions.entries()]) {
        if (action.chatKey === chatKey) {
          pendingActions.delete(token);
          dropped += 1;
        }
      }
      await replyTo(
        chatId,
        dropped === 0
          ? "Nothing pending to cancel."
          : `Cleared ${dropped} pending item(s). A queued prompt is dropped, not sent.\n` +
            "Permission requests are answered with /perm <id> no or the Deny button - they expire to deny.",
      );
    }

    // --- server watchdog ----------------------------------------------------
    // If the OpenCode server stops or restarts, remote control is meaningless:
    // prompts cannot run and completions will never arrive. The owner isolate
    // probes the server on an interval; on a confirmed outage it drops to
    // notify mode, unbinds every chat, and says so on both bots. On recovery
    // it announces that too - mode stays notify until the owner re-enables it.
    // (Setup scope: every helper below is used by activateOwner/cleanup, which
    // live outside the per-message router.)

    const WATCHDOG_MS = 20_000;
    const WATCHDOG_DOWN_AFTER = 3;
    let serverUp: boolean | undefined;
    let serverFailures = 0;
    let watchdog: ReturnType<typeof setInterval> | undefined;

    async function onServerDown(): Promise<void> {
      // Live turns can never complete now: stop their timers instead of
      // leaving them to heartbeat against a dead server.
      for (const turn of liveTurns.values()) {
        if (turn.timer) clearInterval(turn.timer);
        liveTurns.delete(turn.sessionID);
      }
      turnRecords = {};
      await persist(KEY_TURNS, turnRecords);
      const droppedQueues = [...promptQueues.values()].reduce((n, q) => n + q.length, 0);
      promptQueues.clear();
      await persistQueues();
      bindings = {};
      await persist(KEY_BINDINGS, bindings);
      settings.mode = "notify";
      await persist(KEY_SETTINGS, settings);
      audit(`server-down queues-dropped=${droppedQueues}`);
      const text =
        "OpenCode server unreachable - it may have stopped or is restarting. " +
        "Mode set to notify and this chat was unbound from its task/project." +
        (droppedQueues > 0 ? ` ${droppedQueues} queued prompt(s) dropped.` : "") +
        " After the server is back, send /use to bind again, then /mode on.";
      if (activeChatId !== null) {
        await sendToChat(activeChatId, text);
        await sendToChat(activeChatId, text, undefined, "notify");
      } else {
        audit("dropped no-active-chat text=server unreachable");
      }
    }

    async function onServerUp(): Promise<void> {
      audit("server-up");
      const text =
        "OpenCode server is reachable again. Mode is notify (safe default). " +
        "Send /use to bind a task/project, then /mode on to resume remote control.";
      if (activeChatId !== null) {
        await sendToChat(activeChatId, text);
        await sendToChat(activeChatId, text, undefined, "notify");
      } else {
        audit("dropped no-active-chat text=server reachable");
      }
    }

    async function checkServer(): Promise<void> {
      if (!crossOwner) return;
      try {
        await apiGet("/api/session/active");
        serverFailures = 0;
        if (serverUp === false) {
          serverUp = true;
          await onServerUp();
        } else {
          serverUp = true;
        }
      } catch {
        serverFailures += 1;
        if (serverFailures >= WATCHDOG_DOWN_AFTER && serverUp !== false) {
          serverUp = false;
          await onServerDown();
        }
      }
    }

    function startWatchdog(): void {
      if (watchdog !== undefined) return;
      watchdog = setInterval(() => {
        void checkServer();
      }, WATCHDOG_MS);
      void checkServer();
    }

    function stopWatchdog(): void {
      if (watchdog !== undefined) {
        clearInterval(watchdog);
        watchdog = undefined;
      }
    }

    async function route(
      identity: { userId: number; chatId: number; chatKey: string },
      text: string,
    ): Promise<void> {
      const { userId, chatId, chatKey } = identity;
      if (!text.startsWith("/")) {
        // Free text is a prompt only when bound; otherwise offer one-tap bind.
        if (!bindingFor(chatKey)) {
          let rows: TaskRow[] = [];
          try {
            rows = (await listTasks(chatKey, "")).slice(0, 5);
          } catch {
            /* fall through to the plain message */
          }
          if (rows.length === 0) {
            await replyTo(chatId, "No session bound, so that text was not interpreted. Send /tasks to pick one.");
            return;
          }
          await replyTo(
            chatId,
            "Not bound to a session yet - tap one to bind, then send your message again.",
            {
              inline_keyboard: rows.map((row) => [
                button(`${row.title.slice(0, 36)}`, {
                  kind: "bind",
                  chatKey,
                  chatId,
                  sessionID: row.id,
                }),
              ]),
            },
          );
          return;
        }
        await handlePrompt(chatId, userId, chatKey, text);
        return;
      }

      const parts = text.slice(1).split(/\s+/);
      const name = (parts[0] ?? "").toLowerCase();
      const rest = text.slice(1 + name.length).trim();

      if (!commandAllowed(name)) {
        await replyTo(chatId, `/${name} is not permitted by TELEGRAM_ALLOWED_COMMANDS.`);
        return;
      }

      // --- Tier 1 controls ----------------------------------------------------

    // ctx.model.list / ctx.agent.list / ctx.command.list / ctx.skill.list are
    // documented but were NOT runtime-confirmed on this build, so every call is
    // wrapped: on failure the user gets a clear message and free-text fallback
    // instead of a hang or a crash.
    async function listModels(): Promise<any[]> {
      const raw = unwrap<any>(await ctx.model.list(), "model.list");
      if (Array.isArray(raw)) return raw;
      return Array.isArray(raw?.data) ? raw.data : [];
    }

    async function listAgents(): Promise<any[]> {
      const raw = unwrap<any>(await ctx.agent.list(), "agent.list");
      if (Array.isArray(raw)) return raw;
      return Array.isArray(raw?.data) ? raw.data : [];
    }

    async function handleModel(
      chatId: number,
      userId: number,
      chatKey: string,
      arg: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/model arg=${arg === "" ? "<list>" : arg}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const want = arg.trim();
      if (want !== "") {
        const slash = want.indexOf("/");
        if (slash <= 0) {
          await replyTo(chatId, "Use /model <providerID>/<modelID>, e.g. /model opencode/gpt-5");
          return;
        }
        const providerID = want.slice(0, slash);
        const modelID = want.slice(slash + 1);
        try {
          await callSessions("switchModel", {
            sessionID: binding.sessionID,
            model: { providerID, id: modelID },
          });
          audit(`cmd chat=${chatId} model=${providerID}/${modelID} result=ok`);
          await replyTo(chatId, `Model set to ${providerID}/${modelID}.`);
        } catch (error) {
          const message = errorText(error);
          audit(`err op=switchModel error=${JSON.stringify(message)}`);
          await replyTo(chatId, `Could not switch model: ${message}`);
        }
        return;
      }
      let models: any[];
      try {
        models = await listModels();
      } catch (error) {
        await replyTo(
          chatId,
          `Model list is unavailable on this build (${errorText(error)}). ` +
            "You can still switch with /model <providerID>/<modelID>.",
        );
        return;
      }
      if (models.length === 0) {
        await replyTo(chatId, "No models reported by this server.");
        return;
      }
      const rows: any[][] = [];
      for (const m of models.slice(0, 12)) {
        const providerID = String(m?.providerID ?? m?.provider ?? "");
        const id = String(m?.id ?? "");
        if (providerID === "" || id === "") continue;
        const name = String(m?.name ?? id);
        rows.push([
          button(`${name}`.slice(0, 40), {
            kind: "model",
            chatKey,
            chatId,
            sessionID: binding.sessionID,
            data: `${providerID}/${id}`,
          }),
        ]);
      }
      await replyTo(
        chatId,
        `Models (${models.length}):\n` +
          models
            .slice(0, 12)
            .map((m) => `- ${m?.name ?? m?.id}  [${m?.providerID ?? "?"}]`)
            .join("\n"),
        rows.length > 0 ? { inline_keyboard: rows } : undefined,
      );
    }

    async function handleAgent(
      chatId: number,
      userId: number,
      chatKey: string,
      arg: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/agent arg=${arg === "" ? "<list>" : arg}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const want = arg.trim();
      if (want !== "") {
        try {
          await callSessions("switchAgent", { sessionID: binding.sessionID, agent: want });
          audit(`cmd chat=${chatId} agent=${want} result=ok`);
          await replyTo(chatId, `Agent set to ${want}.`);
        } catch (error) {
          await replyTo(chatId, `Could not switch agent: ${errorText(error)}`);
        }
        return;
      }
      let agents: any[];
      try {
        agents = await listAgents();
      } catch (error) {
        await replyTo(
          chatId,
          `Agent list is unavailable on this build (${errorText(error)}). ` +
            "You can still switch with /agent <name>.",
        );
        return;
      }
      const rows = agents
        .filter((a) => typeof a?.name === "string")
        .slice(0, 12)
        .map((a) => [
          button(String(a.name).slice(0, 40), {
            kind: "agent",
            chatKey,
            chatId,
            sessionID: binding.sessionID,
            data: String(a.name),
          }),
        ]);
      await replyTo(
        chatId,
        `Agents:\n${agents
          .slice(0, 12)
          .map((a) => `- ${a?.name ?? a?.id}`)
          .join("\n")}`,
        rows.length > 0 ? { inline_keyboard: rows } : undefined,
      );
    }

    async function handleContext(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/context`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      try {
        // session.get carries tokens and cost; session.list does not carry title
        // but does carry tokens/cost too, and get is authoritative.
        const info = await callSessions<any>("get", { sessionID: binding.sessionID });
        const tokens = info?.tokens ?? {};
        const cost = Number(info?.cost ?? 0);
        const model = info?.model
          ? `${info.model.providerID ?? "?"}/${info.model.id ?? "?"}`
          : "unknown";
        const input = Number(tokens.input ?? 0);
        const output = Number(tokens.output ?? 0);
        const cacheRead = Number(tokens?.cache?.read ?? 0);
        await replyTo(
          chatId,
          [
            `Context for ${shortId(binding.sessionID)}`,
            `Model: ${model}`,
            `Input tokens: ${input}`,
            `Output tokens: ${output}`,
            `Cache read: ${cacheRead}`,
            `Cost: $${cost.toFixed(4)}`,
          ].join("\n"),
        );
      } catch (error) {
        await replyTo(chatId, `Could not read context: ${errorText(error)}`);
      }
    }

    async function handleCompact(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/compact`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      if (liveTurns.has(binding.sessionID)) {
        await replyTo(chatId, "The agent is still working on this session. Try again when it finishes.");
        return;
      }
      try {
        await callSessions("compact", { sessionID: binding.sessionID });
        audit(`cmd chat=${chatId} compact=${binding.sessionID} result=ok`);
        await replyTo(
          chatId,
          "Compaction requested. It runs as a background step; the summary lands in the session.",
        );
      } catch (error) {
        await replyTo(chatId, `Could not compact: ${errorText(error)}`);
      }
    }

    async function handleRename(
      chatId: number,
      userId: number,
      chatKey: string,
      title: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/rename`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const clean = title.trim();
      if (clean === "") {
        await replyTo(chatId, "Usage: /rename <new title>");
        return;
      }
      try {
        await callSessions("update", { sessionID: binding.sessionID, title: clean });
        titleCache.set(binding.sessionID, clean);
        audit(`cmd chat=${chatId} rename=${binding.sessionID} result=ok`);
        await replyTo(chatId, `Renamed to "${clean}".`);
      } catch (error) {
        await replyTo(chatId, `Could not rename: ${errorText(error)}`);
      }
    }

    async function handleForget(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/forget stage=confirm`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const label = binding.label || shortId(binding.sessionID);
      // Two-step: nothing is deleted until the confirm button is pressed.
      await replyTo(
        chatId,
        `Delete session "${label}" (${shortId(binding.sessionID)})?\nThis cannot be undone.`,
        {
          inline_keyboard: [
            [
              button("Confirm delete", {
                kind: "forget-confirm",
                chatKey,
                chatId,
                sessionID: binding.sessionID,
              }),
              button("Cancel", {
                kind: "forget-cancel",
                chatKey,
                chatId,
                sessionID: binding.sessionID,
              }),
            ],
          ],
        },
      );
    }

    async function handleCommandMenu(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/commands`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      let commands: any[];
      try {
        const raw = unwrap<any>(await ctx.command.list(), "command.list");
        commands = Array.isArray(raw) ? raw : Array.isArray(raw?.data) ? raw.data : [];
      } catch (error) {
        await replyTo(chatId, `Command list is unavailable on this build (${errorText(error)}).`);
        return;
      }
      const usable = commands.filter((c) => typeof c?.name === "string");
      if (usable.length === 0) {
        await replyTo(chatId, "No custom commands are registered.");
        return;
      }
      const rows = usable.slice(0, 12).map((c) => [
        button(`${c.description ? "- " : ""}${String(c.name).slice(0, 36)}`, {
          kind: "run-command",
          chatKey,
          chatId,
          sessionID: binding.sessionID,
          data: String(c.name),
        }),
      ]);
      await replyTo(
        chatId,
        `Commands:\n${usable
          .slice(0, 12)
          .map((c) => `- /${c.name}${c.description ? `: ${c.description}` : ""}`)
          .join("\n")}`,
        { inline_keyboard: rows },
      );
    }

    async function handleSkillMenu(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/skills`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      let skills: any[];
      try {
        const raw = unwrap<any>(await ctx.skill.list(), "skill.list");
        skills = Array.isArray(raw) ? raw : Array.isArray(raw?.data) ? raw.data : [];
      } catch (error) {
        await replyTo(chatId, `Skill list is unavailable on this build (${errorText(error)}).`);
        return;
      }
      const usable = skills.filter((s) => typeof s?.name === "string");
      if (usable.length === 0) {
        await replyTo(chatId, "No skills are registered.");
        return;
      }
      const rows = usable.slice(0, 12).map((s) => [
        button(String(s.name).slice(0, 36), {
          kind: "run-skill",
          chatKey,
          chatId,
          sessionID: binding.sessionID,
          data: String(s.id ?? s.name),
        }),
      ]);
      await replyTo(
        chatId,
        `Skills:\n${usable
          .slice(0, 12)
          .map((s) => `- ${s.name}`)
          .join("\n")}\n\nTap one to run it in the bound session.`,
        { inline_keyboard: rows },
      );
    }

    // --- Tier 2: steer, live status, worktree, file browser ------------------

    // Directory the /ls browser is confined to: the bound session's project
    // root. Telegram must never be able to walk the whole filesystem.
    async function rootForSession(sessionID: string): Promise<string | undefined> {
      try {
        const info = await callSessions<any>("get", { sessionID });
        const dir = info?.location?.directory ?? info?.directory;
        return typeof dir === "string" && dir !== "" ? dir : undefined;
      } catch {
        return undefined;
      }
    }

    // Resolve a user-supplied relative path inside root, refusing anything that
    // escapes it via .. or an absolute path - and via symlink/junction, which
    // the lexical check cannot see (a link inside the root can point anywhere).
    function safeJoin(root: string, candidate: string): string | undefined {
      const target = resolve(root, candidate === "" ? "." : candidate);
      const rel = relative(root, target);
      if (rel.startsWith("..") || resolve(root, rel) !== target) return undefined;
      let realRoot: string;
      try {
        realRoot = realpathSync(root);
      } catch {
        return undefined;
      }
      // Check the target itself; if it does not exist yet, check its parent
      // directory instead (a dangling path cannot be read through anyway - the
      // call site's stat/readdir fails safely). If neither resolves, allow the
      // lexical result and let the call site fail safe.
      for (const p of [target, dirname(target)]) {
        let real: string | undefined;
        try {
          real = realpathSync(p);
        } catch {
          continue;
        }
        const realRel = relative(realRoot, real);
        if (realRel.startsWith("..") || resolve(realRoot, realRel) !== real) return undefined;
        return target;
      }
      return target;
    }

    async function handleLs(
      chatId: number,
      userId: number,
      chatKey: string,
      subPath: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/ls path=${subPath === "" ? "." : subPath}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const root = await rootForSession(binding.sessionID);
      if (root === undefined) {
        await replyTo(chatId, "Could not determine this session's project directory.");
        return;
      }
      const target = safeJoin(root, subPath.trim());
      if (target === undefined) {
        await replyTo(chatId, "Path is outside the project directory.");
        return;
      }
      let entries: string[];
      try {
        entries = readdirSync(target);
      } catch (error) {
        await replyTo(chatId, `Cannot read that directory: ${errorText(error)}`);
        return;
      }
      const rows: Array<{ name: string; dir: boolean; size: number }> = [];
      for (const name of entries.slice(0, 300)) {
        if (name === "node_modules" || name === ".git") continue;
        try {
          const st = statSync(join(target, name));
          rows.push({ name, dir: st.isDirectory(), size: st.size });
        } catch {
          /* unreadable entry: skip */
        }
      }
      rows.sort((a, b) => (a.dir === b.dir ? a.name.localeCompare(b.name) : a.dir ? -1 : 1));
      const shown = rows.slice(0, 25);
      const rel = relative(root, target) || ".";
      const buttons = shown.slice(0, 8).map((r) => [
        button(`${r.dir ? "[D] " : "[F] "}${r.name}`.slice(0, 40), {
          kind: "ls-open",
          chatKey,
          chatId,
          sessionID: binding.sessionID,
          data: rel === "." ? r.name : `${rel}${sep}${r.name}`,
        }),
      ]);
      if (rel !== ".") {
        const parent = dirname(rel) === "." ? "" : dirname(rel);
        buttons.unshift([
          button("[up] Up", {
            kind: "ls-open",
            chatKey,
            chatId,
            sessionID: binding.sessionID,
            data: parent,
          }),
        ]);
      }
      await replyTo(
        chatId,
        `[dir] ${rel}
` +
          shown
            .map((r) => `${r.dir ? "[D] " : "[F] "}${r.name}${r.dir ? "" : `  ${r.size}B`}`)
            .join("\n"),
        buttons.length > 0 ? { inline_keyboard: buttons } : undefined,
      );
    }

    async function handleWorktree(
      chatId: number,
      userId: number,
      chatKey: string,
      arg: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/worktree arg=${arg === "" ? "<list>" : arg}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const want = arg.trim();
      let list: any[];
      try {
        const raw = unwrap<any>(await ctx.worktree.list({ projectID: ctx.location?.project?.id }), "worktree.list");
        list = Array.isArray(raw) ? raw : Array.isArray(raw?.data) ? raw.data : [];
      } catch (error) {
        await replyTo(chatId, `Worktree list is unavailable on this build (${errorText(error)}).`);
        return;
      }
      if (want !== "") {
        const target = list.find((w) => String(w?.directory ?? "").toLowerCase() === want.toLowerCase());
        if (target === undefined) {
          await replyTo(chatId, `No worktree matches "${want}". Use /worktree to list them.`);
          return;
        }
        try {
          await callSessions("move", { sessionID: binding.sessionID, directory: target.directory });
          audit(`cmd chat=${chatId} worktree=${target.directory} result=ok`);
          await replyTo(chatId, `Moved this session to ${target.directory}.`);
        } catch (error) {
          await replyTo(chatId, `Could not switch worktree: ${errorText(error)}`);
        }
        return;
      }
      if (list.length === 0) {
        await replyTo(chatId, "No worktrees registered for this project.");
        return;
      }
      await replyTo(
        chatId,
        `Worktrees:\n${list
          .slice(0, 12)
          .map((w) => `- ${w?.directory ?? w?.name}`)
          .join("\n")}\n\nSwitch with /worktree <directory>.`,
      );
    }

    // Live status: one editable message per chat, refreshed on demand with
    // /live. Kept deliberately simple (edit-in-place) rather than a full
    // background updater, so it cannot spam the chat.
    const liveStatusMessage = new Map<string, number>();

    async function handleLive(
      chatId: number,
      userId: number,
      chatKey: string,
      arg: string,
    ): Promise<void> {
      const off = arg.trim() === "off";
      audit(`cmd chat=${chatId} user=${userId} cmd=/live arg=${off ? "off" : "on"}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      if (off) {
        liveStatusMessage.delete(chatKey);
        await replyTo(chatId, "Live status off.");
        return;
      }
      const info = await callSessions<any>("get", { sessionID: binding.sessionID }).catch(() => undefined);
      const turn = liveTurns.get(binding.sessionID);
      const tokens = info?.tokens ?? {};
      const body = [
        `* ${binding.label || shortId(binding.sessionID)}`,
        `Project: ${projectLabel(await rootForSession(binding.sessionID))}`,
        `Model: ${info?.model ? `${info.model.providerID}/${info.model.id}` : "unknown"}`,
        `State: ${turn ? "running" : "idle"}`,
        `Tokens: in ${tokens.input ?? 0} / out ${tokens.output ?? 0}`,
        `Cost: $${Number(info?.cost ?? 0).toFixed(4)}`,
      ].join("\n");
      const previous = liveStatusMessage.get(chatKey);
      if (previous !== undefined) {
        try {
          await editSent(chatId, previous, body);
          return;
        } catch {
          liveStatusMessage.delete(chatKey);
        }
      }
      // replyTo discards the result, so send directly to learn the message id.
      try {
        const sent: any = await sendToChat(chatId, body);
        const id = Number(sent?.messageId ?? 0);
        if (sent?.ok === true && id > 0) liveStatusMessage.set(chatKey, id);
      } catch (error) {
        await replyTo(chatId, `Could not post status: ${errorText(error)}`);
      }
    }

    async function handleSkill(
      chatId: number,
      userId: number,
      chatKey: string,
      name: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/skill arg=${name === "" ? "<list>" : name}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      let skills: any[];
      try {
        const raw = unwrap<any>(await ctx.skill.list(), "skill.list");
        skills = Array.isArray(raw) ? raw : Array.isArray(raw?.data) ? raw.data : [];
      } catch (error) {
        await replyTo(chatId, `Skill list is unavailable on this build (${errorText(error)}).`);
        return;
      }
      const want = name.trim();
      if (want === "") {
        await handleSkillMenu(chatId, userId, chatKey);
        return;
      }
      const match =
        skills.find((s) => String(s?.id) === want) ??
        skills.find((s) => String(s?.name).toLowerCase() === want.toLowerCase());
      if (match === undefined) {
        await replyTo(chatId, `No skill named "${want}". Use /skills to list them.`);
        return;
      }
      try {
        // Attach properly rather than shelling out to session.command: the
        // prompt API accepts a skills[] attachment. The cached OpenAPI schema
        // allows only {id, mention}, so do not send the display name.
        const skillId = String(match?.id ?? match?.name ?? "").trim();
        if (skillId === "") {
          await replyTo(chatId, `Skill "${want}" has no usable id.`);
          return;
        }
        await callSessions("prompt", {
          sessionID: binding.sessionID,
          text: "",
          skills: [{ id: skillId }],
        });
        audit(`cmd chat=${chatId} skill=${skillId} result=ok`);
        await replyTo(chatId, `Started skill ${match?.name ?? skillId}.`);
      } catch (error) {
        await replyTo(chatId, `Could not start skill: ${errorText(error)}`);
      }
    }

    // Steer vs queue: when a turn is running, "steer" injects the message into
    // it (Session.Inbox.Delivery = steer|queue). Falls back to the single-slot
    // queue if the server rejects it.
    async function handleSteer(
      chatId: number,
      userId: number,
      chatKey: string,
      arg: string,
    ): Promise<void> {
      const want = arg.trim().toLowerCase();
      audit(`cmd chat=${chatId} user=${userId} cmd=/steer arg=${want === "" ? "<toggle>" : want}`);
      if (want !== "on" && want !== "off") {
        await replyTo(chatId, `Steer is ${settings.steerBusyTurns ? "on" : "off"}. Use /steer on|off.`);
        return;
      }
      settings.steerBusyTurns = want === "on";
      await persist(KEY_SETTINGS, settings);
      await replyTo(chatId, `Steer ${want}. New messages will be ${want === "on" ? "injected into the running turn" : "queued"}.`);
    }

    // Publish the file-browser handler for the ls-open callback path.
    lsNav = handleLs;
    sessionRoot = rootForSession;

    // --- Tier 3 handlers ----------------------------------------------------

    async function handleAttach(
      chatId: number,
      userId: number,
      chatKey: string,
      subPath: string,
    ): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/attach path=${subPath === "" ? "<none>" : subPath}`);
      const binding = await requireBinding(chatId, chatKey);
      if (!binding) return;
      const want = subPath.trim();
      if (want === "") {
        const staged = stagedFiles.get(chatKey) ?? [];
        await replyTo(
          chatId,
          staged.length > 0
            ? `Staged for your next message:\n${staged.map((f) => `- ${f.name}`).join("\n")}\n\n` +
                "Send your message to send them, or /detach to clear."
            : "Nothing staged. Use /attach <path-in-project>, or just send a " +
              "document/photo in Telegram and it is attached to your next message.",
        );
        return;
      }
      const root = await rootForSession(binding.sessionID);
      if (root === undefined) {
        await replyTo(chatId, "Could not determine this session's project directory.");
        return;
      }
      const target = safeJoin(root, want);
      if (target === undefined) {
        await replyTo(chatId, "Path is outside the project directory.");
        return;
      }
      try {
        const st = statSync(target);
        if (!st.isFile()) {
          await replyTo(chatId, "That is a directory. Use /ls to browse it.");
          return;
        }
        if (st.size > 20 * 1024 * 1024) {
          await replyTo(chatId, `Too large to attach (${st.size} bytes).`);
          return;
        }
      } catch {
        await replyTo(chatId, "No such file in this project.");
        return;
      }
      const name = basename(target);
      stageFiles(chatKey, [{ uri: toFileUri(target), name }]);
      audit(`attach-staged chat=${chatId} name=${name}`);
      await replyTo(chatId, `[ok] ${name} staged. It goes with your next message.`);
    }

    async function handleDetach(
      chatId: number,
      userId: number,
      chatKey: string,
    ): Promise<void> {
      const had = (stagedFiles.get(chatKey) ?? []).length;
      stagedFiles.delete(chatKey);
      audit(`cmd chat=${chatId} user=${userId} cmd=/detach cleared=${had}`);
      await replyTo(chatId, had > 0 ? `Cleared ${had} staged file(s).` : "Nothing was staged.");
    }

    async function handleButtons(chatId: number, userId: number, arg: string): Promise<void> {
      const want = arg.trim().toLowerCase();
      audit(`cmd chat=${chatId} user=${userId} cmd=/buttons arg=${want === "" ? "<show>" : want}`);
      if (want === "") {
        await replyTo(
          chatId,
          `Finished-turn buttons: ${settings.finishedButtons === true ? "on" : "off"}\n` +
            "Buttons only work on this bot, so alerts always arrive here either way.",
        );
        return;
      }
      if (want !== "on" && want !== "off") {
        await replyTo(chatId, "Use /buttons on or /buttons off.");
        return;
      }
      settings.finishedButtons = want === "on";
      await persist(KEY_SETTINGS, settings);
      await replyTo(
        chatId,
        `Buttons ${want}. Finished-turn alerts arrive here in both cases.`,
      );
    }

    async function handleAlerts(chatId: number, userId: number, arg: string): Promise<void> {
      const parts = arg.trim().toLowerCase().split(/\s+/).filter((part) => part !== "");
      const current = {
        completed: alertEnabled("completed"),
        failed: alertEnabled("failed"),
        waiting: alertEnabled("waiting"),
        progress: alertEnabled("progress"),
      };
      audit(`cmd chat=${chatId} user=${userId} cmd=/alerts arg=${parts.join(" ") === "" ? "<show>" : parts.join(" ")}`);
      if (parts.length === 0) {
        await replyTo(
          chatId,
          `Alerts: completed=${current.completed ? "on" : "off"} failed=${current.failed ? "on" : "off"} ` +
            `waiting=${current.waiting ? "on" : "off"} progress=${current.progress ? "on" : "off"}\n` +
            "Completed alerts arrive here for bound sessions; the notify bot alerts every session.\n" +
            "Use /alerts <completed|failed|waiting|progress> on|off.",
        );
        return;
      }
      const [kind, value] = parts;
      if (
        (kind !== "completed" && kind !== "failed" && kind !== "waiting" && kind !== "progress") ||
        (value !== "on" && value !== "off")
      ) {
        await replyTo(chatId, "Usage: /alerts <completed|failed|waiting|progress> on|off.");
        return;
      }
      settings.alerts = { ...current, [kind]: value === "on" };
      await persist(KEY_SETTINGS, settings);
      await replyTo(chatId, `Alert ${kind} ${value}.`);
    }

    async function handleHealth(chatId: number, userId: number): Promise<void> {
      audit(`cmd chat=${chatId} user=${userId} cmd=/health`);
      let server = "unknown";
      try {
        server = await serverBaseUrl(false);
      } catch (error) {
        server = `unavailable (${errorText(error)})`;
      }
      let lock = "missing";
      try {
        const current = readLock();
        if (current !== undefined) {
          const age = Math.max(0, Math.round((Date.now() - current.ts) / 1000));
          lock = `${crossOwner ? "owner" : "standby"}, heartbeat ${age}s ago`;
        }
      } catch {
        lock = "unreadable";
      }
      await replyTo(
        chatId,
        [
          `Server: ${server}`,
          `Lock: ${lock}`,
          `Mode: ${settings.mode}   Alerts: completed=${alertEnabled("completed") ? "on" : "off"} failed=${alertEnabled("failed") ? "on" : "off"} waiting=${alertEnabled("waiting") ? "on" : "off"} progress=${alertEnabled("progress") ? "on" : "off"}`,
          `Live turns: ${liveTurns.size}   Queued prompts: ${[...promptQueues.values()].reduce((n, q) => n + q.length, 0)}`,
          `Pending permissions: ${pendingPermissions.size}   Pending buttons: ${pendingActions.size}`,
          `Storage: ${storeKind}`,
        ].join("\n"),
      );
    }

    switch (name) {
        case "help":
        case "start":
          audit(`cmd chat=${chatId} user=${userId} cmd=/${name}`);
          if (name === "start") {
            const existing = bindingFor(chatKey);
            if (existing) {
              await replyTo(
                chatId,
                `Bound to ${existing.label} (${shortId(existing.sessionID)}), mode ${settings.mode}.\n` +
                  `Just type to send a prompt, or /tasks to switch.`,
              );
            } else {
              await replyTo(
                chatId,
                "Not bound yet. Send /tasks, tap a session, then just type.",
              );
            }
            return;
          }
          await replyTo(chatId, HELP_TEXT);
          return;
        case "tasks":
          await handleTasks(chatId, userId, chatKey, rest);
          return;
        case "projects":
          await handleProjects(chatId, userId, chatKey, rest);
          return;
        case "active":
          await handleActive(chatId, userId, chatKey);
          return;
        case "refresh":
          await handleRefresh(chatId, userId, chatKey, rest);
          return;
        case "fork":
          await handleFork(chatId, userId, chatKey, rest);
          return;
        case "forms":
          await handleForms(chatId, userId, chatKey);
          return;
        case "answer":
          await handleAnswer(chatId, userId, chatKey, rest);
          return;
        case "new":
          await handleNew(chatId, userId, chatKey, rest);
          return;
        case "use":
          await handleUse(chatId, userId, chatKey, rest);
          return;
        case "unbind": {
          audit(`cmd chat=${chatId} user=${userId} cmd=/unbind`);
          const binding = bindingFor(chatKey);
          await clearBinding(chatKey);
          await replyTo(
            chatId,
            binding
              ? `Unbound ${binding.label}. Notifications still arrive.`
              : "Nothing was bound.",
          );
          return;
        }
        case "status":
          await handleStatus(chatId, userId, chatKey);
          return;
        case "stop":
          await handleStop(chatId, userId, chatKey);
          return;
        case "diff":
          await handleDiff(chatId, userId, chatKey, rest);
          return;
        case "read":
          await handleRead(chatId, userId, chatKey, rest);
          return;
        case "model":
          await handleModel(chatId, userId, chatKey, rest);
          return;
        case "agent":
          await handleAgent(chatId, userId, chatKey, rest);
          return;
        case "context":
          await handleContext(chatId, userId, chatKey);
          return;
        case "compact":
          await handleCompact(chatId, userId, chatKey);
          return;
        case "rename":
          await handleRename(chatId, userId, chatKey, rest);
          return;
        case "forget":
          await handleForget(chatId, userId, chatKey);
          return;
        case "revoke":
          audit(`cmd chat=${chatId} user=${userId} cmd=/revoke result=removed`);
          await replyTo(chatId, "PIN support has been removed.");
          return;
        case "commands":
          await handleCommandMenu(chatId, userId, chatKey);
          return;
        case "skills":
          await handleSkillMenu(chatId, userId, chatKey);
          return;
        case "skill":
          await handleSkill(chatId, userId, chatKey, rest);
          return;
        case "ls":
          await handleLs(chatId, userId, chatKey, rest);
          return;
        case "worktree":
          await handleWorktree(chatId, userId, chatKey, rest);
          return;
        case "live":
          await handleLive(chatId, userId, chatKey, rest);
          return;
        case "steer":
          await handleSteer(chatId, userId, chatKey, rest);
          return;
        case "buttons":
          await handleButtons(chatId, userId, rest);
          return;
        case "alerts":
          await handleAlerts(chatId, userId, rest);
          return;
        case "health":
          await handleHealth(chatId, userId);
          return;
        case "messages":
          await handleMessages(chatId, userId, chatKey, rest);
          return;
        case "task":
          await handleTask(chatId, userId, chatKey, rest);
          return;
        case "tasklist":
          await handleTaskList(chatId, userId, chatKey);
          return;
        case "queue":
          await handleQueue(chatId, userId, chatKey);
          return;
        case "btw":
          await handleBtw(chatId, userId, chatKey, rest);
          return;
        case "mcps":
          await handleMcps(chatId, userId, chatKey);
          return;
        case "attach":
          await handleAttach(chatId, userId, chatKey, rest);
          return;
        case "detach":
          await handleDetach(chatId, userId, chatKey);
          return;
        case "mode":
          await handleMode(chatId, userId, chatKey, rest);
          return;
        case "whoami":
          await handleWhoami(chatId, userId, chatKey);
          return;
        case "cancel":
          await handleCancel(chatId, userId, chatKey);
          return;
        case "pin":
          audit(`cmd chat=${chatId} user=${userId} cmd=/pin result=removed`);
          await replyTo(chatId, "PIN support has been removed.");
          return;
        case "perm":
          if (rest === "") {
            await handlePermList(chatId, userId, chatKey);
            return;
          }
          await handlePermAnswer(chatId, userId, chatKey, rest);
          return;
        default:
          // Every command must be explicitly matched; never interpret unknowns.
          audit(`cmd chat=${chatId} user=${userId} cmd=/${name} result=unknown`);
          await replyTo(chatId, `Unknown command /${name}.\n\n${HELP_TEXT}`);
      }
    }

    // --- bot wiring --------------------------------------------------------

    bot.use(async (tctx: any, next: any) => {
      // Every inbound message is a chance to converge: if the owner died
      // without cleanup, the serving isolate promotes itself right here.
      reconcileOwnership("inbound");
      const userId = tctx.from?.id as number | undefined;
      if (!userId || !config.allowed.includes(userId)) {
        audit(`rejected user=${userId ?? "unknown"} chat=${tctx.chat?.id ?? "unknown"}`);
        return;
      }
      if (tctx.chat?.type !== "private") return;

      const chatId = tctx.chat.id as number;
      activeChatId = chatId;
      if (blockedChats.delete(chatId)) {
        // The user can only message the bot again after unblocking it, so an
        // inbound message is proof the 403 is stale.
        audit(`send-resumed chat=${chatId}`);
      }
      if (!seenChats.has(chatId)) {
        seenChats.add(chatId);
        audit(`chat-connected chat=${chatId}`);
        const body =
          "*OpenCode notifications are active.*\n\n" +
          `Your Telegram ID: \`${userId}\`\n` +
          `Active chat_id: \`${activeChatId}\`\n` +
          `Known session titles: ${sessionTitles.size}\n\n` +
          "You will be notified in this chat when an agent finishes a task or " +
          "asks a question. Send /help for the remote-control commands.";
        try {
          await tctx.reply(body);
          audit(`replied chat=${chatId} to=${JSON.stringify("greeting")}`);
        } catch (error) {
          audit(`reply-FAILED chat=${chatId} error=${(error as Error).message}`);
        }
      }

      // Voice transcription via a Whisper-compatible endpoint
      // (OpenAI `/audio/transcriptions` shape). Multipart body is built by hand
      // so no DOM form types are needed. The key never appears in audit lines.
      async function transcribeVoice(bytes: Buffer, filename: string): Promise<string> {
        if (!config.whisperUrl || !config.whisperKey) {
          throw new Error("voice transcription is not configured");
        }
        const boundary = `opencode-${Date.now().toString(36)}-${randomBytes(4).toString("hex")}`;
        const head = Buffer.from(
          `--${boundary}\r\nContent-Disposition: form-data; name="model"\r\n\r\nwhisper-1\r\n` +
            `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${filename}"\r\n` +
            `Content-Type: audio/ogg\r\n\r\n`,
          "utf8",
        );
        const tail = Buffer.from(`\r\n--${boundary}--\r\n`, "utf8");
        const res = await fetch(`${config.whisperUrl.replace(/\/+$/, "")}/audio/transcriptions`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${config.whisperKey}`,
            "content-type": `multipart/form-data; boundary=${boundary}`,
          },
          body: Buffer.concat([head, bytes, tail]),
          signal: AbortSignal.timeout(60_000),
        });
        if (!res.ok) throw new Error(`transcription failed (HTTP ${res.status})`);
        const body: any = await res.json().catch(() => undefined);
        const text = typeof body?.text === "string" ? body.text.trim() : "";
        if (text === "") throw new Error("transcription returned no text");
        return text;
      }

      if (tctx.message?.voice || tctx.message?.audio) {
        const file: any = tctx.message.voice ?? tctx.message.audio;
        const chatKeyNow = chatKeyOf(chatId, userId);
        try {
          if (!config.whisperUrl || !config.whisperKey) {
            audit(`cmd chat=${chatId} user=${userId} cmd=voice result=unconfigured`);
            await replyTo(
              chatId,
              "[i] Voice transcription needs TELEGRAM_WHISPER_URL and TELEGRAM_WHISPER_KEY " +
                "(an OpenAI-compatible /audio/transcriptions endpoint). " +
                "Send the text instead, or use /attach to send a file.",
            );
            return;
          }
          if (Number(file?.file_size ?? 0) > 10 * 1024 * 1024) {
            await replyTo(chatId, "Voice message too large (cap 10 MB). Send a shorter one.");
            return;
          }
          const info: any = await bot.api.getFile(String(file.file_id));
          const rel = String(info?.file_path ?? "");
          if (rel === "") throw new Error("Telegram returned no file path");
          const dl = await fetch(`https://api.telegram.org/file/bot${config.token}/${rel}`);
          if (!dl.ok) throw new Error(`download failed (${dl.status})`);
          const bytes = Buffer.from(await dl.arrayBuffer());
          const transcript = await transcribeVoice(bytes, "voice.ogg");
          audit(`cmd chat=${chatId} user=${userId} cmd=voice chars=${transcript.length} result=ok`);
          await replyTo(chatId, `Heard: "${transcript.slice(0, 300)}"`);
          await route({ userId, chatId, chatKey: chatKeyNow }, transcript);
        } catch (error) {
          audit(`voice-FAILED chat=${chatId} error=${JSON.stringify(errorText(error))}`);
          await replyTo(chatId, `Could not transcribe that voice message: ${errorText(error)}`);
        }
        return;
      }

      // Tier 3: documents, photos and text files are downloaded to a temp dir
      // and attached to the next prompt as files:[{uri,name}].
      const incoming =
        tctx.message?.document ?? tctx.message?.photo?.[tctx.message.photo.length - 1];
      if (incoming && typeof incoming === "object") {
        const fileId = (incoming as any).file_id;
        const name =
          (incoming as any).file_name ?? (incoming as any).name ?? basename(String(fileId));
        // Bound the download before buffering it: Telegram metadata carries the
        // size, so a huge file is rejected without touching memory or disk.
        if (Number((incoming as any).file_size ?? 0) > 20 * 1024 * 1024) {
          await replyTo(chatId, "File too large to attach (cap 20 MB).");
          return;
        }
        const chatKeyNow = chatKeyOf(chatId, userId);
        try {
          const info: any = await bot.api.getFile(String(fileId));
          const rel = String(info?.file_path ?? "");
          if (rel === "") throw new Error("Telegram returned no file path");
          const url = `https://api.telegram.org/file/bot${config.token}/${rel}`;
          const res = await fetch(url);
          if (!res.ok) throw new Error(`download failed (${res.status})`);
          const bytes = Buffer.from(await res.arrayBuffer());
          const safeName = String(name).replace(/[^\w.\- ]+/g, "_").slice(0, 80);
          const target = join(attachmentDir(), `${Date.now()}-${safeName}`);
          writeFileSync(target, bytes);
          stageFiles(chatKeyNow, [{ uri: toFileUri(target), name: safeName }]);
          audit(`attach-staged chat=${chatId} name=${safeName} bytes=${bytes.length}`);
          await replyTo(
            chatId,
            `[ok] Attached ${safeName} (${bytes.length} bytes).\nIt will be sent with your next message.`,
          );
        } catch (error) {
          audit(`attach-FAILED chat=${chatId} error=${JSON.stringify(errorText(error))}`);
          await replyTo(chatId, `Could not download that file: ${errorText(error)}`);
        }
        return;
      }

      const text = String(tctx.message?.text ?? "").trim();
      if (text !== "") {
        try {
          await route({ userId, chatId, chatKey: chatKeyOf(chatId, userId) }, text);
        } catch (error) {
          const message = errorText(error);
          audit(`err op=route error=${JSON.stringify(message)}`);
          await replyTo(chatId, `Command failed: ${message}`);
        }
      }
      await next();
    });

    // Callback data is attacker-controllable (a forwarded message replays it),
    // so every tap is validated against a live token and the tapping user's own
    // state key before it can touch a session.
    bot.on("callback_query:data", async (tctx: any) => {
      reconcileOwnership("callback");
      const userId = tctx.from?.id as number | undefined;
      const chatId = tctx.chat?.id as number | undefined;
      if (!userId || !config.allowed.includes(userId) || !chatId) return;
      const data = String(tctx.callbackQuery?.data ?? "");
      try {
        await tctx.answerCallbackQuery();
      } catch {
        /* answering is best effort */
      }
      if (!data.startsWith(CALLBACK_PREFIX)) return;
      const token = data.slice(CALLBACK_PREFIX.length);
      const action = pendingActions.get(token);
      if (!action) {
        await replyTo(chatId, "That button has expired. Re-run the command.");
        return;
      }
      const chatKey = chatKeyOf(chatId, userId);
      // Notice buttons are bound to no chat key; they inherit the tapper.
      if (action.chatKey !== "" && action.chatKey !== chatKey) {
        audit(`cmd chat=${chatId} user=${userId} cmd=callback kind=${action.kind} result=wrong-user`);
        await replyTo(chatId, "That button belongs to a different chat.");
        return;
      }
      if (action.chatKey === "") action.chatKey = chatKey;
      pendingActions.delete(token);

      try {
        if (action.kind === "allow-once" || action.kind === "always" || action.kind === "deny") {
          const pending = action.permissionToken
            ? pendingPermissions.get(action.permissionToken)
            : undefined;
          if (!pending) {
            await replyTo(chatId, "That permission request is no longer pending.");
            return;
          }
          if (pending.chatKey !== chatKey) {
            audit(`cmd chat=${chatId} user=${userId} cmd=callback kind=${action.kind} result=wrong-user`);
            await replyTo(chatId, "That permission belongs to a different chat.");
            return;
          }
          const response = action.kind === "allow-once" ? "once" : action.kind === "always" ? "always" : "reject";
          const label = action.kind === "allow-once" ? "allow-once" : action.kind === "always" ? "always" : "deny";
          const done = await replyPermission(pending, response, userId, label);
          if (done && tctx.callbackQuery?.message?.message_id) {
            await editSent(
              chatId,
              tctx.callbackQuery.message.message_id,
              `Permission ${label}.`,
            );
          }
          return;
        }
        if (action.kind === "stop") {
          const turn = liveTurns.get(action.sessionID);
          await callSessions("interrupt", { sessionID: action.sessionID });
          if (turn) await finishTurn(turn, "interrupted");
          await replyTo(chatId, `Interrupt sent for ${shortId(action.sessionID)}.`);
          return;
        }
        if (action.kind === "diff") {
          await handleDiff(chatId, userId, chatKey, "");
          return;
        }
        if (action.kind === "model" && action.data) {
          const slash = action.data.indexOf("/");
          const providerID = action.data.slice(0, slash);
          const modelID = action.data.slice(slash + 1);
          await callSessions("switchModel", {
            sessionID: action.sessionID,
            model: { providerID, id: modelID },
          });
          audit(`cmd chat=${chatId} model=${action.data} via=button result=ok`);
          await replyTo(chatId, `Model set to ${action.data}.`);
          return;
        }
        if (action.kind === "agent" && action.data) {
          await callSessions("switchAgent", { sessionID: action.sessionID, agent: action.data });
          audit(`cmd chat=${chatId} agent=${action.data} via=button result=ok`);
          await replyTo(chatId, `Agent set to ${action.data}.`);
          return;
        }
        if (action.kind === "run-command" && action.data) {
          await ctx.session.command({
            sessionID: action.sessionID,
            command: action.data,
            arguments: "",
          });
          audit(`cmd chat=${chatId} run-command=${action.data} via=button result=ok`);
          await replyTo(chatId, `Ran /${action.data}.`);
          return;
        }
        if (action.kind === "run-skill" && action.data) {
          await ctx.session.command({
            sessionID: action.sessionID,
            command: action.data,
            arguments: "",
          });
          audit(`cmd chat=${chatId} run-skill=${action.data} via=button result=ok`);
          await replyTo(chatId, `Started ${action.data}.`);
          return;
        }
        if (action.kind === "ls-open") {
          if (lsNav === undefined) {
            await replyTo(chatId, "File browser is unavailable right now.");
            return;
          }
          await lsNav(chatId, userId, chatKey, action.data ?? "");
          return;
        }
        if (action.kind === "forget-cancel") {
          await replyTo(chatId, "Cancelled. Nothing was deleted.");
          return;
        }
        if (action.kind === "mcp-toggle") {
          const sep = (action.data ?? "").indexOf(":");
          const want = sep > 0 ? action.data!.slice(0, sep) : "";
          const name = sep > 0 ? action.data!.slice(sep + 1) : "";
          if ((want !== "on" && want !== "off") || name === "") {
            await replyTo(chatId, "That button is malformed. Re-run /mcps.");
            return;
          }
          try {
            await apiPost(
              `/api/experimental/mcp/${encodeURIComponent(name)}/${want === "on" ? "connect" : "disconnect"}`,
              {},
            );
          } catch (error) {
            audit(`err op=mcp.toggle server=${name} error=${JSON.stringify(errorText(error))}`);
            await replyTo(chatId, `Could not ${want === "on" ? "enable" : "disable"} ${name}: ${errorText(error)}`);
            return;
          }
          audit(`cmd chat=${chatId} user=${userId} cmd=callback kind=mcp-toggle server=${name} to=${want} result=ok`);
          await replyTo(chatId, `MCP server ${name} ${want === "on" ? "enabled" : "disabled"}. Send /mcps to confirm.`);
          return;
        }
        if (action.kind === "queue-withdraw") {
          const queue = promptQueues.get(action.sessionID) ?? [];
          const index = queue.findIndex((item) => item.id === action.data);
          if (index < 0) {
            await replyTo(chatId, "That queued prompt is already gone.");
            return;
          }
          queue.splice(index, 1);
          if (queue.length === 0) promptQueues.delete(action.sessionID);
          await persistQueues();
          audit(`cmd chat=${chatId} user=${userId} cmd=callback kind=queue-withdraw session=${action.sessionID} result=ok`);
          await replyTo(chatId, "Withdrawn from the queue. It will not be sent.");
          return;
        }
        if (action.kind === "task-delete") {
          const task = action.data !== undefined ? scheduledTasks[action.data] : undefined;
          if (task === undefined) {
            await replyTo(chatId, "That task is already gone.");
            return;
          }
          if (task.runningSessionID !== undefined && liveTurns.has(task.runningSessionID)) {
            await replyTo(chatId, `Task ${task.id} is running right now - wait for it to finish first.`);
            return;
          }
          delete scheduledTasks[task.id];
          await persistTasks();
          audit(`cmd chat=${chatId} user=${userId} cmd=callback kind=task-delete task=${task.id} result=ok`);
          await replyTo(chatId, `Deleted scheduled task ${task.id}.`);
          return;
        }
        if (action.kind === "msg-fork") {
          if (!action.data || !/^msg_[A-Za-z0-9]+$/.test(action.data)) {
            await replyTo(chatId, "That button has no valid message. Re-run /messages.");
            return;
          }
          await doFork(chatId, chatKey, action.sessionID, action.data);
          return;
        }
        if (action.kind === "msg-revert-cancel") {
          await replyTo(chatId, "Revert cancelled. Nothing was changed.");
          return;
        }
        if (action.kind === "msg-revert") {
          if (bindingFor(chatKey)?.sessionID !== action.sessionID) {
            await replyTo(chatId, "Bind that session first with /use, then revert from /messages.");
            return;
          }
          if (!action.data || !/^msg_[A-Za-z0-9]+$/.test(action.data)) {
            await replyTo(chatId, "That button has no valid message. Re-run /messages.");
            return;
          }
          let staged: any;
          try {
            const body: any = await apiPost(`/api/session/${action.sessionID}/revert/stage`, {
              messageID: action.data,
            });
            staged = body?.data ?? body;
          } catch (error) {
            audit(`err op=revert.stage session=${action.sessionID} error=${JSON.stringify(errorText(error))}`);
            await replyTo(chatId, `Could not stage that revert: ${errorText(error)}`);
            return;
          }
          const files = Array.isArray(staged?.files) ? staged.files.length : 0;
          await replyTo(
            chatId,
            `Revert to ${shortId(action.data)}? Everything after it is discarded` +
              (files > 0 ? `, including changes in ${files} file${files === 1 ? "" : "s"}` : "") +
              ".",
            {
              inline_keyboard: [
                [
                  button("Confirm revert", {
                    kind: "msg-revert-confirm",
                    chatKey,
                    chatId,
                    sessionID: action.sessionID,
                    data: action.data,
                  }),
                  button("Cancel", {
                    kind: "msg-revert-cancel",
                    chatKey,
                    chatId,
                    sessionID: action.sessionID,
                  }),
                ],
              ],
            },
          );
          return;
        }
        if (action.kind === "msg-revert-confirm") {
          if (bindingFor(chatKey)?.sessionID !== action.sessionID) {
            await replyTo(chatId, "Bind that session first with /use, then revert from /messages.");
            return;
          }
          try {
            await apiPost(`/api/session/${action.sessionID}/revert/commit`, {});
          } catch (error) {
            audit(`err op=revert.commit session=${action.sessionID} error=${JSON.stringify(errorText(error))}`);
            await replyTo(chatId, `Could not commit that revert: ${errorText(error)}`);
            return;
          }
          titleCache.delete(action.sessionID);
          audit(`cmd chat=${chatId} user=${userId} cmd=callback kind=msg-revert-confirm session=${action.sessionID} result=ok`);
          await replyTo(chatId, `Reverted to ${shortId(action.data ?? "?")}. Later messages discarded.`);
          return;
        }
        if (action.kind === "forget-confirm") {
          await callSessions("remove", { sessionID: action.sessionID });
          audit(`cmd chat=${chatId} forget=${action.sessionID} result=deleted`);
          if (bindingFor(chatKey)?.sessionID === action.sessionID) {
            await clearBinding(chatKey);
          }
          titleCache.delete(action.sessionID);
          await replyTo(chatId, `Deleted ${shortId(action.sessionID)}.`);
          return;
        }
        // "open" and "bind" both mean: bind this chat to that session.
        await bindTo(chatId, chatKey, action.sessionID);
      } catch (error) {
        const message = errorText(error);
        audit(`err op=callback kind=${action.kind} session=${action.sessionID} error=${JSON.stringify(message)}`);
        await replyTo(chatId, `Action failed: ${message}`);
      }
    });

    // grammy long-polls for incoming messages; the chat_id handshake above
    // depends on it, so a polling failure must not take the plugin down.
    bot.catch((error: unknown) => console.error("[telegram-notify] bot error:", error));
    // drop_pending_updates stays false: with the whitelist already correct there
    // is no stale-message risk, and discarding the queue would silently swallow
    // the very messages used to verify delivery.
    // Wrapped so a follower isolate can be promoted later without re-running
    // setup - only the lock holder is ever allowed to poll.
    let polling = false;

    async function startPolling(): Promise<void> {
      if (polling) return;
      polling = true;
      try {
        await bot.start({ drop_pending_updates: false });
      } catch (error: unknown) {
        polling = false;
        console.error("[telegram-notify] polling failed:", error);
        audit(`polling-failed error=${(error as Error).message}`);
      }
    }

    async function stopPolling(): Promise<void> {
      if (!polling) return;
      polling = false;
      try {
        await bot.stop();
      } catch {
        /* best effort: the loop may already be gone */
      }
    }

    // Take ownership, or stand by to inherit it. Every isolate keeps its event
    // subscription, so promotion only has to flip the gate and switch the
    // Telegram side on - no re-running of setup.
    function activateOwner(reason: string): void {
      if (crossOwner) return;
      crossOwner = true;
      heartbeat();
      lockHeartbeat = setInterval(heartbeat, LOCK_HEARTBEAT_MS);
      audit(`owner-acquired reason=${reason} gen=${CODE_GENERATION} dir=${ctx.location?.directory ?? "?"}`);
      startWatchdog();
      startScheduler();
      void startPolling();
      void registerMenu();
    }

    // Converge every isolate to exactly one owner. Runs on every setup, on a
    // timer in every isolate, and on every inbound Telegram update, so owner
    // death, duplicate owners, and zombie pollers all self-heal within ticks:
    // the lock file names a single token, its holder serves, everyone else
    // stands down and stops polling.
    let ownershipTimer: ReturnType<typeof setInterval> | undefined;
    let lastStoodDownAudit = 0;
    // Consecutive ticks where the lock file exists but cannot be parsed. A
    // permanently corrupt lock must eventually be stolen, or no isolate can
    // ever own the bot again.
    let unreadableStreak = 0;

    function reconcileOwnership(reason: string): void {
      const exists = existsSync(LOCK_FILE);
      const current = readLock();
      const mine = current !== undefined && current.token === OWNER_TOKEN;
      if (mine) {
        if (!crossOwner) {
          activateOwner(`reclaimed-${reason}`);
        } else if (!polling) {
          void startPolling();
        }
        return;
      }
      if (crossOwner) crossOwner = false;
      if (polling) {
        void stopPolling().then(() => {
          const now = Date.now();
          if (now - lastStoodDownAudit > 60_000) {
            lastStoodDownAudit = now;
            audit(`stood-down reason=${reason} gen=${CODE_GENERATION}`);
          }
        });
      }
      if (!exists) {
        if (tryCreateLock()) activateOwner(`recreated-${reason}`);
        return;
      }
      // Present but unreadable (mid-write?): wait for the next tick rather
      // than fighting over a lock that may be perfectly healthy. Past ten
      // consecutive unreadable ticks, treat it as corrupt and steal it.
      if (current === undefined) {
        unreadableStreak += 1;
        if (unreadableStreak >= 10 && stealLock()) {
          unreadableStreak = 0;
          activateOwner(`stole-corrupt-${reason}`);
        }
        return;
      }
      unreadableStreak = 0;
      if (Date.now() - current.ts > LOCK_STALE_MS && stealLock()) {
        activateOwner(`stole-${reason}`);
      }
    }

    function startOwnershipTimer(): void {
      if (ownershipTimer !== undefined) return;
      ownershipTimer = setInterval(() => reconcileOwnership("tick"), LOCK_POLL_MS);
    }

    function stopOwnershipTimer(): void {
      if (ownershipTimer !== undefined) {
        clearInterval(ownershipTimer);
        ownershipTimer = undefined;
      }
    }

    reconcileOwnership("setup");
    startOwnershipTimer();

    // --- event stream ------------------------------------------------------

    // Repeat-completion guard. OpenCode has been observed emitting
// session.execution.succeeded twice for a single turn (~0.8s apart), and the
// fallback path below has no live turn to dedupe against - so the phone got the
// same alert twice. Keyed by session+outcome with a short window.
const completedAt = new Map<string, number>();
const COMPLETION_DEDUPE_MS = 30_000;

function claimCompletion(sessionID: string, outcome: string): boolean {
  const key = `${sessionID}:${outcome}`;
  const last = completedAt.get(key);
  if (last !== undefined && Date.now() - last < COMPLETION_DEDUPE_MS) return false;
  completedAt.set(key, Date.now());
  if (completedAt.size > 200) {
    const oldest = completedAt.keys().next().value;
    if (oldest !== undefined) completedAt.delete(oldest);
  }
  return true;
}

async function onCompletion(
      sessionID: string,
      outcome: "succeeded" | "failed",
      detail?: string,
    ): Promise<void> {
      const turn = sessionID ? liveTurns.get(sessionID) : undefined;
      if (turn) {
        // A completion arriving for a live turn right after an interrupt is
        // almost certainly the OLD turn's late event, not the new turn's:
        // treating it as success would end the new turn early AND produce a
        // second summary when the real completion lands. End tracking quietly
        // instead; the real completion completes via the fallback below.
        const interrupted = interruptedAt.get(sessionID) ?? 0;
        if (interrupted !== 0 && Date.now() - interrupted < 300_000) {
          interruptedAt.delete(sessionID);
          audit(`turn session=${sessionID} state=running->idle outcome=stale-after-interrupt`);
          await finishTurn(turn, "interrupted");
          // finishTurn re-arms the marker; clear it so the new turn's real
          // completion is not suppressed as well.
          interruptedAt.delete(sessionID);
          return;
        }
        await finishTurn(turn, outcome, detail);
        return;
      }
      if (Date.now() - (interruptedAt.get(sessionID) ?? 0) < 60_000) {
        interruptedAt.delete(sessionID);
        audit(`turn session=${sessionID} state=idle outcome=suppressed-after-interrupt`);
        return;
      }
      if (!claimCompletion(sessionID, outcome)) {
        audit(`completion-deduped session=${sessionID} outcome=${outcome}`);
        return;
      }
      // Control version: bound chats only, so one conversation holds the summary
      // plus follow-up commands. Notify version: every session, global chat.
      const targets = boundChatIds(sessionID);
      if (targets.length === 0) {
        audit(`completion-unbound-control-skipped session=${sessionID} outcome=${outcome}`);
      }
      await notifyFinished(sessionID, outcome, detail, targets, activeChatId);
    }

    async function handleEvent(type: string, p: any): Promise<void> {
      // Learn session titles opportunistically from any event that carries one,
      // so notifications and /tasks can name the session.
      const seenTitle = titleOf(p);
      const seenId = sessionIdOf(p);
      if (seenTitle && seenId) rememberSession(seenId, seenTitle, directoryOf(p));

      if (type === "session.step.ended") {
        const finish = String(p?.finish ?? "<none>");
        if (!seenFinishes.has(finish)) {
          seenFinishes.add(finish);
          audit(`step-end finish=${finish} (first seen)`);
        }
      }

      if (type === "session.execution.started") {
        if (seenId) {
          executionStartedAt.set(seenId, Date.now());
          if (executionStartedAt.size > 200) {
            const oldest = executionStartedAt.keys().next().value;
            if (oldest !== undefined) executionStartedAt.delete(oldest);
          }
        }
      }

      if (type === "session.execution.failed") {
        const detail =
          p?.error?.message ?? p?.error?.data?.message ?? p?.message ?? p?.reason ?? "unknown error";
        if (seenId) {
          await enqueueSession(seenId, () => onCompletion(seenId, "failed", String(detail)));
        }
        return;
      }

      if (isIdleEvent(type, p)) {
        let sessionID = seenId;
        let title = sessionID ? sessionTitles.get(sessionID) : undefined;
        // Fall back to the server for the authoritative title/id when the
        // session was never seen carrying one (e.g. after a reload).
        if ((!title || !sessionID) && seenId) {
          try {
            const info = await ctx.session.get({ sessionID: seenId });
            title = titleOf(info) ?? title;
            sessionID = sessionIdOf(info) ?? sessionID;
          } catch {
            /* session may belong to another location */
          }
        }
        if (!sessionID) {
          if (!alertEnabled("completed")) {
            audit("completed-suppressed session=<unknown>");
            return;
          }
          // Same rule as session completions: summaries live on the control bot,
          // mirrored on the notify bot.
          if (activeChatId === null) {
            audit("dropped no-active-chat text=Agent has finished");
            return;
          }
          const finishedText = title ? `Agent has finished: ${title}` : "Agent has finished.";
          await sendToChat(activeChatId, finishedText);
          await sendToChat(activeChatId, finishedText, undefined, "notify");
          return;
        }
        await enqueueSession(sessionID, () => onCompletion(sessionID as string, "succeeded"));
        return;
      }

      // V2 publishes questions as permission requests (permission.asked, verified
      // on a live server); V1 used a dedicated question.asked event. Only these
      // types are inspected, so ordinary permission prompts stay silent unless
      // they belong to a remote turn.
      if (type === "question.asked" || type === "permission.asked") {
        const sessionID = seenId ?? String(p?.sessionID ?? "");
        if (sessionID === "") {
          await onPermissionAsked(sessionID, p);
          return;
        }
        await enqueueSession(sessionID, () => onPermissionAsked(sessionID, p));
      }
    }

    const controller = new AbortController();
    void (async () => {
      try {
        for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
          const type = String(event?.type ?? event?.event ?? "");
          const eventId = typeof event?.id === "string" ? event.id : "";
          if (eventId && alreadyHandled(eventId)) continue;
          const p = props(event);
          // Record every distinct event type once in the audit log. This is the
          // only reliable way to confirm which completion signal this server
          // build actually emits, because plugin console output is not
          // surfaced by OpenCode. Bounded: one line per type per process.
          if (!seenTypes.has(type)) {
            seenTypes.add(type);
            audit(`event-type ${type}`);
            if (debug) console.log(`[telegram-notify] event ${type}`);
          }

          try {
            await handleEvent(type, p);
          } catch (error) {
            audit(`err op=event type=${type} error=${JSON.stringify(errorText(error))}`);
          }
        }
      } catch (error) {
        if (!controller.signal.aborted) {
          console.error("[telegram-notify] event subscription ended:", error);
          audit(`err op=subscribe error=${JSON.stringify(errorText(error))}`);
        }
      }
    })();

    // Sweeper for default-deny permission expiry (Â§7/Â§14.5) and stale buttons.
    const sweeper = setInterval(() => {
      pruneExpiredPermissions();
    }, SWEEP_MS);
    sweeper.unref?.();

    console.log("[telegram-notify] ready");
    audit(
      `ready allowed=${config.allowed.length} chat=${config.chatId ?? "auto"} ` +
        `dir=${ctx.location?.directory ?? "unknown"}`,
    );

    // Self-register the command menu with Telegram so the hamburger button stays
    // correct without a manual call. Runs ONLY on the singleton-owning path
    // (never in the per-location attach branch, which fires ~35 times). A
    // Named so a promoted isolate can register the menu without re-running setup.
    async function registerMenu(): Promise<void> {
    // Telegram API failure must never break setup, hence the try/catch.
    try {
      const BOT_COMMANDS: Array<{ command: string; description: string }> = [
        { command: "help", description: "Show all commands" },
        { command: "tasks", description: "List sessions" },
        { command: "projects", description: "List workspaces/projects" },
        { command: "active", description: "List live sessions on this server" },
        { command: "refresh", description: "Refresh cached session titles" },
        { command: "new", description: "Create a session and bind it here" },
        { command: "use", description: "Bind this chat to a session" },
        { command: "fork", description: "Fork the bound session" },
        { command: "forms", description: "List open session questions" },
        { command: "answer", description: "Answer an open question/form" },
        { command: "unbind", description: "Clear the binding" },
        { command: "status", description: "Bound session and running turn" },
        { command: "stop", description: "Interrupt the running turn" },
        { command: "diff", description: "Files changed in the bound session" },
        { command: "read", description: "Last n assistant messages" },
        { command: "model", description: "Show or switch the model" },
        { command: "agent", description: "Show or switch the agent" },
        { command: "context", description: "Tokens and cost for this session" },
        { command: "compact", description: "Compact the session context" },
        { command: "rename", description: "Rename the bound session" },
        { command: "forget", description: "Delete the bound session" },
        { command: "commands", description: "Run OpenCode custom commands" },
        { command: "skills", description: "Run an OpenCode skill" },
        { command: "perm", description: "List or answer permission requests" },
        { command: "mode", description: "Toggle remote execution" },
        { command: "whoami", description: "Your id, binding and mode" },
        { command: "cancel", description: "Drop queued prompt and buttons" },
        { command: "attach", description: "Stage a project file for your next message" },
        { command: "detach", description: "Clear staged files" },
        { command: "ls", description: "Browse project files" },
        { command: "worktree", description: "List or switch git worktrees" },
        { command: "live", description: "Post or refresh a status card" },
        { command: "steer", description: "Inject into a running turn instead of queueing" },
        { command: "skill", description: "Attach a specific skill" },
        { command: "buttons", description: "Toggle Open/Diff/Stop on finished alerts" },
        { command: "alerts", description: "Set notification preferences" },
        { command: "health", description: "Server and plugin health status" },
        { command: "messages", description: "Browse messages, revert or fork" },
        { command: "task", description: "Schedule a prompt to run later" },
        { command: "tasklist", description: "List scheduled tasks" },
        { command: "queue", description: "List queued prompts" },
        { command: "btw", description: "Run now or first in queue" },
        { command: "mcps", description: "List and toggle MCP servers" },
      ];
      if (!crossOwner) return;
      await bot.api.setMyCommands(BOT_COMMANDS);
      audit(`telegram-menu registered=${BOT_COMMANDS.length}`);
    } catch (error) {
      audit(`telegram-menu failed error=${JSON.stringify(errorText(error))}`);
    }
    }
    if (crossOwner) await registerMenu();

    const state = claimSingleton(CODE_GENERATION, () => {
      // Stop polling FIRST: if anything below throws, a live poller must not
      // survive teardown (that produced silent command blackholes before).
      try {
        polling = false;
        void bot.stop().catch(() => {});
      } finally {
        controller.abort();
        clearInterval(sweeper);
        stopWatchdog();
        stopScheduler();
        stopOwnershipTimer();
        if (lockHeartbeat !== undefined) clearInterval(lockHeartbeat);
        // Hand ownership back so a surviving isolate can take over immediately
        // instead of waiting out the stale timeout.
        if (crossOwner) {
          crossOwner = false;
          releaseLock();
        }
      }
    });
    ownedState = state;
    return () => {
      try {
        polling = false;
        void bot.stop().catch(() => {});
      } finally {
        releaseSingleton(state);
        stopWatchdog();
        stopScheduler();
        stopOwnershipTimer();
        if (lockHeartbeat !== undefined) clearInterval(lockHeartbeat);
        if (crossOwner) {
          crossOwner = false;
          releaseLock();
        }
      }
    };
  },
};