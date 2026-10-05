# Remote Control for OpenCode over Telegram — Design

Status: proposed. Not implemented.
Scope: extends `local.telegram-notify` (currently notify-only) into a
remote-control surface equivalent to ZCode's phone UI.

---

## 0. Correction to the earlier sketch

Two claims in the first design were wrong and are corrected here:

- `session.prompt()` does **not** block until the turn finishes. It returns
  `Session.Inbox.User`, i.e. an *admission receipt*. Turn completion must be
  detected from the event stream.
- There is **no** `session.wait` endpoint in the V2 OpenAPI schema, despite the
  plugin guide mentioning `ctx.session.wait`.

Consequence: the design is event-driven, with an explicit per-session turn
state machine, rather than a simple request/response.

---

## 1. Goals / non-goals

**Goals**

- List, create, select and stop OpenCode sessions from a phone.
- Send a prompt to a session and receive the agent's reply in Telegram.
- See what changed (files/diff) without returning to the desktop.
- Answer tool permission requests remotely, so the agent never blocks on a
  human who is not at the machine.
- Survive OpenCode reloads and server restarts without losing bindings.

**Non-goals**

- Streaming token-by-token output (chunks are large and noisy on a phone;
  see §6 for the progressive alternative).
- Multiple simultaneous remote users (design assumes one, but §3 keeps the
  data model N-safe).
- Voice, images, file upload.

---

## 2. Architecture

Three layers, all inside the single plugin process:

```
Telegram  ──update──▶  Command router  ──▶  Session registry (ctx.storage)
                          │                        │
                          │                        ├─ session.create / prompt / interrupt
                          │                        ├─ diff / context / message.list
                          │                        └─ permission.reply
                          ▼
                    Reply renderer  ──sendMessage──▶  Telegram
                          ▲
                          │
                 Event stream (ctx.event.subscribe)
                   session.execution.succeeded / failed
                   permission.asked / replied
```

There is no new process and no new port. The plugin already holds a single
long-poll and a single event subscription (§ generation/singleton in
`index.ts`); the remote-control layer reuses both.

---

## 3. State model

Today the plugin keeps state in module-level `Map`s, which are lost on reload —
already a source of bugs. Remote control needs durable state, so:

| Key (`ctx.storage`, scoped to plugin id) | Value |
|---|---|
| `bindings` | `{ [chatId: string]: { sessionID: string; label: string } }` |
| `sessions` | `{ [sessionID: string]: { title; lastUsed; createdAt } }` |
| `auth` | `{ pinHash; pinSetAt; failedAttempts; lockedUntil }` |
| `settings` | `{ mode: "notify" \| "control"; allowExec: boolean; requirePin: boolean }` |

`ctx.storage` is the documented durable store and is scoped by plugin id, so it
survives reload and is not shared with other plugins.

**Routing rule.** A chat may be bound to exactly one session at a time
(`/use`). Telegram forum topics are deliberately *not* used: they require the
bot to administer a supergroup and complicate per-user isolation. If N users
are ever whitelisted, the key becomes `(chatId, userId)`.

**Discovery rule.** `session.list` is only meaningful per location. Sessions
belong to a directory; the plugin sees the global stream, so `/tasks` must
either filter by the bound location or label each session with its directory.
Decision needed (§14).

---

## 4. Command surface

| Command | Action | Notes |
|---|---|---|
| `/help` | list commands | also the fallback hint when input is unrecognised |
| `/tasks` | list sessions: index, short id, title, status, directory | ZCode's "Task home" |
| `/new [title]` | `session.create`, bind it, return short id | |
| `/use <idx\|id>` | bind this chat to a session | |
| `/unbind` | clear binding | |
| `/status` | active session, running turn?, elapsed, pending permission | |
| `/stop` | `session.interrupt` | ZCode's stop |
| `/diff [file]` | `session.diff` | summary of files changed |
| `/read [n]` | last assistant message(s) | avoids re-sending long text |
| `/perm` | list pending permission requests | interactive flow, §7 |
| `/perm <id> yes\|no\|always` | `session.permission.reply` | |
| `/compact` | `session.compact` | |
| `/model`, `/agent` | `switchModel` / `switchAgent` | shows current values first |
| `/rename <title>` | `session.update` | |
| `/forget` | `session.remove` | destructive — requires confirm button |
| `/mode notify\|control` | toggle (§8) | |
| `/whoami` | show your Telegram id, binding, pin state | |
| `/revoke` | rotate PIN, show new link-equivalent | mirrors ZCode's QR refresh |
| `/cancel` | clear a pending inline-button state | |
| *anything else* | `session.prompt` on the bound session | the main path |

Every command must be **explicitly matched**. Free text is only treated as a
prompt when a session is bound; otherwise it replies with `/help` and never
interprets input.

---

## 5. Interaction model

Typing `/use 3` on a phone is poor UX. Preferred: **inline keyboards**.

- Notification for a finished task carries buttons:
  `[ Open ] [ Diff ] [ Stop ]` and `[ ‹ Prev ] [ Next › ]` when several finish.
- `permission.asked` renders the request with `[ Allow once ] [ Always ] [ Deny ]`,
  each a callback query carrying the permission id.
- Pending answers are held in a short-lived map keyed by callback data, so a
  stale tap cannot affect a newer request.

All callback payloads must be validated against the whitelist and the binding,
because callback data is attacker-controllable if the message is forwarded.

---

## 6. Concurrency and the turn lifecycle

The core state machine, per session:

```
idle ──prompt()──▶ admitted ──▶ running ──execution.succeeded──▶ idle (reply sent)
                      │             │
                      │             ├──execution.failed───────▶ idle (error sent)
                      │             ├──permission.asked──────▶ awaiting_permission
                      │             │                              │
                      │             │◀────permission.replied──────┘
                      │             │
                      │             └──interrupt()──────────▶ idle
                      └──(admission rejected: rate limit, policy)──▶ idle (error sent)
```

Rules:

1. **One in-flight turn per session.** A second `/prompt` while `running` is
   rejected with a clear message, or optionally queued (decision, §14).
2. **`prompt()` returning is not completion.** The reply is produced only when
   `session.execution.succeeded` arrives for that session.
3. **Progress feedback.** Telegram has no push, so immediately ack
   ("▶️ sent, working…"). Then either:
   - stay silent and send the final reply, or
   - send throttled heartbeats (e.g. every 90 s, max N) for long turns.
   Decision (§14). A turn can exceed 30 min, so silence is confusing.
4. **Late events.** If `execution.succeeded` arrives for a session nobody is
   bound to, fall back to the current notification behaviour.
5. **Turn timeout.** Configurable; on expiry send "still running" rather than
   assume failure.
6. **Ordering.** Event handling is serialized per session via a promise chain,
   so a fast-finishing turn cannot interleave its reply with the next one.

---

## 7. Permission flow

This is the highest-risk area, and the one that decides whether remote control
is safe.

Current state: `bash: {"*": "allow"}` in `opencode.json` means nothing is ever
asked, so the agent never blocks — but remote prompts are therefore
unrestricted execution.

Design:

- In `control` mode with `allowExec: false`, the plugin reacts to
  `permission.asked` by pushing an inline keyboard and **suspending** the
  session until answered.
- **Default-deny on expiry.** Unanswered requests are rejected after N minutes.
  Failing open is not acceptable for code execution.
- Show the full action and resources (`action`, `resources`, `message`) so the
  decision is informed — a bare "allow?" is not consent.
- Record every decision in the audit log with the deciding user id.
- `always` applies only to that resource pattern, mirroring the permission
  rules the desktop would have written.

---

## 8. Security model

Threat: whoever controls the Telegram account can operate the machine.

| Control | Rationale |
|---|---|
| `TELEGRAM_ALLOWED_USER_IDS` whitelist | already present; mandatory |
| Optional PIN required before any `prompt` | limits damage if the account is compromised |
| PIN stored as a salted hash in `ctx.storage` | never plaintext, not in `.env` |
| Rate-limit PIN attempts, lock out with backoff | Telegram-side brute force is trivial otherwise |
| Idle TTL — binding expires after inactivity | unattended phone is less likely to act |
| `/mode notify` default on install | no remote execution until explicitly enabled |
| Per-command allowlist option | e.g. allow `/status`, deny `/new` |
| Full audit log of every remote action | accountability + incident review |
| `/revoke` rotates auth | mirrors ZCode's "refresh the QR" |

Explicitly **not** offered: running with no whitelist, or sending file contents
of arbitrary paths back to Telegram without an explicit command.

Residual risk must be stated in `SETUP.md`: this is remote code execution by
design, and the mitigations raise the bar rather than eliminate the risk.

---

## 9. Error handling

| Failure | Response |
|---|---|
| Model rate limit (observed on this machine) | reply "rate limited upstream", do not retry automatically |
| `prompt` admission rejected | surface the server's reason verbatim |
| Session removed / not found | clear binding, tell the user, offer `/tasks` |
| Turn already running | reject or queue (§14) |
| `permission.reply` on an expired request | report and re-list pending |
| Telegram 429 rate limit | queue with backoff, cap the queue, drop on overflow |
| Telegram 403 (bot blocked by user) | mark chat unusable, stop sending, audit it |
| `session.get`/`diff` unauthorized for that location | say "not accessible from here" — never leak |
| Event stream drops (documented as volatile) | resubscribe with backoff; note that events during the gap are lost |
| Plugin reload mid-turn | mark the turn orphaned; on rebind tell the user the result is unknown rather than silently dropping it |
| Chunk > 4096 chars | split on line boundaries, number the parts |

---

## 10. Notification redesign

Current message: `Agent has finished: <title>`. Too thin for a control
surface. New format:

```
✅ Finished — T020 CapitalFirewall authority map
Project: darkfleet
Session: ses_efcc…  (reply /tasks to bind)
Duration: 14m  ·  3 files changed
[ Open ] [ Diff ] [ Stop ]
```

Failure variant uses ⚠️ and includes the error summary. `permission.asked`
becomes an actionable prompt rather than a read-only notice.

---

## 11. Observability

Extend `sent.log` (already proven valuable) with one line per remote action:

```
cmd   chat=… user=… cmd=/use arg=3
prompt chat=… session=… chars=…
turn  session=… state=running->idle ms=142000
perm  session=… request=… decision=allow-once user=…
err   op=prompt session=… error="rate limit exceeded"
```

Redact prompt bodies by default (they may contain secrets); log only length and
a hash unless `settings.logBodies` is enabled.

---

## 12. Cost guardrails

Remote prompts spend the same quota as local ones, and this machine already
hits free-tier rate limits. Add:

- per-turn character cap on inbound prompts
- optional daily prompt cap
- clear "this will consume quota" behaviour documented, not hidden

---

## 13. Rollout

1. Ship the state model + `/help`, `/tasks`, `/whoami`, `/mode` — read-only, no
   execution. Verify on the phone.
2. Add `/use`, `/new`, `/status`, `/stop`, `/diff`, `/read`.
3. Add `prompt` behind PIN + `control` mode, initially with `allowExec: false`
   so only non-exec work runs.
4. Add the permission flow; only then consider `allowExec: true`.
5. Leave `notify` mode as a fallback switch at every stage.

Each step bumps `CODE_GENERATION` (see `SETUP.md`) — without that, reload keeps
the previous logic alive and changes silently do nothing. This already bit us
twice.

---

## 14. Decisions taken (owner delegated these; recorded so they can be revisited)

| # | Decision | Rationale |
|---|---|---|
| 1 | PIN **optional**; when configured it is required for `prompt` and for permission-allow decisions | Requiring a PIN on first use would lock the owner out of their own bot. A warned-unprotected state is better than an unusable one. `requirePin` defaults true but auto-softens while no PIN is set. |
| 2 | **Single-slot queue**: a prompt sent while a turn is running is held and auto-sent on completion, with a confirmation | Phone users double-tap. Rejecting causes retries, which is worse than one bounded queue slot. Slot is overwritten by a newer prompt. |
| 3 | **Heartbeats** every 120 s, max 4 per turn, showing elapsed time | Silence during a 30-minute turn is indistinguishable from a crash. |
| 4 | `/tasks` lists **all** sessions, each labelled with its project directory; optional substring filter | The event stream is global, so pretending otherwise would be misleading. |
| 5 | Permission requests expire after **10 minutes** and are **denied** on expiry | Default-deny is the only acceptable failure mode for code execution. |
| 6 | Inbound prompt cap **4000 chars**; audit logs length + hash, never bodies | Prompt bodies routinely contain secrets and source code. |
| 7 | All state keyed by **`chatId:userId`** | Costs nothing now and prevents cross-user leakage if a second user is ever added. |

Additional standing decisions:

- Installed mode is **`notify`** — no remote execution until `/mode control`.
- `allowExec` defaults to **false** even in control mode.
- Free text is only interpreted as a prompt when a session is bound; otherwise it
  returns `/help` and never guesses.