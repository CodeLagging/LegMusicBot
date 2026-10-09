# Bug scan — summary of fixes

Seven passes over `main.py`, `worker.py`, `db.py`, `appsettings.py` and `launcher.py`: full read-throughs, scenario checks (races, restarts, Discord limits, Lavalink failures), a `ruff` bug-rule scan, and a final regression run of search and autoplay against the real Lavalink / YouTube / Groq. Every fix below was tested (fake players/workers for the failure cases, real services for search). **33 fixes**, plus two small cleanups.

Status: committed locally (`3b46e36` … pass 7), **not pushed or deployed yet**.

---

## Playback could get stuck or stop

| # | Problem | What you'd have seen | Fix |
|---|---|---|---|
| 1 | If Lavalink refused to play a song, the error escaped and the song change died. | Silence with songs still queued; the worker stayed "busy" until the watchdog noticed. | Unplayable songs are skipped; after 3 in a row it stops and goes idle. |
| 2 | The "song ended" / "song failed" handlers had no protection. | Same as above, from any unexpected error. | Errors are logged and the session goes idle (leaves after the idle timeout) instead of hanging. |
| 3 | Lavalink's "track stuck" event (stream stopped sending audio) was ignored. | A song that went silent forever. | Stuck songs are skipped automatically. |
| 4 | After a Lavalink restart mid-song, the queue never resumed (the watchdog only handled *empty* queues). | Songs queued, nothing playing, forever. | Watchdog resumes a stalled queue after two 30-second checks. |
| 5 | Jump To an unplayable song had already dropped the queue up to it. | Nothing playing after Jump To. | It moves on to the next song and tells you. |
| 6 | Seeking a stream with no known length clamped to 0. | ⏩ on a live stream jumped back to the start. | Streams aren't clamped. |
| 7 | `/stop` (or a restart) landing while the worker was still searching/connecting. | Music started anyway with no session — nobody could control or stop it for 30 s+. | The worker checks after connecting; if the session was stopped it leaves instead of playing. |
| 8 | A slow `/play` (> 30 s) timed out in the main bot; the worker's reply then failed and it **ended the new session**. | "Worker timed out" and then the song stopped. | Timeout raised to 90 s, and a lost reply no longer undoes the playback. |
| 9 | Waiting on an autoplay refill that got cancelled (stop/restart) re-raised the cancellation. | A command dying with "Worker closed the connection". | Waits no longer inherit the cancellation. |
| 10 | Replacing a half-disconnected voice player read `player.channel.id` on `None`. | `/play` failing with an internal error. | Leftover is cleaned up safely — and (pass 5) only after marking the session as connecting, so its own "left voice" event can't end it. *(The ordering bug in this fix was introduced in pass 1 and caught in pass 5.)* |

## Workers and the main bot

| # | Problem | What you'd have seen | Fix |
|---|---|---|---|
| 11 | The main bot read worker logs with a 64 KB line limit; one longer line stopped the reader, the pipe filled up, and **the worker froze**. | A worker silently stops responding. | Reader never stops; long lines are split. |
| 12 | A worker alive but frozen was never detected (only "process exists" was checked). | A worker stuck "busy" until a manual restart. | 4 failed 30-second health checks (2 min) → killed and restarted. |
| 13 | Dev ``restart`` and the watchdog could both start a worker. | **Two copies of one worker** logged in with the same token, one never stopped. | Starts/restarts share a lock. |
| 14 | After ``shutdown n``, the worker's watchdog exited for good; ``restart n`` brought it back unwatched. | That worker never auto-recovers from a crash again. | Restart re-attaches a watchdog. |
| 15 | Workers adopted from a previous main bot couldn't be stopped (no process handle). | Restarting one created a duplicate next to the old one. | They're told to shut down over IPC first. |
| 16 | `/play` could pick a worker that was down (shut down / crashed / restarting). | "Worker socket not available" while another worker was free. | Unresponsive workers are skipped. |
| 17 | A worker crashing during a graceful restart counted as "still finishing a song". | Restart waiting the full 5 + 2 minutes for nothing. | Unresponsive workers aren't waited for. |
| 18 | If the main bot's Discord login died for good (e.g. bad token), the process kept running offline. | Bot offline, service "active", never restarted. | Exits with an error so the launcher/systemd restart it. |
| 19 | Worker replies over 64 KB were rejected. | `/control` failing on long playlist queues (a few hundred songs). | Limit raised to 8 MB on both ends. |
| 20 | Fire-and-forget tasks weren't referenced (Python can garbage-collect them mid-run). | Rare, silent loss of a status update, DB write or background loop. | All such tasks are kept until they finish. |
| 21 | `launcher.py` trusted any process named `main.py` from a stale `.bot.pid`. | After a hard kill + PID reuse, it could wait 9 min on — and kill — **an unrelated program**. | Also checks the process runs from the bot's folder. |
| 22 | Timed-out yt-dlp processes were killed but never reaped. | Lingering zombie processes. | They're awaited after the kill. |
| 23 | Database connections were only committed, never closed. | Relied on CPython closing them implicitly. | Every connection is committed and closed. |

## Search, AI and history

| # | Problem | What you'd have seen | Fix |
|---|---|---|---|
| 24 | Strong history with a song's normal upload overrode an explicit "slowed"/"sped up" request. | "noite quente slowed" → the normal song. | An altered request picks among altered results only. *(Found by the live regression run.)* |
| 25 | The AI-skip shortcut accepted *any* altered version for an altered request. | "…slowed" could get a "(Sped Up)" upload. | The top result must be the kind asked for, otherwise the AI decides. |
| 26 | AI decisions were cached per search for everyone, including ones shaped by one user's history. | Your history influencing someone else's pick. | Personalised decisions are cached per user. |
| 27 | The first song `/autoplay` played wasn't recorded in autoplay's listen/skip memory. | Autoplay learning from every song but the first. | Recorded like the rest. |
| 28 | Spotify links that fail (Spotify refusing lookups) said "Nothing found." | Looked like the song doesn't exist. | Explains the Spotify refusal and suggests searching by name. |

## Discord UI

| # | Problem | What you'd have seen | Fix |
|---|---|---|---|
| 29 | `/reset-algo` dropdown used song keys as values; Discord caps values at 100 characters and SoundCloud/other URLs are longer. | The algo menu failing to open. | Values are positions in the menu's own list. |
| 30 | Clearing the `/control` queue dropdown kept the old selection. | Jump To / Remove acting on a song you deselected. | Clearing clears the selection. |
| 31 | `/settings` right after a restart built a worker dropdown with zero options. | `/settings` failing for a few seconds after restart. | Worker options hidden until workers exist. |
| 32 | Song titles containing `[` `]` ("[Official Video]") ended the Markdown link early. | Broken clickable titles in Now Playing / queued embeds. | Brackets escaped (worker and main embeds). |
| 33 | One invalid entry in `dev_ids` / `whitelist` raised on every message. | Bot erroring on all messages after a settings typo. | Invalid entries are skipped and logged once. |

Also two cleanups with no behaviour change: the watchdog's leftover indentation, and skipping status/idle calls for a session that already ended.

---

## Checks that came back clean

- `ruff` with bug-finding rules (bugbear, async, error-handling): only cosmetic notes (`zip()` without `strict=`, one unused loop variable).
- Live regression (real Lavalink/YouTube/Groq, copy of your history): "noite quente" → Flame Runner, "blinding lights" → The Weeknd, "noite quente slowed" → SUPER SLOWED, "noite quente sped up" → Sped Up, "trap queen" → Fetty Wap, "noite quente m22" → M22; autoplay → 5 related picks; Spotify link → clear message.

## Not changed (outside the bot's code)

- Spotify lookups fail until the Spotify app owner in Lavalink's config has Premium.
- YouTube bot-checking is handled by your yt-dlp cookies + bgutil PO-token setup.
