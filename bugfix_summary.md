# Bug scan — summary of fixes

Eight passes (plus two later fixes) over `main.py`, `worker.py`, `db.py`, `appsettings.py` and `launcher.py`: full read-throughs, scenario checks (races, restarts, Discord limits, Lavalink failures), a `ruff` bug-rule scan, and a final regression run of search and autoplay against the real Lavalink / YouTube / Groq. Every fix below was tested (fake players/workers for the failure cases, real services for search). **42 fixes**, plus two small cleanups.

Status: pushed to GitHub and deployed (passes 1–8).

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

## Final pass (8)

| # | Problem | What you'd have seen | Fix |
|---|---|---|---|
| 34 | wavelink only says "playing" once a song reaches Lavalink. Two quick `/play`s, or a `/play` during a song change, could both start a song. | **A song lost**: the second replaced the first (two "Now Playing" messages, one song gone). Reproduced in a test. | One song change at a time per session: the later `/play` waits a moment and is queued. |
| 35 | The watchdog treated a slow song change (slow YouTube lookup, ~1 min) as a stalled queue. | Same as above: a second song started next to the one loading. | Watchdog leaves sessions alone while a song change is running. |
| 36 | A song starting while nobody was in the channel cancelled the idle timer (everyone left mid song change, or a fixed channel nobody is in). | Bot playing to an empty channel forever, especially with autoplay on. | The idle timer keeps running when nobody is listening. |
| 37 | `/reset-algo` → Yes answered only after telling every worker to forget (up to 5 s); Discord gives 3 s. | "This interaction failed" although the reset worked. | The click is acknowledged first, then the message updates. |
| 38 | During a restart's "finish the song" wait, `/stop` and `/control` were refused like `/play`. | Couldn't stop or skip the last song for up to 5 minutes. | Only commands that start playback are blocked. |
| 39 | Dragging the bot to another channel left "🎵Playing - title" on the old one. | Stale status on an empty channel. | Status moves with the bot. |
| 40 | A typo in `algo_max_kb` / `autoplay_seed_count` (e.g. `null`) raised during song changes and while a session ended. | Songs not advancing; the bot not leaving voice. | Invalid values are logged once and the default is used. |

## Found later

| # | Problem | What you'd have seen | Fix |
|---|---|---|---|
| 41 | `/control`'s Stop, Autoplay and Control buttons had the same id in every version of the panel. Each refresh (song change, most button presses) retires the old version, and discord.py then forgot those ids for the message — including the new version's buttons. | **Stop, Autoplay and Control did nothing** after the panel had refreshed once ("This interaction failed"), with nothing in the logs. | Ids are unique per panel version. Every control was tested through discord.py's real dispatch with refreshes in between. Pause now shows ⏸/▶ and the panel shows "paused"; after Stop it says "Stopped". |
| 42 | A search word that's part of one result's artist name ("morena" vs the artist "Illest Morena") was treated as naming the artist, which switched off the user's history. | "morena" played "Thobela Morena" instead of LeoTHM's "MORENA" that you played 3 times. | A word is an artist name only when more results have it in the artist than in the title. "noite quente m22" still picks M22. |

Also fixed while adding lyrics: `requirements.txt` had `python-dotenv>=1.0.0yt-dlp` on one line (a missing line break), so `pip install -r requirements.txt` failed on a fresh install.

Also two cleanups with no behaviour change: the watchdog's leftover indentation, and skipping status/idle calls for a session that already ended.

---

## Checks that came back clean

- `ruff` with bug-finding rules (bugbear, async, error-handling): only cosmetic notes (`zip()` without `strict=`, one unused loop variable).
- Live regression (real Lavalink/YouTube/Groq, copy of your history): "noite quente" → Flame Runner, "blinding lights" → The Weeknd, "noite quente slowed" → SUPER SLOWED, "noite quente sped up" → Sped Up, "trap queen" → Fetty Wap, "noite quente m22" → M22; autoplay → 5 related picks; Spotify link → clear message.

## Not changed (outside the bot's code)

- Spotify lookups fail until the Spotify app owner in Lavalink's config has Premium.
- YouTube bot-checking is handled by your yt-dlp cookies + bgutil PO-token setup.
