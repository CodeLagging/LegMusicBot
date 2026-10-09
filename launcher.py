"""systemd entry point for the music bots.

Keeps `systemctl restart` / `systemctl stop` instant while the bot shuts down gracefully:

- The launcher starts `main.py --server` as a child and waits for it.
- On stop, systemd signals only the launcher (unit has KillMode=process). The launcher asks the bot
  to shut down over .main.sock, waits for its ACK and exits. systemd considers the stop done, so
  systemctl returns right away, while the bot keeps finishing songs, plays shutdown.mp3 and exits
  by itself.
- On start, if a previous bot is still finishing (its PID is in .bot.pid), the launcher waits for
  it to exit before starting a new one.
- If the bot exits on its own (crash), the launcher exits with the same code, so Restart=on-failure
  still applies.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR  = Path(__file__).parent
PID_FILE    = SCRIPT_DIR / ".bot.pid"
MAIN_SOCKET = SCRIPT_DIR / ".main.sock"

# A previous bot finishing songs takes at most ~7.5 min (5 min wait + messages + shutdown).
OLD_BOT_WAIT = 540
ACK_TIMEOUT  = 10

_stop_requested = False


def log(msg: str) -> None:
    print(f"[Launcher] {msg}", flush=True)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _previous_bot() -> int | None:
    """PID of a bot from an earlier launcher that is still running, if any."""
    try:
        pid = int(PID_FILE.read_text().strip())
    except (OSError, ValueError):
        return None
    if not _alive(pid):
        return None
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes()
        cwd     = Path(os.readlink(f"/proc/{pid}/cwd")).resolve()
    except OSError:
        return None
    # A stale .bot.pid (bot killed with -9) can point at a reused PID. Only treat it as our bot if
    # it's main.py running from this folder, so an unrelated program is never waited on or killed.
    if b"main.py" in cmdline and cwd == SCRIPT_DIR.resolve():
        return pid
    return None


def _ask_bot_to_stop(pid: int) -> None:
    """Ask the bot to shut down gracefully and wait for its ACK; fall back to SIGTERM
    (which the bot also handles gracefully) if it doesn't answer."""
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(ACK_TIMEOUT)
            s.connect(str(MAIN_SOCKET))
            s.sendall(b'{"op": "shutdown"}\n')
            reply = json.loads(s.makefile().readline() or "{}")
        if reply.get("status") == "ok":
            log(f"Bot (PID {pid}) acknowledged — it finishes current songs in the background")
            return
    except Exception as exc:
        log(f"No ACK from the bot ({exc}) — sending SIGTERM instead")
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass


def _on_stop_while_waiting(signum, frame) -> None:
    global _stop_requested
    _stop_requested = True


def main() -> int:
    signal.signal(signal.SIGTERM, _on_stop_while_waiting)
    signal.signal(signal.SIGINT, _on_stop_while_waiting)

    old = _previous_bot()
    if old:
        log(f"Previous bot (PID {old}) is still finishing songs — waiting for it to exit")
        deadline = time.monotonic() + OLD_BOT_WAIT
        while _alive(old) and time.monotonic() < deadline and not _stop_requested:
            time.sleep(1)
        if _stop_requested:
            log("Stopped while waiting — not starting a new bot")
            return 0
        if _alive(old):
            log(f"Previous bot (PID {old}) didn't exit in {OLD_BOT_WAIT}s — killing it")
            try:
                os.kill(old, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            time.sleep(2)

    # Output goes straight to the journal, and keeps going there after the launcher exits.
    bot = subprocess.Popen([sys.executable, "-u", str(SCRIPT_DIR / "main.py"), "--server"],
                           cwd=str(SCRIPT_DIR))
    log(f"Started bot (PID {bot.pid})")

    def on_stop(signum, frame):
        if bot.poll() is None:
            _ask_bot_to_stop(bot.pid)
        os._exit(0)   # don't wait for the bot: that's what makes systemctl return immediately

    signal.signal(signal.SIGTERM, on_stop)
    signal.signal(signal.SIGINT, on_stop)

    code = bot.wait()
    log(f"Bot exited by itself with code {code}")
    return code


if __name__ == "__main__":
    sys.exit(main())
