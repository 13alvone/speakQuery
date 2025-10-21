# PyCharm Remote Debugging for SpeakQuery

The repository now ships with a reusable debugger hook that can attach any
Python process (including subprocesses launched from shell scripts) to a PyCharm
Debug Server.  The integration is disabled by default and is activated only when
`PYCHARM_ATTACH` is set to a truthy value.

## Prerequisites

1. **PyCharm Professional** with the *Python Debug Server* ("Run → Attach to
   Process" → *Python Debug Server*).
2. Install the matching helper package on the machine running SpeakQuery:

   ```bash
   pip install pydevd-pycharm
   ```

   > PyCharm also ships the helper under *Settings → Build, Execution,
   > Deployment → Python Debugger → Gear Icon → "Download debug egg"* if
   > direct internet access is unavailable.

3. Allow the debugger port through any local firewalls.

## Configuring the PyCharm Debug Server

1. Open **Run → Edit Configurations...**.
2. Press **+** and choose **Python Debug Server**.
3. Pick a listening port (e.g. `5678`) and enable "Use IPython if available" if
   desired.
4. For Docker/WSL targets set the *Host* to an address reachable from the
   container (e.g. `host.docker.internal` on macOS/Windows or the host IP on
   Linux).
5. Start the configuration; PyCharm will wait for incoming connections.

## Runtime Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `PYCHARM_ATTACH` | `0` | Enables the auto-attach hook when truthy. |
| `PYCHARM_DEBUG_HOST` | Auto-detected (`host.docker.internal` in Docker, otherwise `127.0.0.1`) | Hostname/IP of the PyCharm Debug Server. |
| `PYCHARM_DEBUG_PORT` | `5678` | Debug server TCP port. |
| `PYCHARM_DEBUG_SUSPEND` | `0` | When truthy, pauses the process on start until PyCharm resumes it. |
| `PYCHARM_DEBUG_REDIRECT_OUTPUT` | `0` | Mirror stdout/stderr back to PyCharm. |
| `PYCHARM_DEBUG_PATCH_MULTIPROCESSING` | `1` | Ask `pydevd` to patch `multiprocessing` so forks connect automatically. |
| `PYCHARM_DEBUG_TRACE_CURRENT_THREAD` | `0` | Limit tracing to the current thread only. |

> The hook performs strict validation and logs warnings for invalid values; it
> never raises fatal errors even if PyCharm is unreachable.

## CLI & Shell Launchers

All Python entry points now load `devtools/debug_attach.py` through
[`sitecustomize.py`](../sitecustomize.py).  To launch the entire stack with
remote debugging enabled:

```bash
PYCHARM_ATTACH=1 \
PYCHARM_DEBUG_HOST=host.docker.internal \
PYCHARM_DEBUG_PORT=5678 \
./run_all.sh
```

Each launcher prints a reminder when attach mode is active so you can confirm
that the correct host/port will be used.

For single commands simply prefix the environment variables:

```bash
PYCHARM_ATTACH=1 python query_engine/QueryEngine.py
```

## Docker Workflows

Both the `docker-compose` service and `dev_docker_quick_reset.sh` pass through
all debugger-related variables.  Example Compose usage:

```bash
PYCHARM_ATTACH=1 \
PYCHARM_DEBUG_HOST=host.docker.internal \
PYCHARM_DEBUG_PORT=5678 \
docker-compose up --build
```

The values can also be defined in your `.env` file.  On native Linux hosts you
may need to replace `host.docker.internal` with the actual host IP (e.g.
`172.17.0.1`).

## Multi-process Notes

- `patch_multiprocessing` is enabled by default to automatically trace Python
  workers created via `multiprocessing` or `concurrent.futures`.
- Processes spawned via `fork` inherit `PYCHARM_ATTACH` and therefore run the
  hook again during interpreter start-up.
- Set `PYCHARM_DEBUG_PATCH_MULTIPROCESSING=0` if you prefer to avoid
  monkey-patching and attach child processes manually.

## Platform-specific Guidance

- **Windows / WSL**: use the Windows-side PyCharm Debug Server and point the
  Linux environment at the Windows host IP (often `host.docker.internal`).
- **macOS**: `host.docker.internal` resolves automatically for Docker Desktop and
  local shells.
- **Linux**: Docker does not map `host.docker.internal` by default; add it via
  `/etc/hosts`, Docker's `--add-host` flag, or set `PYCHARM_DEBUG_HOST` to the
  output of `ip addr show docker0`.

## Verification Checklist

1. Run the unit tests covering the attach hook:

   ```bash
   pytest tests/test_debug_attach.py
   ```

2. Start PyCharm's debug server and execute a short script to confirm the
   automatic attach (a `sleep` keeps the process alive long enough to connect):

   ```bash
   PYCHARM_ATTACH=1 \
   PYCHARM_DEBUG_HOST=host.docker.internal \
   PYCHARM_DEBUG_PORT=5678 \
   python -c "import time; time.sleep(10)"
   ```

   PyCharm should list the process under *Debugger* with both stdout/stderr
   streaming (when `PYCHARM_DEBUG_REDIRECT_OUTPUT=1`).

3. For multiprocessing scenarios, run:

   ```bash
   PYCHARM_ATTACH=1 \
   PYCHARM_DEBUG_HOST=host.docker.internal \
   PYCHARM_DEBUG_PORT=5678 \
   python - <<'PY'
   import multiprocessing as mp
   import time
   def worker():
       time.sleep(10)
   if __name__ == "__main__":
       mp.Process(target=worker).start()
       time.sleep(10)
   PY
   ```

   You should observe both parent and child processes appear in PyCharm when
   `patch_multiprocessing` remains enabled.

