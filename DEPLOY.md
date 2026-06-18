# Deploying the labeler

The labeler and the event ingester run as systemd services on a single Linux
host. Deploying means: pull the new source, build a release binary, swap it in
over the running one, and restart the service. There is no CI/CD pipeline — this
is a manual operation over SSH.

> **Connection details** (host, SSH user, sudo password) are shared privately —
> ask tolf. They are deliberately kept out of this public repo. The rest of this
> doc assumes you're already SSH'd into the box.

## How it's laid out on the host

- **Services** (systemd, run as the `fbl` user, `Restart=always`):
  - `fbl-bsky-labeler.service` → `/home/fbl/bsky-labeler`
  - `fbl-bsky-event-ingester.service` → `/home/fbl/bsky-event-ingester`
  - The deployed binaries in `/home/fbl/` are **root-owned**, so swapping them
    needs `sudo`.
- **Source** is a Cargo workspace at `~/consfyi/` with two members, each its own
  git checkout:
  - `~/consfyi/bsky-labeler/` (this repo)
  - `~/consfyi/event-ingester/` (the ingester repo)
  - The workspace root `Cargo.toml` / `Cargo.lock` are local to the box and are
    **not** version-controlled.
- The labeler binds `127.0.0.1:3001` and is fronted by nginx at
  `bsky-labeler.cons.fyi`. It connects to Postgres via `postgres:///fbl` (peer
  auth), the same DB the ingester writes the firehose cursor into.

## Deploy

Run from `~/consfyi`. This builds only the member you pulled, verifies the
binary exists before touching production, keeps a `.bak` for rollback, and swaps
atomically (no window where the binary is missing). To deploy the **labeler**:

```bash
cd ~/consfyi/bsky-labeler && git pull --ff-only && \
cd ~/consfyi && cargo build --release && \
test -x target/release/bsky-labeler && \
sudo cp -p ~fbl/bsky-labeler ~fbl/bsky-labeler.bak && \
sudo install -m755 target/release/bsky-labeler ~fbl/bsky-labeler.new && \
sudo mv -f ~fbl/bsky-labeler.new ~fbl/bsky-labeler && \
sudo systemctl restart fbl-bsky-labeler && \
echo "=== deployed, tailing log (Ctrl-C to stop) ===" && \
sudo journalctl -fu fbl-bsky-labeler
```

For the **ingester**, substitute `event-ingester` for the checkout dir and
`bsky-event-ingester` everywhere else (binary name, unit name).

### Why it's written this way

- Everything is `&&`-chained, so a failed `git pull` or `cargo build` **stops
  before production is touched** — the binaries are only swapped if the build
  produced a runnable artifact (`test -x`).
- `cp -p … .bak` snapshots the currently-running binary first, so rollback is one
  command.
- `install` + `mv -f` swaps via an atomic rename. There is no `rm` of the live
  binary, so there's never a moment where the file is missing (the old
  rm-then-cp recipe could leave production with no binary if the copy failed).
- Only the changed service is rebuilt/redeployed, keeping the blast radius small.

## Verify

A healthy startup tails clean (no panic/backtrace, no restart loop — the service
should **stay** up, not restart every second). The recurring
`subscribeLabels … Connection reset` lines are normal subscriber churn, not an
error.

Then confirm the `/health` endpoint and the monitor:

```bash
curl -s -w '\n%{http_code}\n' https://bsky-labeler.cons.fyi/health
# expect: cursor_us=<n> lag_secs=<small>   then   200
```

`/health` returns **200** when the ingester's firehose cursor is fresh (lag ≤ 10
min) and **503** when it has stalled. A scheduled GitHub Action
(`.github/workflows/monitor.yml`) hits it every 15 minutes; a red run is the
alert (GitHub emails repo watchers). You can also trigger it on demand from the
Actions tab (it has `workflow_dispatch`).

## Rollback

If the new binary misbehaves, restore the backup taken during deploy:

```bash
sudo mv -f ~fbl/bsky-labeler.bak ~fbl/bsky-labeler && sudo systemctl restart fbl-bsky-labeler
```

## Useful commands

```bash
sudo systemctl status fbl-bsky-labeler          # is it running?
sudo journalctl -fu fbl-bsky-labeler            # live logs
sudo systemctl restart fbl-bsky-labeler         # restart without redeploying
```
