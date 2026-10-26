# OpenCode Session Sync for Hermes

A separate, manually invoked Hermes plugin for importing and refreshing local OpenCode conversations.

```sh
hermes opencode-sync
```

No cron job, background source watcher, model prompt or two-way sync is installed.

## Install

Requirements: Hermes **0.22.5 or newer** with the canonical gateway/native-owner API (manifest floor `>=0.22.5`), Python 3.10+, and a local OpenCode SQLite database. Linux and macOS are supported; Windows users should run inside WSL. This version requires the gateway's primary profile and `plugins.isolation: in_process`. It fails closed rather than opening another database writer when the required owner is unavailable or the API is served in an isolated secondary plugin host.

Before catalog approval:

```sh
hermes plugins install https://github.com/cryptobredda/hermes-opencode-sync
hermes plugins enable opencode-sync
hermes opencode-sync --dry-run
hermes opencode-sync
```

After a maintainer reviews and merges the catalog entry, the short install name will be `opencode-sync`. This repository being submitted does not mean it is approved.

## Commands

```sh
hermes opencode-sync --dry-run
hermes opencode-sync --directory .
hermes opencode-sync --session ses_EXACT_SOURCE_ID
hermes opencode-sync --source /path/to/opencode.db
hermes opencode-sync --json
```

With no filter, the source is `$XDG_DATA_HOME/opencode/opencode.db`, falling back to `~/.local/share/opencode/opencode.db`. `--directory` means an exact canonical directory, not neighboring paths or descendants. Source identifiers are validated before lookup and never corrected or case-normalized.

## What carries over

- New chats and updates to previously imported chats, without repeated copies of unchanged history.
- Titles, working directories, ordered user/assistant history, reasoning, tool-call/result records, compaction state and inline attachments.
- Stable logical IDs and deterministic message UIDs. Long histories occupy linked storage segments. Native local routing is advanced with a new segment, so a previously opened logical ID can still reach the updated context.
- Existing Hermes model choices, custom titles, pins, archives and hidden flags. A new segment inherits those operator choices.

System/control events are archived history, not new system instructions. Inline attachments and unusually large outputs are stored as content-addressed private files. External file/URL references are not downloaded or copied from unrelated project directories.

## Conflict and busy protection

**If you continued or edited the imported copy in Hermes, it is left intact and reported as a conflict.** This plugin does not guess how to merge two independently edited timelines. There is no `--force` switch that discards Hermes work.

Running or queued Hermes turns, workers and transcript/compression leases are guarded. Retry a busy import after the turn finishes. Source-side removed or rewound rows are retained as inactive archival rows rather than erasing their imported bytes. Imports deliberately deleted in Hermes are not silently resurrected. Moving the working directory of an already bound Hermes chat requires an explicit Hermes workspace move first.

Run results use exit code `0` for complete, `2` for preserved conflicts/busy/per-chat failures, and `1` for source/setup/transport failure. Details are printed and recorded in `last-run.json`. A dry run creates neither the Hermes database nor sync state.

## Owner-side update architecture

The source is held in a consistent read-only SQLite transaction. An SQLite authorizer limits reads to history/schema tables and denies credential/account tables. No OpenCode configuration or login state is rewritten. Changes arriving after that snapshot wait for your next run.

Writes go to a namespaced backend API on the existing profile gateway, authenticated with a private same-user native owner ticket obtained through Hermes' control socket. The route refuses non-native requests. The CLI never opens a second writable Hermes database connection. Each changed segment is verified on the writer connection and then read back by the client. A before-update compressed backup and per-segment checkpoints allow safe retries after interrupted runs.

### Internal API disclosure / compatibility limitation

The CLI registration and dashboard API mounting are public plugin surfaces, but the guarded writer currently calls **undocumented gateway/storage helpers** (`hermes_cli.gateway_client`, owner context and mutation tracking, lease guards, import bindings, native lineage advancement and row serialization). Catalog validation reports these as warnings. They can change on any Hermes release. The code does not replace, wrap or rebind core functions or tables, and it does not bypass the owner or write through an isolated host. A maintainer may require a stable public imported-history update seam before accepting this listing; that limitation is explicitly disclosed in the catalog PR.

No source files or TUI bundles are patched. An already-open chat may need to be reselected from `/sessions` after syncing; this plugin does not inject a TUI refresh handler. After updating the plugin itself, restart the gateway to load the updated backend API.

## Local data and privacy

Under the active Hermes home, `imports/opencode-sync/` holds:

- `state-<source-path-hash>.json`: source digests, logical-to-segment mapping and last-imported fingerprints;
- `backups/<source-session-ID>/*.json.gz`: pre-update segment snapshots;
- `assets/`: content-addressed attachment/output bytes;
- `runs/*.json` and `last-run.json`: run outcomes and refusal details.

These files use private permissions. **Transcripts and backups can contain anything a user pasted into a chat, including incidental secrets.** Do not publish these files with bug reports. Keep the state file and referenced assets. Existing initial-import manifests under `imports/opencode-*/` can seed older installations; new users need no such manifest. Full multi-gigabyte source databases are not recopied every run.

### Security / side-effect disclosure

- Reads the selected OpenCode database and relevant history only; login/credential table reads are denied. Does not read `.env`, `auth.json`, browser profiles, keychains or another tool's login files.
- Reads only imported Hermes histories for provenance/conflict checks, through the selected profile's SDK handle. Writes verified import-owned rows through that profile's live owner, retaining backups and guarded native routing.
- Makes loopback calls to the selected Hermes owner API. It makes no third-party network calls and runs no imported tools, shell commands, generations or paid jobs. Starting the normal profile gateway when absent is delegated to Hermes' own runtime API.
- Uses no provider credentials, creates no auth grants, and does not copy/refresh OAuth tokens. Its one-use local ticket is obtained from Hermes rather than a credential file.
- No telemetry, updater, model-facing tool, agent hook or middleware is registered. `register(ctx)` only registers the CLI command. Backend code is enabled through Hermes' normal plugin consent gate.

## Development

```sh
python -m unittest discover -s tests -v
hermes plugins validate . --install-deps
```

Codec/source tests run with the standard library alone. Owner/storage integration tests require Hermes installed/importable; without it they are explicitly skipped, not reported as passed. The suite exercises read-only credential-table exclusion, append/update idempotency, Hermes-side edit refusal, busy leases, malformed identities, native routing, preserved metadata, partial-run recovery and a new-profile dry run with no database/gateway. The CI compatibility job pins a real Hermes release commit. No live user database or source credentials are used in tests.

MIT licensed. Independent community integration; not affiliated with or endorsed by OpenCode or Nous Research.
