# xgfs

`xgfs` is a Go-based virtual filesystem that sits between user interfaces (CLI, HTTP, S3, NFS, FUSE) and one or more storage backends (local blobs, S3/OSS/COS, hybrid mirrors). It keeps inode-style metadata, deduplicates data at the shard level, optionally encrypts shards with AES-256-CTR, and exposes POSIX-flavoured operations everywhere.

---

## Features

- **Unified metadata graph** – numeric inode IDs (root = `1`), copy-on-write shard lists, reference tracking, and background garbage collection.
- **Pluggable storage** – local `PathStore`, `RemoteStore` (HTTP/S3 compatible), or `HybridStore` that mirrors data across tiers and caches cold reads back to primary.
- **Multiple frontends** – built-in CLI plus HTTP, S3-compatible, NFS, and FUSE servers powered by the same virtual filesystem.
- **Shard-level encryption** – centralized `pkg/encryption` module (AES-256-CTR today) with configurable method/key exposed via CLI/config.
- **POSIX-aware middleware** – all access paths run through `pkg/vfs.FS`, which enforces permissions, maintains metadata caches, and exposes higher-level helpers (chmod/chown/rename/mkfifo, etc.).

---

## Repository Layout

| Path | Purpose |
|------|---------|
| `cmd/xgfs` | Cobra-based CLI plus server commands (`serve-http`, `serve-s3`, `serve-nfs`, `mount-fuse`). |
| `pkg/fs` | Interfaces shared by every backend (`Fs`, `Object`, `Directory`, `IOOptions`, `ID`). |
| `pkg/vfs` | Virtual filesystem wrapper; injects `vfs.User` context and surfaces POSIX helpers. |
| `pkg/localfs` | Concrete backend that combines metadata + blob stores, handles chunking, locking, and GC. |
| `pkg/meta` | Metadata stores (memory, JSON file, BoltDB), ref tracker, migrations. |
| `pkg/blob` | Blob stores (`PathStore`, `RemoteStore`, `HybridStore`) plus supporting options. |
| `pkg/sharder` | Chunk/concat logic with concurrency + encryption awareness. |
| `pkg/encryption` | Shared crypto primitives (methods, keys, helpers). |
| `pkg/server/*` | HTTP, S3, NFS, FUSE servers and middleware. |
| `pkg/gc` | Reference-tracked garbage collector shared by localfs + CLI `gc` command. |
| `docs/` | Design notes + `CODEX_OVERVIEW.md` (operator guide). |
| `AGENTS.md` | Contribution standards: formatting, testing, commit style. |

---

## Quick Start

### 1. Build the CLI

```bash
go fmt ./...
go build ./cmd/xgfs
```

### 2. Create a config (TOML or YAML)

```toml
# xgfs.toml
root = "/var/lib/xgfs/blobs"
storage_provider = "local"
chunk = 4                         # MiB shards

encrypt = true
encryption_method = "aes-256-ctr"
key = "0123456789abcdeffedcba98765432100123456789abcdeffedcba9876543210" # 32-byte hex

meta = "/var/lib/xgfs/meta.db"    # BoltDB/JSON path depending on config
meta_cache_size = 2048
meta_cache_ttl = "5s"
```

The CLI automatically loads `./xgfs.toml|yaml` or `~/.config/xgfs/xgfs.*`. Flags override config values, and every option is also exposed as `XGFS_*` environment variables (e.g. `XGFS_ROOT`).

### 3. Basic CLI usage

```bash
./xgfs ls /
./xgfs put /docs/README < README.md
./xgfs cat /docs/README
./xgfs cp /docs/README /docs/README.bak
./xgfs gc  # manually trigger garbage collection
```

When encryption is enabled, the logical file size stays consistent while shards on disk include IV overhead; metadata stores track both.

---

## Running Servers

All servers reuse the configured filesystem. Common flags: `--api-key`, `--rate-limit <ops>`, `--rate-window <duration>`.

```bash
# HTTP gateway (LIST/GET/PUT/PATCH)
./xgfs serve-http --listen :8080 --api-key secret123

# S3-compatible gateway (SigV4 signing)
./xgfs serve-s3 --listen :9000 --api-key secret123

# NFS export (go-nfs) with export root=/ and handle cache size 4096
./xgfs serve-nfs --listen :2049 --export /

# Mount via FUSE
./xgfs mount-fuse --mountpoint /mnt/xgfs
```

Hybrid deployments can specify `--hybrid-provider`, `--hybrid-endpoint`, `--hybrid-mirror`, and `--hybrid-cache-read` to mirror shards to a secondary store (e.g. S3) while keeping a hot local copy.

---

## Configuration Reference

| Flag / Key | Description |
|------------|-------------|
| `--root` / `storage_provider` | Root directory for local blobs and the primary provider name (`local`, `s3`, `oss`, `cos`). |
| `--storage-*` (endpoint, bucket, region, access/secret/session keys) | Provider-specific attributes for remote stores. |
| `--chunk` | Shard size in MiB passed to the sharder. |
| `--encrypt`, `--encryption-method`, `--key` | Enable shard encryption and select the method (currently `aes-256-ctr`). Key must be 32-byte hex. |
| `--meta` | Metadata file/Bolt store location. |
| `--hybrid-*` | Configure secondary blob store and mirroring behavior. |
| `--meta-cache-size`, `--meta-cache-ttl` | Tune the virtual filesystem metadata cache (0 disables). |
| `--config` | Load an explicit TOML/YAML file (defaults are `./xgfs.*` and `~/.config/xgfs/xgfs.*`). |

All settings are mirrored to environment variables (e.g. `XGFS_STORAGE_PROVIDER`).

---

## Development & Testing

1. Follow `AGENTS.md` (Go style, `gofmt`, tests, Conventional Commits).
2. Run the suite: `GOCACHE=$(pwd)/.gocache go test ./...`
3. Optional lint: `golangci-lint run`
4. Never bypass `pkg/vfs`; CLI and servers rely on it for user context and permission checks.

Useful packages for contributors:

- `pkg/xerrors`: canonical error kinds surfaced by HTTP/S3/NFS layers.
- `pkg/server/middleware`: API key + rate limiting shared by all network servers.
- `pkg/gc`: background sweeper wiring; invoked automatically by `localfs` and manually via `xgfs gc`.

---

## Community & Contributions

Issues, bug reports, and feature requests are welcome. When contributing:

- Keep files under ~300 lines when possible, prefer table-driven tests, and document exported symbols.
- Mention user-facing impact in PR descriptions and include manual and automated test evidence.
- If your change affects storage layout or protocols, update `docs/CODEX_OVERVIEW.md` and this README accordingly.

Happy hacking!
