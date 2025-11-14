# xgfs – Codex Operator Notes

Snapshot of the project for on-call Codex agents. Update this file whenever large architectural changes land so future sessions boot quickly.

## 1. Mission & Guarantees

- Goal: `github.com/jacktea/xgfs` is a Go-based virtual filesystem that sits between user-facing interfaces (CLI, HTTP, S3, NFS, FUSE) and multiple storage tiers (local blobs, S3/OSS/COS, hybrid mirrors).
- Guarantees:
  - POSIX-like metadata + inode graph with numeric IDs (root is `1`), ref-counted shards, and copy-on-write semantics.
  - Deduplicated shard storage with optional AES-256-CTR encryption via `pkg/encryption`.
  - Pluggable metadata stores (in-memory, JSON file, BoltDB) and blob stores (local path, HTTP/S3, hybrid).
  - Reference-tracked garbage collection coordinated between metadata and blob tiers.

## 2. Layer Cake Overview

```
CLI / Servers (cmd/xgfs, pkg/server/*)
        ↓
Virtual FS facade (pkg/vfs) enforcing user context & POSIX ops
        ↓
Local backend (pkg/localfs) = metadata store + blob store + caches
        ↓
Metadata stores (pkg/meta)    Blob stores (pkg/blob)
        ↓                          ↓
Sharder (pkg/sharder) ←→ Encryption (pkg/encryption) ←→ GC (pkg/gc)
```

- `pkg/fs`: canonical interfaces (`Fs`, `Object`, `Directory`, `IOOptions`). `fs.ID` is now `uint64`; JSON/Text marshalers still understand legacy strings (`"root"`, `"inode-7"`).
- `pkg/vfs`: wraps any `fs.Fs`, injects `vfs.User` context, enforces POSIX permissions, and exposes `PosixFs` for backends that need richer operations.
- `pkg/localfs`: orchestrates metadata + blob stores, maintains metadata cache, handles CoW shards, runs GC sweeper, and exposes a `PosixAdapter` used by CLI/servers.
- `pkg/meta`: memory/file/Bolt implementations all speak in numeric IDs. `RefTracker` feeds `pkg/gc.Sweeper` to delete orphaned shards once counts drop to zero.
- `pkg/blob`: concrete stores (`PathStore`, `RemoteStore`, `HybridStore`); all use `blob.PutOptions{Encryption: encryption.Options}` so encryption decisions are centralized.
- `pkg/sharder`: splits streams into fixed-size shards, uploads via worker pools, records both logical (`Size`) and physical (`StoredSize`) bytes, and drives `Concat` for reads.
- `pkg/encryption`: currently AES-256-CTR with IV headers; implement `Method` + `Key` combos here when adding algorithms instead of sprinkling crypto code elsewhere.

## 3. Access Surfaces

All frontends call into the same `vfs.Filesystem`, so behavior is consistent:

- **CLI (`cmd/xgfs`)** – commands: `ls`, `cat`, `put`, `cp`, `mkdir`, `chmod`, `chown`, `rename`, `mkfifo`, `gc`, plus `serve-http`, `serve-s3`, `serve-nfs`, `mount-fuse`. Flags are mirrored to Viper config (`xgfs.toml|yaml` + `XGFS_*` env).
- **HTTP (`pkg/server/httpapi`)** – REST-ish API with API-key + token-bucket middleware; supports PATCH/POST for chmod/chown/rename/mkfifo and Range GETs.
- **S3 gateway (`pkg/server/s3gw`)** – SigV4-compatible surface with metadata passthrough (`X-Amz-Meta-Posix-*`) and `POST ?rename=` semantics.
- **NFS (`pkg/server/nfs`)** – built on `go-nfs`, exports the same filesystem with billy adapters and middleware-provided auth/limits.
- **FUSE (`pkg/server/fuse`)** – go-fuse v2 server exposing the virtual FS locally; leverages `ReadAt`/`WriteAt` for kernel block IO.

HTTP/S3/NFS share middleware from `pkg/server/middleware` (API key, rate limiter).

## 4. Configuration Cheat Sheet

| Flag / Config Key | Purpose |
|-------------------|---------|
| `--root` / `storage_provider=local` | Directory for `PathStore` blobs (default `.xgfs/blobs`). |
| `--storage-provider` (`local`, `s3`, `oss`, `cos`) | Selects backing blob store; each provider has accompanying endpoint/bucket/region/keys. |
| `--chunk` | Shard size (MiB) fed to `pkg/sharder`. |
| `--encrypt` & `--encryption-method` | Enables shard encryption (`aes-256-ctr` today). Requires `--key` hex (32 bytes). |
| `--meta` / `--hybrid-root` | Metadata path for file/Bolt stores; root for secondary local mirror. |
| `--meta-cache-size`, `--meta-cache-ttl` | Configure `vfs` metadata cache. |
| Hybrid flags (`hybrid-provider`, `hybrid-mirror`, `hybrid-cache-read`) | Mirror writes to secondary store and optionally hydrate cache on reads. |
| Server flags (`--api-key`, `--rate-limit`, `--export`, `--handle-cache`, etc.) | Apply to HTTP/S3/NFS commands respectively. |

Remember to wrap every backend in `vfs.New(...)` (already done in CLI) so permission enforcement and caching stay consistent.

## 5. Build & Test Workflow

From repo root:

1. Format: `go fmt ./...`
2. Compile CLI: `go build ./cmd/xgfs`
3. Tests: `go test ./...` (or `GOCACHE=$(pwd)/.gocache go test ./...` when sandboxed). Targeted packages support `-run`.
4. Optional lint: `golangci-lint run` if available.

Before landing changes:

- Follow `AGENTS.md` for style, naming, testing (>80% coverage goal), and Conventional Commit messages.
- Keep `_test.go` beside source files; integration tests belong under `test/integration` with `//go:build integration`.
- Never bypass `vfs.FS`; CLI/servers rely on it for user context.

## 6. Implementation Notes & Pitfalls

- **IDs:** The entire tree now uses numeric `fs.ID`. Legacy serialized strings (`"root"`, `"inode-42"`) still parse via `fs.ID.UnmarshalText`, but new code should treat IDs as numbers. Root is constant `meta.RootID == 1`.
- **Shard sizes:** `sharder.Shard` tracks `Size` (plaintext) vs `StoredSize` (ciphertext). Metadata persists both so gc/accounting stays accurate even with encryption overhead.
- **Encryption:** Use `encryption.Options` everywhere; never manually write IVs. CLI exposes `--encryption-method` and `--key`. Local + remote stores automatically add/remove IV prefixes.
- **Hybrid writes:** `blob.HybridStore` uploads to primary, optionally mirrors to secondary, and can cache read-back shards. It intentionally reads entire objects into memory; plan accordingly.
- **GC:** `localfs` spins a background sweeper unless `DisableAutoGC` is set. There’s also an explicit `xgfs gc` command. If you add a new store type, ensure it implements the `blob.Store` interface so GC can delete shards.
- **POSIX ops:** Mutating APIs (chmod/chown/rename/mkfifo) should go through `vfs.PosixFs` to ensure hooks (locking, permission checks) run once. HTTP/NFS/S3 servers already do this.
- **Testing tips:** FUSE/NFS/HTTP tests rely on IPv4 loopback listeners; stick to the existing `newHTTPTestServer` helper when adding cases. Blob/layout changes must update `pkg/blob/pathstore_test.go` and remote store tests to keep expectations in sync.

## 7. Future TODOs

- Remote/hybrid write retries + better error surfaces.
- Snapshot/restore tooling for metadata stores.
- Integration suite that stands up hybrid tiers and verifies cache rehydration + permission checks end-to-end.

Refer back to this document at the start of every Codex engagement to avoid rediscovering these details.
