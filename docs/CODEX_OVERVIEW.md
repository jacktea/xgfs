# xgfs – Codex Operator Notes

This document captures the working context Codex agents need so new sessions can ramp quickly without rediscovering the codebase.

## High-Level Intent

- Project goal: a Go implementation of an rclone-style virtual filesystem (`github.com/jacktea/xgfs`) that abstracts multiple storage backends (local blobs, S3/OSS/COS, hybrid tiers) and exposes multiple access frontends (CLI, FUSE, HTTP, NFS, S3-compatible gateway).
- Key guarantees: inode-style metadata, shard-level deduplication/encryption, copy-on-write cloning, concurrent I/O, caching tiers, and reference-tracked garbage collection.

## Repository Layout

- `cmd/xgfs`: primary CLI (list/cat/put/copy) for interacting with configured backends.
- `pkg/fs`: shared filesystem contracts (`Fs`, `Object`, `Directory`, metadata structs, `IOOptions`).
- `pkg/vfs`: virtual filesystem shim that wraps any `fs.Fs`, handles user context, and exposes the `PosixFs` contract (passthrough today, caching/policy home tomorrow).
- `pkg/localfs`: concrete backend combining `meta.Store` + `blob.Store`; implements deduped shard I/O, hybrid mirroring, caching.
- `pkg/meta`: metadata stores (memory, file-backed) plus ref-tracker for shard GC.
- `pkg/blob`: blob stores; includes local `PathStore`, remote store w/ cache, hybrid wrappers; shard IDs map to content-addressed files.
- `pkg/sharder`: chunking/concat logic with concurrency + optional AES-CTR encryption.
- `pkg/server/*`: access layers — `fuse` (go-fuse v2), `httpapi`, `nfs`, `s3gw`.
- `AGENTS.md`: repository guidelines (Go formatting, commands, naming, testing expectations).

## Build & Test

- Format: `go fmt ./...` (required before review).
- Compile CLI: `go build ./cmd/xgfs`.
- Run tests: `GOCACHE=$(pwd)/.gocache go test ./...` (env var avoids sandbox cache dirs). Individual packages support standard `-run` filters.
- Optional: `golangci-lint run` if tooling is available locally.

## Filesystem Abstractions

- `fs.Fs` exposes storage-focused operations (stat/create/mkdir/remove/list/link/copy) plus feature flags; backends implement only what they can truly guarantee.
- `vfs.FS` wraps any backend `fs.Fs`, forwarding calls today and eventually layering caching, hybrid coordination, and POSIX shims; it always satisfies `vfs.PosixFs`.
- `fs.Object` now supports both streaming reads (`Read` → `io.WriterAt`) and random access (`ReadAt`) to enable chunked concurrency.
- `IOOptions` allows callers to specify shard size, concurrency, and cache hints to propagate down the pipeline.
- CLI flag `--meta-cache-size` controls the metadata cache inside `vfs.FS`; set it to zero to disable or tune TTL via `--meta-cache-ttl`.
- Global CLI flags may be specified in a config file (`xgfs.yaml`/`xgfs.toml`) or via `--config <path>`; Viper also honors `XGFS_*` environment variables matching the flag names.

## Storage & Metadata Stack

1. **Metadata (`pkg/meta`)**
   - Tracks inode trees, shard refs, and link counts.
   - `RefTracker` increments/decrements shard references so unused shards can be garbage-collected by blob stores.
2. **Blob Stores (`pkg/blob`)**
   - `PathStore`: local fan-out directories (`root/aa/bb/hash`) to avoid huge flat folders.
   - `RemoteStore`: wraps HTTP/S3-like stores with shard-level in-memory caching for reads.
   - `HybridStore`: mirrors writes to a secondary store and can cache-on-read back into primary.
3. **Sharder (`pkg/sharder`)**
   - `ChunkAndStore`: splits incoming streams into fixed-size chunks, hashes for dedupe, optional AES-CTR encryption, uploads via goroutine pools.
   - `Concat`: fetches shards concurrently, writing directly into provided `WriterAt`.

## Garbage Collection

- `meta.RefTracker` now queues zero-ref shards instead of deleting eagerly; `pkg/gc.Sweeper` coordinates metadata + blob stores so deletions happen atomically across tiers.
- `localfs` starts a background sweeper (30s interval by default) and exposes `RunGC` + CLI command `xgfs gc` for manual runs or cron-style invocations.

## Local Backend (`pkg/localfs`)

- Resolves metadata + blob stores from config (local paths, injected stores, hybrid options).
- Maintains metadata cache (LRU) for faster stats.
- Implements Copy-on-Write by referencing shard lists across objects until modified.
- Handles writes by chunking through the sharder; handles reads/read-at through shard-aware concatenation.
- Supports `Link` (hard) and `LinkKind` (symlink) semantics, consistent with metadata store.
- Exposes `PosixAdapter()` so `vfs.FS` can opt into native rename/chmod/mkfifo implementations without requiring callers to depend on `localfs` directly.
- `HybridOptions` integrate with `blob.HybridStore` to mirror shards to remote/cloud tiers and optionally hydrate local cache on reads.

## Access Layers

- **CLI (`cmd/xgfs`)**: commands (`ls`, `cat`, `put`, `cp`, `chmod`, `chown`, `rename`, `mkfifo`, `gc`) run against a `vfs.FS` wrapper, so POSIX operations always pass through the same layer; identity is sourced from the invoking OS user.
- **FUSE (`pkg/server/fuse`)**: go-fuse v2 server mapping real filesystem operations to `fs.Fs` calls; uses `ReadAt`/`WriteAt` for kernel-driven offsets.
- **HTTP API (`pkg/server/httpapi`)**: lightweight REST bridging HTTP verbs to fs actions; now requires a `vfs.Filesystem`, so chmod/rename/mkfifo always route through the same layer.
- **NFS (`pkg/server/nfs`)**: backed by `github.com/willscott/go-nfs`; exports any `vfs.Filesystem` via a billy adapter, supports API-key/rate limits via shared middleware, and exposes `--export`/`--handle-cache` flags.
  - The adapter now calls `vfs.PosixFs.Rename/SetAttr` directly, so NFS clients see atomic renames and metadata changes without copy/delete fallbacks.
- **S3 Gateway (`pkg/server/s3gw`)**: protocol adapter exercised by smoke tests; now hard-requires a `vfs.Filesystem` so metadata headers and POST rename can trust the POSIX layer.
  - Supports PUT metadata headers `X-Amz-Meta-Posix-{Mode|Uid|Gid}`, exposes the same headers on GET/HEAD, and accepts `POST /object?rename=/new/path` to delegate `vfs.PosixFs.Rename`.

## Protocol Middleware

- `pkg/server/middleware` provides shared HTTP middleware (API-key auth, token-bucket rate limiting); HTTP, S3, and NFS servers opt-in via CLI flags.
- HTTP now supports PATCH/POST to call chmod/chown/rename/mkfifo on any backend implementing `vfs.PosixFs`, in addition to paginated directory listings; S3 `list-type=2` honors `max-keys` and emits continuation tokens for smoke tests.
- CLI flags `serve-http`/`serve-s3`/`serve-nfs` expose `--api-key`, `--rate-limit`, `--rate-window`, and export/cache bounds so deployments can align with front-door policies.

## Caching & Concurrency

- Metadata cache: configurable entry count + TTL in `localfs.Config`.
- Blob cache: remote store keeps hot shards in-memory; hybrid store can persist fetched shards locally.
- I/O pipeline relies on `IOOptions.Concurrency` to decide the shard worker pool size for both uploads and downloads; `WriterAt` contract enables zero-copy concurrent downloads.

## Development Tips for Codex Agents

- Always consult `AGENTS.md` for Go style, testing, and hybrid-storage expectations.
- When touching blob layout logic, update `pkg/blob/pathstore_test.go` (regression coverage expects directory fan-out).
- For new fs.Object implementations, wire up `Read`, `ReadAt`, `Write`, `WriteAt`, `Truncate`, `Flush` consistently so frontends (FUSE/HTTP) stay compatible.
- FUSE tests require IPv4-bound listeners inside the sandbox; existing tests already account for this—mirror the approach.
- Use `newSliceWriter`/`writerAtBuffer` helpers in tests when you need an `io.WriterAt` sink; avoids re-implementing buffers.

## Known Gaps / Future Work

- Hybrid writes do not retry per-shard; add circuit-breakers + idempotent retry envelopes so remote outages do not wedge local commits.
- Metadata compaction and backup tooling is manual; we need a `meta snapshot` command plus WAL-style recovery tests in `pkg/meta`.
- End-to-end tests only cover localfs; introduce integration cases that stand up hybrid tiers (fake S3) and exercise cache rehydration + mirror drift reconciliation.

Keep this file up to date whenever major architectural shifts land so future Codex sessions can bootstrap quickly.
