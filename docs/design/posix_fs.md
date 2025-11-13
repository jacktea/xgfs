# POSIX Filesystem Overhaul

This document captures the Phase 1 design for layering `github.com/jacktea/xgfs/pkg/vfs`
on top of the backend contracts in `pkg/fs`, reworking metadata storage on top of BoltDB/Badger,
and paving
the way for protocol adapters (CLI/FUSE/HTTP/S3/NFS) to expose the new semantics.

## Goals

1. Applications should treat xgfs like a Unix filesystem: rename is atomic, chmod/chown/utimes
   persist, special files/locks behave predictably, and permissions are enforced per-user.
2. Metadata must survive crashes and scale to millions of inodes; transactions guarantee
   referential integrity (directories/inodes/shards). We standardise on BoltDB first (Badger
   remains an optional backend with the same store interface).
3. All front-ends (CLI/FUSE/HTTP API/S3 gateway/NFS) receive feature-parity with the CLI,
   including new commands/endpoints for chmod/chown/rename, plus advisory locks and xattrs.
4. Windows remains future work; design choices note the required shims but implementation
   focuses on Unix semantics.

## Expanded `vfs` Contract

The current interface (stat/create/mkdir/remove/list/link/copy) is insufficient. We will add:

| Capability | API surface |
|------------|-------------|
| File descriptors with flags/modes | `OpenFile(path string, flags OpenFlags, perm os.FileMode) (Object, error)` |
| Atomic rename | `Rename(oldPath, newPath, opts RenameOptions) error` |
| Attribute mutation | `SetAttr(path string, changes AttrChanges) error` |
| Permission checks | `Access(path string, mode AccessMode, u User) error` |
| Special files | `Mknod(path string, kind SpecialKind, perm os.FileMode, dev DeviceNumber, u User) error`, `Mkfifo`, `Socket` |
| Symlink/readlink | `Symlink`, `Readlink` |
| Hard links | `Link(source, target string, opts LinkOptions) error` |
| Advisory locks | `Lock(path string, opts LockOptions) error`, `Unlock(path string, opts LockOptions) error` |

New supporting types:

- `OpenFlags` bitmask (RDONLY/WRONLY/RDWR/APPEND/CREATE/EXCL/TRUNC/SYNC).
- `User` struct (uid, gid, supplementary gids) propagated via `context`.
- `AttrChanges` describing optional mode/uid/gid/size/atime/mtime/ctime/xattrs modifications.
- `RenameOptions` (overwrite bool, noReplace bool, exchange bool).
- `SpecialKind` enum (Regular, Directory, Symlink, FIFO, Socket, CharDevice, BlockDevice).
- `LockOptions` (exclusive/shared, blocking vs non-blocking, owner hints) for advisory locks.
- `InodeMetadata` replacing the minimal `Metadata`: includes IDs, link counts, uid/gid,
  permissions, timestamps, xattrs, device numbers, lock state and references to shard lists.

All existing fs implementations embed the new interface in phases: Phase 1 introduces
the types, Phase 2 wires them through `localfs`.

### Wrapper (`vfs.FS`) and Options

- `vfs.New(fs.Fs, vfs.Options)` is the canonical entry point frontends should use. The wrapper
  satisfies both `fs.Fs` and `vfs.PosixFs`, forwarding to the backend today and eventually
  injecting caching, hybrid mirroring behaviour, and POSIX emulation.
- `vfs.Options.PassthroughOnly` (added in Phase 2) documents that we currently just return
  `fs.ErrNotSupported` when the backend lacks native POSIX support; future revisions will
  toggle shims/heuristics instead of hard failures.
- Backends such as `localfs` expose `vfs.PosixProvider` to hand `vfs` a native adapter
  without forcing the backend type itself to satisfy `PosixFs`; this keeps storage-layer
  APIs minimal while still surfacing richer capabilities.

## Metadata Model

We treat metadata as an inode graph:

- **Inode record** (`inodes` bucket): key=`inodeID`, value=encoded `Inode` struct containing
  identity, mode, uid/gid, times, shard refs, link count, special fields, xattrs (map).
- **Directory entries** (`dir/<inodeID>` bucket): map child name → child inode ID. Directory
  inodes store only metadata; the directory listing lives in the bucket, ensuring rename is
  atomic under a Bolt transaction.
- **Parent sets** (`parents/<inodeID>` bucket) optional for reverse lookup; used for hard-link
  enumeration and GC when link count hits zero.
- **Locks** (`locks` bucket): map `pathHash+ownerID` to advisory lock state.
- **Xattrs** optionally stored inline (if small) or referenced via `xattrs/<inodeID>/<key>`.
- **Shard references** remain in the inode struct; GC is triggered when link count hits zero
  (the Bolt transaction writes a tombstone into `gc_queue` for the sweeper).

### BoltDB Schema

```
root bucket
├── meta (global counters: nextInodeID, nextTxnID, formatVersion)
├── inodes (inodeID → protobuf/CBOR payload)
├── dir:<inodeID> (childName → childInodeID)
├── parents:<inodeID> (parentInodeID → linkName set)
├── locks (pathHash+owner → lock record)
├── xattrs:<inodeID> (key → value or reference)
├── gc_queue (shardID → metadata snapshot for sweeper)
```

**Transactions:** every metadata mutation runs inside a Bolt write transaction guaranteeing
`inodes`/`dir`/`parents` consistency. Rename becomes: delete entry from old parent bucket,
insert into new parent bucket, update link counts, commit. For high write concurrency we
provide a short-lived RW transaction per operation (Bolt single-writer fits our typical
workload). Badger equivalent uses managed transactions.

**Encoding:** We will use protobuf (or msgpack) for `Inode` to keep backward compatibility.

## Permissions and Users

- Each request carries a `vfs.User` obtained from the calling frontend (CLI inherits real UID,
  HTTP/NFS supply it in headers or RPC auth). The helper `vfs.NewContextWithUser` embeds
  it into contexts consumed by the frontends/backends.
- Permission checks happen in `localfs` before metadata mutation: owner/group/other bits,
  sticky bit (on directories), setuid/setgid semantics (only recorded, not executed).
- `vfs.FS.Access` now enforces owner/group/other semantics on top of backend metadata so
  frontends observe consistent permission errors even when backends shortcut enforcement. The
  same helper is applied before `Rename` (write+exec on parent directories) and `SetAttr`
  (owner-or-root) so mutating operations no longer rely on backend-specific checks.
- Metadata lookups are cached in-memory (`vfs.Options.MetadataCacheSize/TTL`) so repeated
  permission checks for the same paths do not thrash backends; mutating operations eagerly
  invalidate affected paths so cached entries never go stale.
- Superuser (uid=0) bypasses checks except sticky-bit unlink restrictions.
- Supplementary groups enable shared directories; `vfs.User` holds `[]uint32`.
- `Access(path, mode, user)` is exposed for CLI `test -w` style queries and NFS ACCESS RPC.

## Special Files and Locks

- Special files share the same shard mechanism but with empty data (fifo/socket) or device
  numbers (char/block). `Inode.Special` stores `{Kind, Major, Minor}`.
- FIFOs and sockets rely on higher layers to implement actual IPC; Phase 2 will provide stubs
  returning `ErrNotSupported` on open until we map them to local pipes or network endpoints.
- Advisory locks: per-inode map keyed by `(owner, pid, range)`. :  
  - `LockOptions` includes byte-range (start,length), exclusive/shared, blocking, owner ID.  
  - Implementation uses `locks` bucket + in-memory table for fast checks.  
  - Protocols (NFS, FUSE) translate their lock requests to this API.

## Store Backend Choice

- BoltDB (go.etcd.io/bbolt) is the default due to its simplicity and crash consistency.
  Config options: `Path`, `BucketPrefix`, `NoSync` (for testing), `FreelistType`.  
- Badger V4 remains an option; we will wrap both behind `meta.Store` capabilities (Phase 2).
  The Bolt implementation ships first to de-risk schema; Badger impl can reuse the same
  interface with different transaction semantics.

## Protocol Implications

While Phase 1 focuses on design, we already fix the targets for Phase 4:

- CLI: new subcommands (`chmod`, `chown`, `chgrp`, `rename`, `mkfifo`, `stat`, `lock`), plus
  arguments to `put/cp` for owner/mode.
- HTTP API: PATCH/POST endpoints for attribute changes, rename API, xattr CRUD, Access checks.
- S3 gateway: map POSIX metadata to `x-amz-meta-*` and ACL; optionally expose `x-amz-posix-*`
  headers.
- NFS: stop simulating rename (call new API), surface ACCESS/LOCK RPC using the new methods.
- FUSE: implement the full set of fuseops (Setattr, Chmod, Chown, Utimens, Link, Rename2, Mknod).

## Migration & Compatibility

- Existing deployments using `meta.FileStore` need a migration tool:
  1. Freeze writes, dump current mem/file store as JSON.  
  2. Tool replays into Bolt store, deriving inode IDs and link graphs, preserving shard refs.  
  3. Switch configuration to new store path, run integrity check (`xgfs meta verify`).  
  4. Optional rollback by retaining the old metadata file.
- Format version will be bumped; `localfs.Config` gains `MetaEngine` (memory/file/bolt/badger).

## Next Steps (Phase 2+)

1. Implement Bolt store (`meta/boltdb_store.go`) with full transaction semantics and tests.
2. Extend `localfs` to honour the new interface (rename/chmod/chown/locks).
3. Swap protocol adapters to call the new APIs and add regression coverage.
4. Update docs/CLI/HTTP/S3/NFS/FUSE to reflect the POSIX feature set.

Phase 1 deliverables (this doc + interface skeleton + Bolt store scaffolding) unblock those
implementation steps without breaking the existing build.
