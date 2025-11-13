# Repository Guidelines

## Project Structure & Module Organization

The module root (`go.mod`) defines `github.com/jacktea/xgfs`; treat it as the single source of truth. Place runnable binaries inside `cmd/<service>` and shareable libraries under `internal/` or `pkg/`. Keep `_test.go` files beside the code they cover, fixtures under `testdata/`, and any static assets under `assets/`. Each new package should expose only the minimal API needed by other modules.

## Build, Test, and Development Commands

- `go fmt ./...` – auto-format before sending a review.
- `go build ./cmd/xgfs` – compile the primary CLI, catching type errors early.
- `go test ./...` – run the full unit suite; add `-run Name` for targeted checks.
- `golangci-lint run` – run the aggregated linters if you have the tool installed locally.
- Wrap every backend in `pkg/vfs.FS` before handing it to CLI/HTTP/S3/NFS. The wrapper now
  enforces POSIX permissions (Access/Rename/SetAttr) and is the supported way to obtain
  `vfs.PosixFs`. CLI flag `--meta-cache-size` controls the vfs metadata cache (0 disables).
- CLI configuration can also be supplied via `--config <file>` (TOML or YAML). By default the
  CLI searches for `xgfs.toml|yaml` in the current directory and `~/.config/xgfs/`.

## Coding Style & Naming Conventions

Favor idiomatic Go: tabs for indentation, 100-character soft limit, and lower-case package names. Exported identifiers use PascalCase and include a short GoDoc comment; unexported helpers remain camelCase. Prefer pure functions and keep files under ~300 lines. Run `gofmt` and (optionally) `gofumpt` before committing; never hand-edit generated code.

## Testing Guidelines

Write table-driven tests covering edge cases and panic boundaries. Use `go test ./... -cover` and keep package coverage above 80%; include regression tests whenever you fix a bug. Integration or slow tests belong under `test/integration` and should be guarded with `//go:build integration`. Seed random sources so reruns stay deterministic.

## Commit & Pull Request Guidelines

Follow Conventional Commits (`feat:`, `fix:`, `chore:`) because the repository currently lacks an established history. Keep messages in the imperative mood and reference related issues (e.g., `fix: handle empty manifest (#42)`). PRs must describe intent, testing performed, and user-facing impact; attach screenshots for UI changes and include a checklist of updated docs/configs. Ensure CI scripts or reviewers can reproduce your steps with the commands above.
