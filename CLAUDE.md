# mkq

BullMQ-compatible Go-native job queue library. Drop-in alternative to asynq for mk-go, with wire-compatible Redis layout so bull-board / Misskey admin UI / other-language BullMQ workers can share queues.

## Authoritative design

Primary design document lives in the mk-go repo:
https://github.com/Elythia-Network/elythia/blob/develop/docs/design/mkq-design.md

Design-level changes go through a mk-go PR. Implementation-level notes (godoc, internal design decisions) belong in this repo under `docs/` or as package doc comments.

## Reference implementations

- **BullMQ** — authoritative for wire format. https://github.com/taskforcesh/bullmq
  - Redis key layout, Job JSON shape, and Lua script semantics must stay compatible with BullMQ v6.
  - Cross-check BullMQ source before writing any code that reads or writes Redis.
- **asynq** — current mk-go driver, for semantic reference only. https://github.com/hibiken/asynq

## Workflow

All work is tracked via GitHub Issue → branch → Pull Request. No direct commits to `main`.

1. Open or pick up an issue describing the scope.
2. Branch naming: `feature/<summary>` or `fix/<summary>`.
3. Work in small, reviewable commits. Confirm with the user before running `git commit`.
4. Open a PR referencing the issue. Follow the PR format defined in the user-global guideline (Summary / Key Changes / Testing / Related Issue).

## Go conventions

- Go version: matches mk-go (see mk-go `go.mod`). Generics required.
- Apply `gofmt` and `goimports` before every commit.
- Godoc comments: English.
- Inline comments:
  - English for mechanical clarifications tied to Go idioms.
  - Japanese for design rationale, BullMQ references, and non-obvious "why" notes.
- No emojis anywhere in code, docs, or commit messages.

## Redis / BullMQ compatibility rules

- Key naming, separator (`:`), and ID generation (INCR counter) follow BullMQ.
- Job HASH field names (`data`, `opts`, `progress`, `returnvalue`, `stacktrace`, etc.) follow BullMQ.
- State transitions and Lua script responsibilities mirror BullMQ v6 (see design doc Section 5).
- Divergence from BullMQ is allowed only at the API surface (Go generics / goroutines / context), never at the Redis wire format.

## Performance stance

Compatibility defines the wire format; it does not cap client implementation.

- Encouraged on the client side: goroutine-based concurrency, go-redis pipelining, allocation-lean JSON handling, `time.AfterFunc` for delayed-job wakeup.
- Non-negotiable: Redis payload shape, Lua atomic semantics, and cross-language queue sharing.
- Optimizations that assume mkq is the sole writer of a queue must opt-out when foreign workers are detected.
- Extended Lua scripts are additive, never replacements for BullMQ-equivalent scripts.
