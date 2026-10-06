# Upstream provenance

Lua scripts in this directory are vendored verbatim from BullMQ.

- Upstream: https://github.com/taskforcesh/bullmq
- Source path: `src/commands/`
- Pinned commit: see `.gitmodules` and the checked-out SHA of
  `third_party/bullmq` (recorded redundantly here:
  `578ead3f575c8a3999e1a751b0cee7e34f6d936b`, 2026-09-18)
- License: MIT (see `THIRD_PARTY_NOTICES.md` at the repo root)

## Why the pin is a commit, not a tag

BullMQ のリリースタグは、バージョンを上げる `chore(release)` コミットの
**手前**に打たれる。つまり `v6.3.8` タグの `package.json` は `6.3.7` のままで、
`6.3.8` と書かれているのは次のコミットになる。

`lua sync verify` は `third_party/bullmq/package.json` の `version` と
`tests/interop/node/package.json` の bullmq ピンを突き合わせるので、
タグに合わせると必ず 1 バージョンずれて落ちる。そのため submodule は
`chore(release)` コミット側に置く。`chore(release)` が触るのは changelog /
`package.json` / `src/version.ts` だけで `src/commands/` は動かないため、
Lua の中身はタグと同一になる。

## Why both submodule and vendored copies?

Go modules don't pull submodule contents on `go get`, so the Lua
files must live inside the module tree for `go:embed` to work for
downstream consumers. The submodule at `third_party/bullmq` is
test/dev/CI tooling only — never required for `go get
github.com/elythia-network/mkq` to succeed.

## Files vendored

Entry points:

- `addStandardJob-9.lua`
- `addDelayedJob-6.lua`
- `addPrioritizedJob-9.lua`
- `moveToActive-11.lua`
- `moveToFinished-14.lua`
- `extendLock-2.lua`
- `releaseLock-1.lua`
- `retryJob-11.lua`
- `moveToDelayed-11.lua`
- `moveStalledJobsToWait-9.lua`
- `addJobScheduler-11.lua`
- `updateJobScheduler-12.lua`
- `updateProgress-3.lua`
- `updateData-1.lua`
- `addLog-2.lua`
- `getCounts-1.lua`
- `getRanges-1.lua`
- `removeJob-2.lua`
- `drain-5.lua`
- `promote-9.lua`
- `reprocessJob-7.lua`
- `getMetrics-2.lua`
- `pause-7.lua`

`includes/` — every script transitively reachable from the entry points
above via `--- @include "..."` directives.

The `-N` suffix on entry-point scripts is the BullMQ convention for the
expected `KEYS` count and is preserved verbatim.

## Re-vendoring

```
git submodule update --init --recursive
cd third_party/bullmq
git fetch
git checkout <new-sha>
cd -
script/sync-lua.sh
# Update the SHA + date noted at the top of this file, then commit
# everything (.gitmodules submodule pointer + sync'd lua + this file).
```

The `lua sync verify` GitHub Actions workflow runs `script/sync-lua.sh`
on every PR and fails if the result diverges from this directory,
catching both "submodule pin advanced but lua not refreshed" and
"vendored lua hand-edited without updating the submodule".

To add a NEW entry-point script (one that's not yet listed above), copy
it manually the first time along with any new transitive includes
referenced by its `--- @include "..."` directives. The sync script only
mirrors files that already exist locally.

Do not hand-edit vendored files. mkq's `@include` preprocessor (in the
parent `internal/lua` package) is responsible for resolving directives at
load time, mirroring BullMQ's `script-loader.ts`.
