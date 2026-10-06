package mkq

// Version is the mkq library version, written into the BullMQ meta
// HASH at Queue.Define time so admin tools (bull-board, custom
// dashboards) can identify which client populated the queue.
//
// The literal lives here rather than being read from
// runtime/debug.ReadBuildInfo so it stays meaningful in unit tests
// and in users who vendor mkq via tooling that doesn't preserve
// build info. Bump it on every release.
const Version = "1.5.0"

// versionTag is the value written to meta.version. BullMQ TS writes
// `bullmq:<semver>`; mkq mirrors the convention with a `mkq:` prefix
// so a foreign reader can identify the client.
func versionTag() string { return "mkq:" + Version }
