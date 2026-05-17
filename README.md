# hla4-rs

Designed to implement IEEE 1516-2025 / HLA 4. Conformance test suite and
certification evidence in progress. Not IEEE-certified.

## Status

Early MVP. Federation lifecycle, publish/subscribe, time management,
ownership transfer, DDM region routing, synchronization points, and
save/restore are covered end-to-end by the integration test suite (135
tests across 27 binaries, ubuntu + macos in CI). Not yet
production-hardened.

## Workspace

| Crate              | Purpose                                                    |
|--------------------|------------------------------------------------------------|
| `hla-core`         | Handles, logical time, error types. No I/O.                |
| `hla-encoding`     | HLA standard MIM data types (HLAinteger32BE, etc.)         |
| `hla-omt`          | FOM / OMT XML parsing and modular FOM merge.               |
| `hla-fedpro-proto` | Generated protobuf types from the HLA 4 Federate Protocol. |
| `hla-wire`         | Session, framing, transport (TCP / TLS / WebSocket).       |
| `hla-rti`          | RTI server: federation registry, matching, routing.        |
| `hla-federate`     | Federate-side library (RTIambassador + callbacks).         |
| `hla-cli`          | `rtiexec` server binary.                                   |

`hla-rti`'s service dispatch is split along the IEEE 1516.1 service
groups: `dispatch/{federation_mgmt, declaration, object_mgmt, data_flow,
ownership, time_mgmt, sync_points, save, restore, advisories, ddm_*,
directed, introspection, instance_lookup, handle_lookup, helpers}.rs`.
Each submodule is 80–600 lines; the top-level `dispatch_call` match
dispatcher lives in `dispatch/mod.rs`.

## API style

Async-only. `FederateAmbassador` uses native async-fn-in-trait
(stable Rust 1.75+) with explicit `Send` bounds — no `async_trait`
macro on the callback hot path. `RtiAmbassador` methods are async
and return per-service error enums (all `#[non_exhaustive]`).

IEEE 1516.1 exception names are carried as a single `HlaException`
enum rather than free-floating string literals — every variant maps
to one canonical wire string, enforced by a unit test, so a typo at a
dispatch handler is a compile error.

## Cooperative shutdown

`RtiNode::shutdown()` cancels a `CancellationToken` watched by every
accept loop, the suspended-session janitor, and per-session reader
loops. `rtiexec` wires SIGINT and (on Unix) SIGTERM to that token
so `docker stop` / `kubectl delete` / `systemctl stop` drain
cleanly.

## Performance

A common complaint against open-source HLA RTI implementations is
that they trail proprietary offerings on raw throughput. The
initial scaffolding bakes in a few design choices aimed at closing
that gap:

- **Encode-once fan-out.** When the RTI delivers an attribute
  reflection or interaction to N subscribers, the `CallbackRequest`
  protobuf is encoded exactly once and shared across all recipients
  as a `bytes::Bytes` — cloning is a refcount bump, not a copy.
- **Cancel-safe framing.** `tokio_util::codec::Framed` with a
  custom `FedProCodec` keeps the inbound `BytesMut` alive across
  drops of the polling future. The server's main `select!` can
  share its read with a liveness tick without ever losing bytes to
  a partial read.
- **Reverse subscription index.** A `class_subscribers` map is
  maintained alongside the per-`(class, attribute)` matrix so
  "which federates care about this class?" lookups run in
  O(subscribers) instead of scanning the entire subscription table.
- **No `Box<dyn Future>` on the callback path.** Federate callback
  dispatch uses native AFIT, avoiding the per-call heap allocation
  that `async_trait` would impose.
- **Lean tokio.** Only the runtime features actually used
  (`rt-multi-thread`, `macros`, `io-util`, `net`, `sync`, `time`,
  `signal`) are pulled in — no `tokio = ["full"]`.

These are starting-point choices, not benchmark claims. A proper
performance comparison against pRTI / MAK RTI is future work.

## Build / dev workflow

```bash
cargo check --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all --check
```

CI runs the above plus `cargo doc` (with `RUSTDOCFLAGS=-D warnings`)
and `cargo-deny check` on every push. Requires `protoc` on `PATH`
(GitHub Actions uses `arduino/setup-protoc@v3`).

Workspace lint baseline (resolver 3, edition 2024, MSRV 1.95) lives
in `[workspace.lints]` in the root `Cargo.toml`; the rationale and
the broader May-2026 Rust meta this project tracks against are
documented in [`doc/BESTPRACTICES.md`](doc/BESTPRACTICES.md).

## Third-party material

`crates/hla-fedpro-proto/vendor/fedproclient/` contains the `.proto` files from
[Pitch-Technologies/FedProClient](https://github.com/Pitch-Technologies/FedProClient)
(Apache 2.0). The proto files themselves additionally carry an IEEE royalty-free
copy/distribute/derivative-works grant in their headers.
