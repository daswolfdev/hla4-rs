# hla4-rs

Rust implementation of an IEEE 1516-2025 (HLA 4) Run-Time Infrastructure.

## Status

Early MVP. Federation lifecycle, publish/subscribe, time management,
ownership transfer, DDM region routing, synchronization points, and
save/restore are covered end-to-end by the integration test suite.
Not yet production-hardened and not yet IEEE-conformance-certified.

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

## API style

Async-only. `FederateAmbassador` uses native async-fn-in-trait
(stable Rust 1.75+) with explicit `Send` bounds — no `async_trait`
macro on the callback hot path. `RtiAmbassador` methods are async
and return per-service error enums.

## Performance

A common complaint against open-source HLA RTI implementations is
that they trail proprietary offerings on raw throughput. The
initial scaffolding bakes in a few design choices aimed at closing
that gap:

- **Encode-once fan-out.** When the RTI delivers an attribute
  reflection or interaction to N subscribers, the `CallbackRequest`
  protobuf is encoded exactly once and shared across all recipients
  as a `bytes::Bytes` — cloning is a refcount bump, not a copy.
- **Zero-copy frame codec.** Inbound frames are read into a single
  `BytesMut`, the header is decoded in place, and the payload is
  handed to routing via `split_off(...).freeze()` — no post-read
  copies. WebSocket transport slices tungstenite's `Bytes` directly.
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
performance comparison against pRTI/MAK RTI is future work.

## Third-party material

`crates/hla-fedpro-proto/vendor/fedproclient/` contains the `.proto` files from
[Pitch-Technologies/FedProClient](https://github.com/Pitch-Technologies/FedProClient)
(Apache 2.0). The proto files themselves additionally carry an IEEE royalty-free
copy/distribute/derivative-works grant in their headers.

## Build

```bash
cargo check --workspace
cargo test  --workspace
```

Requires `protoc` on `PATH`.
