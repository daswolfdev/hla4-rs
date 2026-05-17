# hla4-rs

Rust implementation of an IEEE 1516-2025 (HLA 4) Run-Time Infrastructure.

## Status

Scaffolded skeleton. Not yet functional.

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

Async-only. Federates implement `FederateAmbassador` as an `async_trait`;
`RtiAmbassador` methods are async and return per-service error enums.

## Third-party material

`crates/hla-fedpro-proto/vendor/fedproclient/` contains the `.proto` files from
[Pitch-Technologies/FedProClient](https://github.com/Pitch-Technologies/FedProClient)
(Apache 2.0). The proto files themselves additionally carry an IEEE royalty-free
copy/distribute/derivative-works grant in their headers.

## Build

```bash
cargo check --workspace
```

Requires `protoc` on `PATH`.
