# Rust Best Practices — May 2026

> Snapshot of current Rust idioms, ecosystem choices, and tooling defaults as
> of May 17, 2026, scoped to this workspace (edition 2024, MSRV 1.95, Tokio,
> rustls, prost, quick-xml). Recommendations are opinionated — where the
> community is split, the trade-off is named.

---

The Rust ecosystem in mid-2026 sits in an unusually settled place. Edition 2024 has been stable for over a year, the MSRV-aware resolver lets libraries support older toolchains without contortions, async fn in traits is the default, and the standard library has absorbed most of what used to live in `lazy_static`/`once_cell`. What follows is opinionated guidance for an experienced engineer working on a Tokio-based workspace targeting Rust 1.95, edition 2024.

## 1. Language Edition and Toolchain

### Edition 2024 is the default; commit to it

Every new crate in this workspace should declare `edition = "2024"` and `resolver = "3"` at the workspace root. Edition 2024 was stabilized in Rust 1.85 (Feb 2025) and is now a year+ in production. The resolver-3 behavior is the default for edition 2024 packages, and the MSRV-aware resolver (stable in 1.84) means Cargo will prefer dependency versions compatible with your declared `rust-version`.

```toml
# Workspace root Cargo.toml
[workspace]
resolver = "3"
members = ["crates/*"]

[workspace.package]
edition = "2024"
rust-version = "1.95"
license = "MIT OR Apache-2.0"
repository = "https://github.com/example/hla4-rs"
```

### Notable stabilizations to use (1.85 → 1.95)

The features below have all shipped on stable and are appropriate to use *unconditionally* on MSRV 1.95:

- **`let` chains** (1.88, edition 2024 only). Replace nested `if let Some(x) = ... { if cond { ... } }` ladders with `if let Some(x) = foo() && cond && let Ok(y) = bar() { ... }`. This is the single biggest readability win of the edition.
- **Async closures** (1.85). `async || { ... }` is finally a first-class construct returning a future. Prefer over `move || async move { ... }` workarounds.
- **Native `async fn` in traits / RPITIT** (1.75; now mature). Use directly — see the async section for caveats around `Send` bounds.
- **Precise capturing `use<>` bound on RPIT** (1.82, fully stable in trait position 1.86). In edition 2024, RPIT *captures all in-scope lifetimes by default*; if you need to narrow that, use `fn foo<'a, 'b, T>(...) -> impl Trait + use<'a, T>`. Know that this changed: porting code from edition 2021 often surfaces unexpected lifetime captures.
- **Trait upcasting** (1.86). `&dyn Sub` coerces to `&dyn Super` directly — drop the manual `as_super()` boilerplate.
- **Naked functions** (1.88). For low-level FFI / interrupt handlers only.
- **`#[target_feature]` on safe fns** (1.86). Allows SIMD intrinsics callers to be safe when feature detection has been done.
- **`if let` guards on match arms** (1.95). `match x { Foo(v) if let Bar(b) = v.get() => ... }` — useful for protocol/codec match arms.
- **`bool: TryFrom<u8 | i32 | ...>`** (1.95). No more `n != 0` boilerplate when decoding wire bytes — but it errors on values >1, which is what you want for protocol parsers.
- **`cfg_select!`** (1.95). Cleaner than nested `cfg` attributes for selecting between expressions.
- **`core::hint::cold_path`** (1.95). Mark a branch as cold without enclosing it in a `#[cold]` function.
- **Atomic `update`/`try_update`** (1.95). Higher-level CAS loops without writing your own `compare_exchange_weak` spin.
- **`Vec::into_raw_parts`, `String::into_raw_parts`** (1.93). FFI-friendly disassembly without `mem::forget` gymnastics.
- **`Duration::from_mins`, `from_hours`** (1.91). Use them in tests instead of `Duration::from_secs(60 * 5)`.
- **Strict / carrying / borrowing integer arithmetic** (1.91). Use `strict_add` in protocol code where wrap is a bug, not a feature.
- **LLD as default linker on x86_64-linux** (1.90). Local builds get faster automatically; no action required.

### Features still on nightly (do not rely on)

- **`gen` blocks / coroutines.** The `gen` keyword is reserved in edition 2024, but `gen { ... yield ... }` bodies are still nightly-only as of 1.95. Use `async_stream::stream!` or hand-rolled `Stream` impls for now.
- **Never type `!`.** Rust 1.92 made the fallback lints deny-by-default in preparation for stabilization; the type itself is still not nameable in stable code. Use `std::convert::Infallible` for error positions and let the compiler infer `!` in diverging contexts.
- **Specialization, full GATs HRTBs in some positions, `impl Trait` in associated types of `dyn` traits.** Avoid.

### MSRV policy

For a domain library like a HLA/IEEE-1516 stack, the community consensus is "N-2 stable" — track two releases behind the latest stable, raising MSRV as a minor bump and noting it in CHANGELOG. Hyper's "support a compiler released within the last 6 months" policy is the other common stance. Set `rust-version` in `[workspace.package]`, *and* CI on that exact toolchain via `rust-toolchain.toml`:

```toml
# rust-toolchain.toml
[toolchain]
channel = "1.95.0"
components = ["rustfmt", "clippy"]
profile = "minimal"
```

A separate CI job should run against `stable` and `beta` to catch upcoming lint changes early.

## 2. Async Ecosystem

### Use native `async fn` in traits; reach for `async-trait` only when forced

Since 1.75, `async fn` is allowed in traits and desugars to RPITIT. For new code, write:

```rust
trait HlaFederate {
    async fn join(&mut self, name: &str) -> Result<FederateHandle, JoinError>;
    async fn resign(self) -> Result<(), ResignError>;
}
```

The `async-trait` macro is **not** deprecated, but it is no longer the default. You only need it when:

1. You require `dyn Trait` (object-safe async trait). Native AFIT traits are not yet `dyn`-compatible. The `async-trait` macro produces `Pin<Box<dyn Future + Send + '_>>`, which is `dyn`-safe.
2. You need to *guarantee* that all impls produce `Send` futures regardless of generic substitutions — `async-trait` bakes `Send` into the signature; native AFIT does not.

For (2), the **modern answer is `trait_variant`** (an official `rust-lang` crate). It generates two parallel traits: one without `Send` and one (`: Send`) for work-stealing executors:

```rust
#[trait_variant::make(HlaFederate: Send)]
pub trait LocalHlaFederate {
    async fn join(&mut self, name: &str) -> Result<FederateHandle, JoinError>;
}
```

Downstream code in Tokio (work-stealing) bounds on `HlaFederate`; embedded/single-thread code bounds on `LocalHlaFederate`. This is the recommended pattern as of 2026. Reach for `async-trait` only when you specifically need a heap-allocated boxed future (dynamic dispatch over runtime-selected impls).

### Tokio idioms

- **Spawn tasks at the boundaries**, not deep inside library code. A library exposes futures; the application decides where to spawn. This makes cancellation, runtime selection, and testing all simpler.
- **`tokio::main` only in binaries.** Libraries should be runtime-agnostic where possible; use `tokio::test` in tests and let the binary choose `#[tokio::main(flavor = "multi_thread")]`.
- **Prefer `tokio::sync` channels over `std::sync::mpsc`** in async code — they integrate with the runtime; `std::sync::mpsc::recv()` blocks the worker.
- **`spawn_blocking` for CPU work and *all* sync-only APIs.** Alice Ryhl's "Async: What is blocking?" remains the canonical reference: anything taking >10–100 µs without an `await` is "blocking" in async-Rust terms, full stop. Wrap `quick_xml` deserialization of large documents, `prost` decoding of multi-MB messages, and any synchronous file I/O in `spawn_blocking`.
- **Use `tracing::Instrument` (the `.instrument(span)` combinator) on spawned tasks** so spans propagate.

### `tokio::select!` and cancellation safety

Every branch of `select!` is **cancelled** when another wins. A future that has consumed bytes from a socket but not yet produced a message is a data-loss bug waiting to happen. The hard rule:

> Inside `loop { select! { ... } }`, every awaited future must be **cancel-safe** — i.e., dropping it mid-poll leaves no observable state corruption.

Cancel-safe APIs in Tokio are documented as such. Notably:

- `AsyncReadExt::read` / `read_exact`: **NOT cancel-safe** if you have a partial read (bytes consumed, not yet returned). Use `read_buf` against a long-lived `BytesMut` you own, or wrap the reader in a `FramedRead` and select on `frame.next()` which **is** cancel-safe.
- `tokio::time::sleep`: cancel-safe.
- `mpsc::Receiver::recv`: cancel-safe.
- `JoinSet::join_next`: cancel-safe.
- `tokio_tungstenite` `WebSocketStream::next`: cancel-safe (it's a `Stream`).
- `WebSocketStream::send`: **NOT cancel-safe** — partial frame writes are possible. Sequence sends with `.await` on its own line.

When in doubt, encode the await as a *value* you can resume: `let fut = pin!(slow_op()); loop { select! { _ = &mut fut => ..., _ = other => ... } }`. Re-polling `&mut fut` is fine; dropping `fut` is not.

### Structured concurrency: prefer `JoinSet` over loose `tokio::spawn`

Bare `tokio::spawn` produces detached tasks that outlive their logical parent — the well-known async "task leak". For a federate that owns N protocol handlers, use `JoinSet`:

```rust
let mut tasks = JoinSet::new();
for peer in peers {
    tasks.spawn(handle_peer(peer));
}
while let Some(result) = tasks.join_next().await {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = ?e, "peer task failed"),
        Err(join_err) if join_err.is_panic() => std::panic::resume_unwind(join_err.into_panic()),
        Err(_) => {} // cancelled
    }
}
// tasks drops here -> all children aborted. Structured.
```

For graceful shutdown across many tasks, combine `JoinSet` with `tokio_util::sync::CancellationToken`. The token is the idiomatic way to propagate "please stop" without resorting to channels-as-flags.

### io_uring: still not for general production

`tokio-uring` has seen sporadic releases and lags new kernel io_uring features. `glommio` and `monoio` (thread-per-core, io_uring) win on micro-benchmarks for proxy/storage workloads but split the ecosystem — you cannot trivially run an axum router under monoio. For an HLA federate on commodity Linux, **stick with default Tokio (epoll)**. Revisit io_uring only when you have measured that the syscall overhead is your bottleneck, which is rare for protocol/state-synchronization workloads.

## 3. Error Handling

### The 2026 split: thiserror 2.x for libraries, anyhow for binaries, eyre for user-facing diagnostics

This split is unchanged from 2024 in shape, but `thiserror` 2.0 (released late 2024) is now the default. It tightened up `#[from]` semantics, made `#[source]` more flexible, and added improved support for transparent variants. Use it everywhere library code defines an error type. **Every library crate in the workspace exports a `pub enum Error` and a `pub type Result<T> = std::result::Result<T, Error>;`.**

```rust
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum CodecError {
    #[error("unexpected end of input at offset {offset}")]
    Truncated { offset: usize },

    #[error("invalid tag {tag:#x}")]
    InvalidTag { tag: u32 },

    #[error("protobuf decode failed")]
    Protobuf(#[from] prost::DecodeError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
```

Notes:
- Apply `#[non_exhaustive]` to every public error enum so adding variants is not a breaking change.
- Prefer `#[error(transparent)]` for variants that wrap a foreign error — it forwards `Display` and `source()` without adding noise.
- Do **not** use `anyhow::Error` in a library's public API. Doing so erases type information and forces every downstream caller into a `Box<dyn Error>`-shaped world.

For binaries, `anyhow` is still the right default:

```rust
use anyhow::{Context, Result};

fn main() -> Result<()> {
    let cfg = load_config("federate.toml")
        .with_context(|| "failed to load federate config")?;
    // ...
}
```

`.context(...)` / `.with_context(|| ...)` is the canonical way to add a layer of explanation. The closure form avoids the formatting cost on the happy path.

Use **`eyre`** (specifically `color-eyre`) when the error messages are user-facing — CLI tools that the operator reads. `eyre` is a fork of `anyhow` with pluggable report handlers; `color-eyre` adds colored, sectioned output with optional `SpanTrace` (tracing) integration.

Use **`miette`** when you need *Elm-style diagnostic output* — source spans, underlines, labels. This is appropriate for parsers, compilers, and DSL tooling. For raw network/protocol errors, `miette` is overkill.

### Backtraces

`std::backtrace::Backtrace` is stable. Both `anyhow::Error` and `thiserror`'s `#[backtrace]` attribute capture them when `RUST_BACKTRACE=1` is set. Library errors should carry a `Backtrace` only when the error variant represents a programming bug (e.g., an assertion failure); routine errors (`Truncated`, `InvalidTag`) should not — backtraces are expensive and noisy in logs.

### `snafu` is fine but no longer recommended for new code

`snafu` predates `thiserror`'s `#[from]` ergonomics. It's still used in some large codebases (Apache Arrow, InfluxDB) and is a perfectly good library, but new workspaces should default to `thiserror` for consistency with the rest of the ecosystem.

## 4. Project Structure and Workspace Hygiene

### Workspace inheritance is mandatory at this scale

Centralize *everything* that is not crate-specific. A representative layout:

```toml
# /Cargo.toml
[workspace]
resolver = "3"
members = ["crates/*"]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.95"
license = "MIT OR Apache-2.0"
repository = "https://github.com/example/hla4-rs"
authors = ["..."]

[workspace.dependencies]
# Async runtime + networking
tokio        = { version = "1.40", features = ["macros", "rt-multi-thread", "net", "io-util", "sync", "time", "signal"] }
tokio-util   = { version = "0.7",  features = ["codec", "rt"] }
tokio-tungstenite = { version = "0.24", default-features = false, features = ["rustls-tls-webpki-roots"] }
rustls       = { version = "0.23", default-features = false, features = ["ring"] }

# Codec / protocol
prost        = "0.13"
prost-types  = "0.13"
quick-xml    = { version = "0.36", features = ["serde", "serialize"] }
bytes        = "1"

# Serde
serde        = { version = "1", features = ["derive"] }
serde_json   = "1"

# Errors + diagnostics
thiserror    = "2"
anyhow       = "1"

# Logging
tracing            = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "registry", "fmt"] }

# CLI
clap = { version = "4", features = ["derive"] }

# Concurrency
parking_lot = "0.12"
dashmap     = "6"
arc-swap    = "1"

# Testing
proptest    = "1"
insta       = { version = "1", features = ["yaml"] }
rstest      = "0.23"
tokio-test  = "0.4"

# Internal crates — declare here once
hla4-core      = { path = "crates/hla4-core",      version = "0.1.0" }
hla4-codec     = { path = "crates/hla4-codec",     version = "0.1.0" }
hla4-transport = { path = "crates/hla4-transport", version = "0.1.0" }

[workspace.lints.rust]
unsafe_code = "warn"        # or "forbid" if no FFI
missing_docs = "warn"
rust_2024_compatibility = { level = "warn", priority = -1 }
unused_must_use = "deny"

[workspace.lints.clippy]
all      = { level = "warn", priority = -1 }
pedantic = { level = "warn", priority = -1 }
nursery  = { level = "warn", priority = -1 }
cargo    = { level = "warn", priority = -1 }
# Targeted exemptions — see lint section
module_name_repetitions = "allow"
must_use_candidate      = "allow"
missing_errors_doc      = "allow"   # thiserror enums document themselves
missing_panics_doc      = "allow"
```

```toml
# /crates/hla4-codec/Cargo.toml
[package]
name        = "hla4-codec"
version.workspace      = true
edition.workspace      = true
rust-version.workspace = true
license.workspace      = true
repository.workspace   = true

[dependencies]
tokio       = { workspace = true }
bytes       = { workspace = true }
prost       = { workspace = true }
quick-xml   = { workspace = true }
serde       = { workspace = true }
thiserror   = { workspace = true }
tracing     = { workspace = true }
hla4-core   = { workspace = true }

[dev-dependencies]
proptest    = { workspace = true }
insta       = { workspace = true }
rstest      = { workspace = true }
tokio-test  = { workspace = true }

[lints]
workspace = true
```

Important: each crate **must** explicitly opt into workspace lints with `[lints] workspace = true`. Cargo does *not* inherit them implicitly. There's a separate diagnostic for crates missing this, but it's worth verifying.

### Feature hygiene

- Every crate has `default = []` for libraries, or a sensible minimal default for binaries. Never make `tokio`'s full feature set transitively required.
- Group features semantically (`rustls`, `native-tls`, `metrics`) rather than per-dependency.
- Use **weak dependency features** (`dep?/feature` syntax, stable since 1.60) to avoid pulling in optional deps via feature unification.
- Document every public feature in the crate's `lib.rs` with `#![cfg_attr(docsrs, feature(doc_auto_cfg))]` so docs.rs renders feature badges.
- **Test the feature matrix in CI with `cargo hack --feature-powerset --depth 2 check`.** This is the single most effective way to catch feature-unification bugs.

### Profiles

```toml
[profile.release]
lto           = "thin"
codegen-units = 1
opt-level     = 3
strip         = "symbols"
panic         = "abort"     # for binaries; keep "unwind" for libraries

[profile.bench]
inherits = "release"
debug    = "line-tables-only"   # symbols for profilers; small size

[profile.dev]
opt-level = 0
debug     = "line-tables-only"  # faster than full debuginfo, still good backtraces

# Dev-but-fast: build deps with optimizations, your code without
[profile.dev.package."*"]
opt-level = 3
```

The "dev-but-fast" trick — `[profile.dev.package."*"] opt-level = 3` — applies to dependencies only. It dramatically speeds up runtime of debug binaries (especially anything that walks large data structures: serde, prost, regex) while keeping your crate's compile time fast.

## 5. Lints and Clippy

### Recommended baseline (in `[workspace.lints]`)

**`rust` lints**:
- `unsafe_code = "forbid"` if you have no FFI; `"warn"` if you have a single `unsafe`-using crate that documents every block. Mixing safety-critical and FFI-heavy code in the same crate is a smell.
- `missing_docs = "warn"` for any crate with a public API. Combined with `#![warn(missing_docs)]` per crate.
- `unused_must_use = "deny"`.
- `rust_2024_compatibility = { level = "warn", priority = -1 }` while transitional code remains.
- `unreachable_pub = "warn"` — surfaces `pub` items that should be `pub(crate)`.
- `let_underscore_drop = "warn"` — catches `let _ = mutex.lock()` (drops immediately).
- `unsafe_op_in_unsafe_fn` — already warn-by-default in edition 2024. Promote to `deny` if you take unsafe code seriously.

**`clippy` lint groups** — enable, then exempt:

```toml
[workspace.lints.clippy]
all      = { level = "warn", priority = -1 }
pedantic = { level = "warn", priority = -1 }
nursery  = { level = "warn", priority = -1 }   # accept some churn for value
cargo    = { level = "warn", priority = -1 }

# Pedantic noise that's almost always wrong to enforce:
module_name_repetitions   = "allow"
similar_names             = "allow"
too_many_lines            = "allow"
must_use_candidate        = "allow"
missing_errors_doc        = "allow"
missing_panics_doc        = "allow"
cast_precision_loss       = "allow"
cast_possible_truncation  = "allow"
cast_sign_loss            = "allow"

# Promote to deny — these are real bugs:
dbg_macro                = "deny"
todo                     = "warn"   # OK in development, fail CI on PRs
unimplemented            = "deny"
mem_forget               = "deny"
lossy_float_literal      = "deny"
string_to_string         = "deny"
```

`priority = -1` is critical: it sets a baseline that individual lint overrides can supersede.

### Enable `clippy::nursery` deliberately

The official guidance is "cherry-pick from nursery." In practice, on a large workspace this means a lot of triage. The pragmatic middle ground is to enable `nursery` workspace-wide as `warn` and exempt the noisy ones as they come up. The high-value nursery lints include `option_if_let_else`, `or_fun_call`, `redundant_pub_crate`, `useless_let_if_seq`, and `derive_partial_eq_without_eq`.

### Per-file overrides

For generated code (`prost-build` output, `tonic-build`), use:

```rust
#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo, missing_docs)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/hla4.proto.rs"));
}
```

## 6. Tooling and CI

A 2026-vintage Rust workspace CI pipeline runs:

1. **`cargo fmt --check`** (or `taplo fmt --check` for `Cargo.toml`).
2. **`cargo clippy --workspace --all-targets --all-features -- -D warnings`**.
3. **`cargo nextest run --workspace --all-features`** — `cargo-nextest` is the default test runner in 2026. Reasons: parallel by default, per-test isolation, JUnit XML output, retries for flaky tests, test partitioning for sharded CI, much better output. RustRover added native nextest support in 2026.1. `cargo test` is now used mainly for doctests (`cargo test --doc`) since nextest doesn't run them.
4. **`cargo hack --feature-powerset --depth 2 check`** — feature-combination smoke test.
5. **`cargo deny check`** — licenses, advisories, banned crates, sources. `cargo-deny` subsumes the role of `cargo-audit`; you no longer need both. Configure with `deny.toml`:
   ```toml
   [advisories]
   version = 2
   db-urls = ["https://github.com/rustsec/advisory-db"]
   yanked = "deny"

   [licenses]
   version = 2
   allow = ["MIT", "Apache-2.0", "Apache-2.0 WITH LLVM-exception", "BSD-3-Clause", "ISC", "Unicode-DFS-2016", "Unicode-3.0"]

   [bans]
   multiple-versions = "warn"
   wildcards = "deny"
   deny = [
     { name = "openssl" },     # we use rustls
     { name = "openssl-sys" },
   ]
   ```
6. **`cargo machete`** — unused dependencies. Replaces `cargo-udeps` for most cases; works on stable.
7. **`cargo semver-checks check-release`** — for library crates only. `cargo-semver-checks` shipped 245+ lints in 2025 and catches the vast majority of accidental SemVer breaks. Run on every PR that touches a `pub` item.
8. **`typos`** — fast typo checker for source and docs.
9. **`cargo miri test -p hla4-codec`** — if you have any `unsafe`, miri is non-negotiable. Run on a subset (the unsafe-using crates) since it's slow.
10. **`cargo mutants`** — mutation testing. Run weekly, not per-PR; expensive. Use `--in-place` and focus on critical modules (codec, dispatcher).

Other tools worth setting up:

- **`sccache`** for local and CI caching of compiled artifacts. The Mozilla version is the default; use the GitHub Actions or S3 backend.
- **`cargo binstall`** for installing prebuilt tool binaries instead of `cargo install` (which compiles).
- **`cargo sweep -t 30`** to garbage-collect old build artifacts that pile up in `target/`.
- **`taplo`** for `Cargo.toml` formatting and schema validation. Drop a `taplo.toml`.
- **`cargo-vet`** if your organization has a security review process. Google's audits are publicly importable.

## 7. Performance and Unsafe

### When to reach for `unsafe`

Don't, until you have a profile-driven reason. The standard checklist before adding `unsafe`:

1. Have you tried the safe API and measured? `SmallVec`, `bytemuck::cast_slice`, `slice::split_at`, `MaybeUninit` arrays via safe wrappers all cover the 90% case.
2. Is the invariant local enough to document and test? If unsafety leaks across module boundaries, restructure.
3. Does miri pass?

When you do write `unsafe`:

- Edition 2024 makes `unsafe_op_in_unsafe_fn` warn-by-default. Every unsafe op inside an unsafe fn must be in an explicit `unsafe { ... }` block. Embrace this.
- Each `unsafe { ... }` block carries a `// SAFETY:` comment immediately above it, explaining *why* the preconditions hold *at this call site*.
- `unsafe fn` carries a `# Safety` doc section listing preconditions the caller must guarantee.
- Run `cargo miri test` on the crate, ideally in CI on a nightly job.

### Benchmarking: criterion vs divan

Both are good. The current opinionated guidance:

- **`criterion`** if you have an existing suite, need its richer statistical analysis (regression detection, custom measurements), or rely on its HTML reports.
- **`divan`** for new benchmarks. It's significantly simpler, supports generics natively (`#[divan::bench(types = [u32, u64])]`), measures allocations, and is much faster to iterate on. CI integration via CodSpeed.

Use `std::hint::black_box` (stable since 1.66) on inputs and outputs to defeat dead-code elimination. Avoid the older `criterion::black_box` — the std version is the right one.

### PGO and LTO

For binaries that are perf-critical:

- **`lto = "thin"`, `codegen-units = 1`** is the sweet spot for most release builds. `fat` LTO buys a few percent more but doubles link time.
- **PGO** with `cargo-pgo` (or hand-rolled `-Cprofile-generate` / `-Cprofile-use`) reliably delivers 5–15% on real workloads for protocol/codec-heavy binaries. The workflow: build instrumented binary, run a representative workload, build optimized binary using the collected profile. Worth doing for production binaries; not worth doing for libraries.
- **BOLT** (LLVM post-link optimization) is the next step beyond PGO; meaningful gains on large binaries, complex to set up. Reserve for production deployments where the 1–3% extra matters.

### `#[inline]` guidance

- `#[inline]` on small generic functions that cross crate boundaries — without it, monomorphizations cannot inline across crates.
- `#[inline(always)]` only when you have measured. The compiler is usually right.
- `#[inline(never)]` on cold error paths to keep the hot-path icache footprint small.

## 8. API Design

### The defaults

- **`#[non_exhaustive]` on every public enum and most public structs.** Lets you add variants/fields without a SemVer break.
- **Sealed traits** for traits not meant to be implemented downstream. Add a private supertrait:
  ```rust
  mod private { pub trait Sealed {} }
  pub trait Codec: private::Sealed { /* ... */ }
  ```
  Library traits like `tokio::io::AsyncRead`-shaped extension traits should almost always be sealed.
- **Newtypes for domain values** — `FederateHandle(u32)`, `ObjectClassId(NonZeroU32)`, `LogicalTime(i64)`. Use the `derive_more` crate (`#[derive(From, Into, Display)]`) to avoid boilerplate. Newtypes prevent accidentally passing a `FederateHandle` where a `ObjectInstanceId` was expected — invaluable in a protocol stack.
- **`#[derive(Debug, Clone)]`** is the default minimum on data types; `Hash, Eq, PartialEq` when used as map keys; `Serialize, Deserialize` behind a feature.

### Builder pattern: `bon` is the modern answer

The `bon` crate, especially since v3.0, is the recommended way to generate builders. It uses typestate to enforce required-field setting at compile time, the generated typestate is now human-readable, and it supports named parameters on free functions and methods. Use it instead of hand-written builders, `derive_builder`, or `typed-builder`.

```rust
use bon::Builder;

#[derive(Builder)]
pub struct FederateConfig {
    pub name: String,
    pub federation: String,
    #[builder(default = Duration::from_secs(30))]
    pub timeout: Duration,
    #[builder(into)]
    pub fom_modules: Vec<PathBuf>,
}

// Use:
let cfg = FederateConfig::builder()
    .name("alpha")
    .federation("Battlespace")
    .fom_modules(["modules/RPR-FOM.xml"])
    .build();
```

### Typestate

For protocol state machines (`Disconnected → Joining → Joined → Resigned`), typestate is unbeatable:

```rust
pub struct Federate<S> { state: S, /* ... */ }
pub struct Disconnected;
pub struct Joined { handle: FederateHandle }

impl Federate<Disconnected> {
    pub async fn join(self, name: &str) -> Result<Federate<Joined>, JoinError> { /* ... */ }
}
impl Federate<Joined> {
    pub async fn resign(self) -> Result<Federate<Disconnected>, ResignError> { /* ... */ }
    pub fn send_interaction(&mut self, /* ... */) { /* ... */ }
}
```

Methods that don't make sense in a state literally don't exist on that state. The compiler enforces protocol correctness.

### `From`/`TryFrom`

- Implement `From<T>` only for *infallible* conversions where the meaning is unambiguous. Avoid `From<u32> for FederateHandle` if zero is invalid — use `TryFrom`.
- `TryFrom<&[u8]>` is the idiom for parsing wire formats: `let msg = ProtoMessage::try_from(&buf[..])?;`.
- Don't use `Into` in trait bounds (`fn foo<T: Into<String>>(x: T)`) on hot paths — it pessimizes the caller. Generic `impl AsRef<str>` is usually more flexible.

### `Cow`, `&str` vs `String`, `impl Trait`

- **Take `&str` in function signatures**; return `String` only when ownership transfer is essential.
- **`Cow<'a, str>`** when the function *might* need to allocate (e.g., XML entity decoding). Don't use it speculatively.
- **Return `impl Iterator`** for adapter functions. Return `Box<dyn Iterator>` only when erasure across runtime branches is needed.
- **`impl Trait` in argument position is equivalent to a generic parameter** — easy reading at the cost of no explicit turbofish. Fine for closures and futures, less ergonomic when callers want to spell the type.

## 9. Concurrency Primitives

### `parking_lot` vs `std::sync` — re-evaluate

The 2026 picture is: **`std::sync::Mutex` is good enough for the vast majority of cases.** It uses futexes on Linux, is small (no boxing), and has competitive performance for short critical sections. Use `parking_lot` when:

- You measure heavy contention and `parking_lot::Mutex`'s fairness reduces tail latency.
- You need `RwLock` with priority semantics or `RawMutex` for custom synchronization primitives.
- You need to lock the same `Mutex` in `const` context (parking_lot's `const fn new` works on older MSRVs; `std::sync::Mutex::new` is const since 1.63).
- You need a `MappedMutexGuard` / `MappedRwLockReadGuard` for projecting through a lock.

A reasonable workspace default is: **`std::sync` everywhere, with `parking_lot` reserved for the hot paths where you have a benchmark proving it matters.** The convenience win of `parking_lot::Mutex::lock()` not returning a `Result` is real, but the std `Mutex` has been steadily closing the gap.

### `dashmap` for concurrent maps

`DashMap` remains the idiomatic concurrent hashmap when you have read-write traffic from many threads — used in tracing, deno, lots of production stacks. Caveats:

- It uses sharded `RwLock`s internally. If your key distribution is skewed or you hold guards while doing more work, contention shifts to whichever shard is hot.
- For mostly-reads with rare updates, `arc_swap::ArcSwap<HashMap<K, V>>` (or the `im` persistent hashmap inside `ArcSwap`) is often *faster* because readers do no locking at all — they take a snapshot Arc and let the writer atomically swap in the new map.
- For append-only workloads (interner-style), consider `boxcar::Vec` or `papaya::HashMap` (a newer lockfree map gaining traction).

### `arc-swap`

Use `ArcSwap<T>` for hot-read, cold-write singletons: routing tables, FOM (object model) snapshots, feature flags. `arc_swap::cache::Cache` gives near-zero-overhead reads.

### `crossbeam`

The toolkit is mature and stable. Specific recommendations:

- **`crossbeam-channel`** — superior to `std::sync::mpsc` for sync code: bounded/unbounded, `select!`, fairness. For async code, use `tokio::sync::mpsc` instead.
- **`crossbeam-epoch`** — only if building your own lock-free data structures; the learning curve is steep.
- **`crossbeam-deque`** — work-stealing deques for custom schedulers.
- **`crossbeam-utils`** — `CachePadded` for false-sharing avoidance, `AtomicCell`, scoped threads (though `std::thread::scope` since 1.63 covers most cases).

### `rayon`

Default choice for data parallelism on CPU work. `par_iter()` and you're done. Inside an async runtime, run rayon work via `spawn_blocking` or use rayon's own thread pool deliberately — never call `par_iter()` directly inside an async fn on a tokio worker thread (it blocks).

## 10. Serialization and Protocol

### Choosing a wire format

| Format | When to use |
|---|---|
| **prost (protobuf)** | Cross-language RPC, schema evolution required, well-known consumers. Default for HLA-over-protobuf transports. |
| **bincode 2.x** | Internal Rust-to-Rust persistence, snapshotting. Bincode 2 finally split from serde and has its own derive; faster than 1.x. |
| **postcard** | Embedded, constrained environments, COBS framing, varint-encoded small messages. Same author as the rest of the embedded Rust stack. |
| **rkyv** | Zero-copy reads from mmap'd files, latency-sensitive in-process IPC. Big-hammer, infects your type model with `Archived<T>` doubles. |
| **borsh** | Determinism is critical (blockchains). Not relevant to HLA. |
| **serde_json / serde_yaml** | Config files, human-readable wire formats, web API boundaries. |
| **quick-xml + serde** | HLA FOM XML, FDD parsing. |

For an HLA stack with `prost` already in scope, use it for the protocol messages and reserve `bincode 2.x` for ephemeral on-disk state (e.g., a federate's recovery snapshot). Don't mix formats unless you have a reason.

### `prost` idioms

- Drive code generation from a `build.rs` using `prost-build`, not `tonic-build`, if you don't need gRPC.
- Wrap the generated `mod` to apply lint allows (see lint section).
- Don't expose `prost`-generated types in your public API — wrap them in newtypes. Otherwise every consumer takes a `prost` version dependency.
- For schema evolution, use `optional` fields and `#[non_exhaustive]`-style discipline at the protobuf level.
- `quick-protobuf` exists and is faster on some workloads, but you write more boilerplate (manual `Option` and `Cow` handling) and lose `tonic`/`prost-types` interop. Default to `prost`.

### `quick-xml` idioms for HLA FOMs

FOM/FDD files can be large (tens of MB for omnibus FOMs). Two modes:

1. **Serde derives, full materialization** — fine for one-shot loading at federate init.
   ```rust
   #[derive(Debug, Deserialize)]
   #[serde(rename = "objectModel")]
   pub struct ObjectModel {
       pub objects: Objects,
       pub interactions: Interactions,
       // ...
   }
   let model: ObjectModel = quick_xml::de::from_reader(reader)?;
   ```
   Wrap in `spawn_blocking` if called from async context — XML parsing is CPU work.

2. **Event-based `Reader`** for streaming over very large inputs, with `Event::Start`, `Event::End`, `Event::Text`. Use a `Vec<u8>` buffer that you `.clear()` between events. Bound buffered events via `Deserializer::event_buffer_size` (if using the serde path with `serialize` feature) to avoid pathological memory use.

Key serde-quick-xml mapping points: use `$text` for element text content, `$value` for "match any inner element," and `@attr` for attributes. Document your mapping — XML serde is notoriously full of foot-guns.

## 11. Logging and Observability

`tracing` is the universal default in 2026. The `log` crate is still alive as a passive logging frontend, but no new code should pull it in directly. Bridge crates handle interop:

- **`tracing-log`** — `log` events flow into `tracing` subscribers. Enable when consuming third-party crates that still emit via `log`.
- **`log-tracing`** is the reverse direction; rarely needed.

### Recommended subscriber setup

```rust
use tracing_subscriber::{prelude::*, EnvFilter, fmt};

pub fn init_tracing() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,hla4=debug"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false).compact())
        // OpenTelemetry layer optional, gated on feature
        .try_init()?;
    Ok(())
}
```

Use `registry()` as the base composer; it's what lets you add OTLP/file/JSON layers without restructuring.

### Span discipline

- **Instrument every public async fn** with `#[tracing::instrument(skip(self, large_arg), fields(handle = %self.handle))]`. The `skip` argument is critical — you don't want a `Debug` of a 10 MB buffer in every span.
- Use `tracing::info!(event_field = value, "human message")` — structured fields, not formatted strings.
- `tracing::Span::current().record("...")` lets you fill in fields whose values are only known mid-function.
- `.in_current_span()` / `.instrument(span)` on spawned tasks so spans propagate.

### OpenTelemetry integration

The `tracing-opentelemetry` crate bridges tracing spans to OTLP exporters. Compatibility between `opentelemetry`, `opentelemetry_sdk`, and `tracing-opentelemetry` versions is critical — they release in lockstep. As of mid-2026, the matching set is `opentelemetry` 0.28.x + `opentelemetry_sdk` 0.28.x + `tracing-opentelemetry` 0.29.x. Mismatched versions silently drop trace context.

Always call the SDK's shutdown function before exit (typically via a `shutdown_tracer_provider()` or scoped guard) so buffered spans flush. Lost-traces-on-shutdown is the most common observability bug in async Rust services.

### Metrics

Three reasonable choices: `metrics` (facade crate, like `tracing` for metrics), the `opentelemetry` metrics API, or `prometheus`-the-crate for direct Prometheus exposition. For a workspace already on OTLP, use OpenTelemetry metrics. Otherwise `metrics` + `metrics-exporter-prometheus` is the lightest path.

## 12. Testing

### Default toolkit

- **`cargo nextest`** as the runner (see CI section).
- **`#[tokio::test(flavor = "multi_thread")]`** for tests that spawn tasks; default (current-thread) for sequential tests.
- **`rstest`** for table-driven tests:
  ```rust
  #[rstest]
  #[case(b"\x00\x01\x02", Ok(MsgType::Hello))]
  #[case(b"\xff", Err(CodecError::InvalidTag { tag: 0xff }))]
  fn decode_msg_type(#[case] input: &[u8], #[case] expected: Result<MsgType, CodecError>) {
      assert_eq!(MsgType::decode(input), expected);
  }
  ```
  `rstest` also gives you fixtures (`#[fixture]`) — better than constants for nontrivial setup. `rstest` has largely displaced `test-case` and friends.
- **`proptest`** for property-based testing. Dominant over `quickcheck` in 2026 — better shrinking, derive macros via `proptest-derive`, integration with `arbitrary`. Use it on every parser/encoder pair to enforce round-trips:
  ```rust
  proptest! {
      #[test]
      fn roundtrip(msg in arb_message()) {
          let bytes = encode(&msg);
          let decoded = decode(&bytes).unwrap();
          prop_assert_eq!(msg, decoded);
      }
  }
  ```
- **`insta`** for snapshot tests. Use for any test whose expected output is a complex string (error formatting, generated XML, formatted protocol traces). `cargo insta review` for the interactive update workflow.
- **`mockall`** for mocking traits. Still the leading mocking framework; alternatives like `mock-it` and `faux` are niche. For most code, *don't* mock — testing real impls in-process is more valuable. Reserve mocks for trait boundaries that cross the network/filesystem/clock.
- **`tokio-test`** for `BufReader`/`BufWriter`-style mock async I/O and time control.

### Property tests for a codec workspace

Round-trip and "decode-fails-gracefully on arbitrary bytes" are the two indispensable proptests:

```rust
proptest! {
    #[test]
    fn decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..4096)) {
        let _ = Message::decode(&bytes);   // must not panic, return is OK
    }
}
```

Pair with `cargo-fuzz` (libFuzzer integration) for serious campaign-style fuzzing of the codec; proptest is for fast in-test coverage.

### Integration testing

- **`testcontainers`** for any test that needs a real database, message broker, or RTI peer. The async API is now ergonomic.
- **`wiremock`** for HTTP-level mocks at integration boundary.

### Test layout

The standard `crate/tests/*.rs` works at the workspace level; cross-crate integration tests should live in a dedicated `crates/integration-tests` crate that depends on the others. This avoids the cyclic-feature-flag problems of `#[cfg(test)]` shenanigans.

## 13. Security and Supply Chain

### Baseline

- **`cargo deny check`** in CI — replaces `cargo audit` (which still works but is single-purpose). `deny.toml` config above.
- **MSRV-aware resolver** (set `resolver = "3"`) means you can hold MSRV without manually pinning patch versions; Cargo prefers compatible versions.
- **Pin your toolchain** in `rust-toolchain.toml` for reproducibility in CI and Docker builds.
- **`cargo update --dry-run`** and review the diff before merging dependency updates.

### `cargo-vet` vs `cargo-crev`

- **`cargo-vet`** (Mozilla) is an audit-tracking tool: you record "we (or someone we trust) reviewed crate X version Y." Audits are cumulative and shareable. Use this if your organization has security review obligations. Google publishes its audit set; importing it covers a large fraction of common dependencies for free.
- **`cargo-crev`** is a web-of-trust system with cryptographic signing. Less corporate; more individual-contributor focused. Compatible with `cargo-vet` via the `crevette` exporter.

For most workspaces, set up `cargo-vet`, import Google + Mozilla audits, and audit anything left over in-house.

### Other tools

- **`cargo-auditable`** — embeds the dependency tree into your compiled binary so it can be scanned later (e.g., by `osv-scanner` or a SBOM tool). Use in release builds.
- **`cargo-cyclonedx`** — generates SBOMs in CycloneDX format. Often required by enterprise procurement.

### Namespaced crates: still pending

RFC 3243 (packages-as-optional-namespaces) is mostly implemented in `rustc` as of mid-2026 but has not yet shipped end-to-end on `crates.io`. For now, the `cratename-subname` convention is still the norm. Don't design your crate naming around it.

## 14. Documentation

### Rustdoc discipline

- **`#![warn(missing_docs)]`** in every library crate's `lib.rs`.
- **Every public item gets a doc comment** with at least a one-line summary; functions get an `# Examples` section, fallible functions get `# Errors`, panicking functions get `# Panics`, and `unsafe` functions get `# Safety`.
- **Doc tests are integration tests for free.** Make them meaningful — `# use hla4_codec::*;` hidden imports, real assertions.
- For long examples, hide infrastructure with `#` prefix and keep the "story" lines visible.

### `#[doc(cfg(...))]` for feature-gated items

Set `#![cfg_attr(docsrs, feature(doc_auto_cfg))]` in `lib.rs` so docs.rs auto-renders feature badges next to feature-gated items. Configure `[package.metadata.docs.rs]`:

```toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
targets = ["x86_64-unknown-linux-gnu"]
```

### `cargo doc --no-deps --workspace --document-private-items`

The workhorse local-doc-build command. Add `--open` for browser preview. CI should run `cargo doc --no-deps --workspace -D warnings` to catch broken intra-doc links.

### Rustdoc-scrape-examples

Stable since 1.80, enabled by adding `--scrape-examples-output-path` flags. Docs.rs runs it automatically. It surfaces real callers as examples — invaluable for an HLA library where typical usage is intricate.

### Project-level docs

Use **mdBook** for narrative documentation (architecture, user guides). Keep API reference in rustdoc, conceptual docs in mdBook. `mdbook test` runs Rust code blocks against your crate.

## 15. Notable Ecosystem Shifts

A snapshot of crate-level shifts that matter to a 2026 project:

### Datetime: chrono → jiff (for new code with timezone needs)

- **`jiff`** (by BurntSushi) is the new recommendation when you need timezone awareness. It bundles IANA TZ data, has DST-safe arithmetic, and rejects ambiguous datetimes by default rather than silently picking one.
- **`chrono`** is still fine, well-maintained, and ubiquitous. Don't migrate working code without reason; do use `jiff` for greenfield.
- **`time`** is also still maintained, but its timezone story is weak (relies on `time-tz`). Prefer `jiff` over `time` for new code.

### HTTP client: reqwest / hyper / ureq

- **`reqwest`** is the convenient default. Configure with `default-features = false, features = ["rustls-tls", "json"]` to drop OpenSSL.
- **`hyper` 1.x** directly when you're building a server framework or proxy and need precise control.
- **`ureq`** for blocking, simple HTTP clients (CLI tools, build scripts). Lightweight, no async runtime needed.

### Web framework: `axum` is the default

`axum` is the default choice in 2026, deeply integrated with `tower`, `tokio`, `tracing`, and `hyper`. `actix-web` retains a slight raw-throughput edge but the ecosystem gravity is around `axum`. `rocket` and `warp` are legacy choices for new projects.

### TLS: rustls everywhere

Use `rustls` 0.23+ with the `ring` or `aws-lc-rs` crypto provider. Drop `native-tls` and `openssl` unless you have a specific reason (FIPS validation, system trust store). Set `tokio-tungstenite` features to `rustls-tls-webpki-roots` and reqwest's to `rustls-tls`; never let `openssl-sys` sneak into your dependency graph (use `cargo deny` to enforce).

### Standard library absorbed

The following ecosystem crates have been *displaced by the standard library*. New code should not depend on them:

- **`lazy_static`** → `std::sync::LazyLock` (stable 1.80). For interior mutable lazy state, `OnceLock` (stable 1.70). The `once_cell` crate is still useful for its `unsync` types in single-threaded contexts, but the `sync` types are redundant.
- **`once_cell::sync::Lazy`** → `LazyLock`.
- **`once_cell::sync::OnceCell`** → `OnceLock`.
- **`scoped_threadpool`** (and `crossbeam::scope` for many uses) → `std::thread::scope` (stable 1.63).
- **`itertools::Itertools::chain`** is fine but plain `iter::chain` got a free function in 1.91.

### Deprecated / dormant

- **`failure`** — long dead. Migrate any straggler code to `thiserror` + `anyhow`.
- **`error-chain`** — dead.
- **`futures-cpupool`** — replaced by `tokio::task::spawn_blocking` and `rayon`.
- **`async-std`** — effectively unmaintained as of late 2024; tokio is the de facto runtime. Smol remains an active alternative for embedded/specialized cases.
- **`structopt`** — superseded by `clap` 3.x's `derive` feature, now mature in `clap` 4.x.
- **`serde_yaml`** — the original `dtolnay/serde-yaml` was archived in 2024. Use `serde_yml` (a maintained fork) or, if you control both ends, switch to TOML or JSON.
- **`actix`** the actor framework (separate from `actix-web`) — mostly dormant. For actor patterns, build directly on `tokio` channels, or use `ractor` / `kameo`.

### Worth knowing about

- **`bon`** for builders (covered above).
- **`derive_more`** for `From`/`Into`/`Display`/`AsRef`/`Deref` derives. Pairs naturally with newtypes.
- **`indexmap`** for insertion-ordered map/set. Used by serde\_yaml, toml, lots of config code.
- **`smallvec`** / **`tinyvec`** for stack-allocated small vectors. `tinyvec` is fully safe; `smallvec` uses `unsafe` for performance.
- **`bytes`** — `Bytes` / `BytesMut` for zero-copy buffer slicing in networking code. Tokio is built on it; use it directly in your codecs.
- **`tokio-util::codec`** — `Framed`, `LengthDelimitedCodec`, `Decoder`/`Encoder` traits. The right abstraction for layering protocol parsers on async streams.
- **`futures-concurrency`** for ergonomic concurrent combinators (`join`, `race`, `merge`) that work cleanly with cancellation. Better than nested `select!` in many cases.
- **`tower`** — middleware stack abstraction; `axum`, `tonic`, and reqwest's `tower` integration share its `Service` trait.
- **`papaya`** — newer lock-free hashmap; worth benchmarking against `DashMap` on contention-heavy workloads.

## Appendix: a starter `clippy` and `rustfmt` configuration

```toml
# rustfmt.toml
style_edition = "2024"
max_width = 100
group_imports = "StdExternalCrate"
imports_granularity = "Module"
# The following are still "unstable" features of rustfmt; require nightly to apply.
# Tolerable since fmt isn't load-bearing for correctness.
```

```toml
# clippy.toml
avoid-breaking-exported-api = false
msrv = "1.95.0"
cognitive-complexity-threshold = 30
too-many-arguments-threshold = 8
```

## Appendix: minimum viable CI matrix

```yaml
# .github/workflows/ci.yml (sketch)
jobs:
  check:
    strategy:
      matrix:
        toolchain: ["1.95.0", "stable", "beta"]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@master
        with: { toolchain: ${{ matrix.toolchain }}, components: "clippy,rustfmt" }
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --check
      - run: cargo clippy --workspace --all-targets --all-features -- -D warnings
      - run: cargo nextest run --workspace --all-features
      - run: cargo test --doc --workspace --all-features
  audit:
    steps:
      - uses: EmbarkStudios/cargo-deny-action@v2
  features:
    steps:
      - run: cargo install cargo-hack --locked
      - run: cargo hack --feature-powerset --depth 2 check --workspace
  msrv-verify:
    steps:
      - run: cargo install cargo-msrv --locked
      - run: cargo msrv verify
```

---

### Sources

- [Announcing Rust 1.85.0 and Rust 2024 — Rust Blog](https://blog.rust-lang.org/2025/02/20/Rust-1.85.0/)
- [Announcing Rust 1.86.0 — Rust Blog](https://blog.rust-lang.org/2025/04/03/Rust-1.86.0/)
- [Announcing Rust 1.88.0 — Rust Blog](https://blog.rust-lang.org/2025/06/26/Rust-1.88.0/)
- [Announcing Rust 1.91.0 — Rust Blog](https://blog.rust-lang.org/2025/10/30/Rust-1.91.0/)
- [Announcing Rust 1.92.0 — Rust Blog](https://blog.rust-lang.org/2025/12/11/Rust-1.92.0/)
- [Rust 1.93.0 changelog — releases.rs](https://releases.rs/docs/1.93.0/)
- [Rust 1.95.0 changelog — releases.rs](https://releases.rs/docs/1.95.0/)
- [Rust 2024 — The Rust Edition Guide](https://doc.rust-lang.org/edition-guide/rust-2024/index.html)
- [`unsafe_op_in_unsafe_fn` — Rust Edition Guide](https://doc.rust-lang.org/nightly/edition-guide/rust-2024/unsafe-op-in-unsafe-fn.html)
- [Changes to `impl Trait` in Rust 2024 — Rust Blog](https://blog.rust-lang.org/2024/09/05/impl-trait-capture-rules/)
- [RFC 3617 — precise capturing](https://rust-lang.github.io/rfcs/3617-precise-capturing.html)
- [Async: What is blocking? — Alice Ryhl](https://ryhl.io/blog/async-what-is-blocking/)
- [Cancelling async Rust — sunshowers](https://sunshowers.io/posts/cancelling-async-rust/)
- [Announcing `async fn` and RPITIT in traits — Rust Blog](https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/)
- [`trait_variant` docs](https://docs.rs/trait-variant)
- [Cargo Workspaces — The Cargo Book](https://doc.rust-lang.org/cargo/reference/workspaces.html)
- [Cargo `[lints]` table — The Cargo Book](https://doc.rust-lang.org/cargo/reference/lints.html)
- [Clippy Lints — official docs](https://rust-lang.github.io/rust-clippy/master/index.html)
- [cargo-nextest](https://nexte.st/)
- [cargo-deny](https://github.com/EmbarkStudios/cargo-deny)
- [cargo-machete](https://github.com/bnjbvr/cargo-machete)
- [cargo-hack](https://github.com/taiki-e/cargo-hack)
- [cargo-semver-checks](https://github.com/obi1kenobi/cargo-semver-checks)
- [cargo-mutants](https://github.com/sourcefrog/cargo-mutants)
- [Rust Performance Book — Build Configuration](https://nnethercote.github.io/perf-book/build-configuration.html)
- [Profile-guided Optimization — rustc book](https://doc.rust-lang.org/beta/rustc/profile-guided-optimization.html)
- [`bon` v3.0 release — typestate redesign](https://bon-rs.com/blog/bon-v3-release)
- [Future-proofing — Rust API Guidelines](https://rust-lang.github.io/api-guidelines/future-proofing.html)
- [`parking_lot` README](https://github.com/Amanieu/parking_lot)
- [`DashMap`](https://github.com/xacrimon/dashmap)
- [`rkyv` — Zero-copy deserialization](https://rkyv.org/)
- [Rust Serialization Benchmark](https://david.kolo.ski/rust_serialization_benchmark/)
- [`prost`](https://github.com/tokio-rs/prost)
- [`quick-xml` docs](https://docs.rs/quick-xml)
- [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry)
- [`miette`](https://docs.rs/miette)
- [`jiff` — comparison with chrono/time](https://github.com/BurntSushi/jiff/blob/master/COMPARE.md)
- [Standard library lazy types RFC 2788](https://rust-lang.github.io/rfcs/2788-standard-lazy-types.html)
- [cargo-vet](https://github.com/mozilla/cargo-vet)
- [RustSec Advisory DB](https://rustsec.org/)
- [WebSocket guide — tokio-tungstenite / axum / JoinSet](https://websocket.org/guides/languages/rust/)
- [MSRV-aware resolver — RFC 3537](https://rust-lang.github.io/rfcs/3537-msrv-resolver.html)
