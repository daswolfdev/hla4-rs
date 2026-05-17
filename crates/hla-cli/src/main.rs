//! `rtiexec` — start an HLA 4 RTI node.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use hla_rti::RtiNode;
use hla_wire::session::DEFAULT_PORT;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "rtiexec", about = "HLA 4 RTI server")]
struct Args {
    /// Address to bind. Defaults to 0.0.0.0:15164 (the HLA 4 default port).
    #[arg(long, default_value_t = format!("0.0.0.0:{DEFAULT_PORT}"))]
    bind: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let addr: SocketAddr = args.bind.parse()?;
    let node = Arc::new(RtiNode::new(addr));

    // Wire Ctrl-C (SIGINT) and SIGTERM to the node's cooperative
    // shutdown token. SIGTERM is the conventional signal from
    // systemd / container runtimes (`docker stop`, `kubectl delete`);
    // ctrl_c maps to SIGINT for interactive use.
    let shutdown_node = Arc::clone(&node);
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        shutdown_node.shutdown();
    });

    // `serve` returns Ok(()) once the shutdown token is cancelled.
    node.run().await?;
    tracing::info!("rtiexec exited cleanly");
    Ok(())
}

/// Wait for the first SIGINT or SIGTERM and return. On non-Unix targets,
/// only SIGINT (via `ctrl_c`) is observed.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "could not install SIGTERM handler; ctrl-c only");
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("SIGINT received — beginning graceful shutdown");
                return;
            }
        };
        tokio::select! {
            r = tokio::signal::ctrl_c() => {
                if let Err(e) = r {
                    tracing::warn!(error = %e, "ctrl_c handler failed");
                    return;
                }
                tracing::info!("SIGINT received — beginning graceful shutdown");
            }
            _ = term.recv() => {
                tracing::info!("SIGTERM received — beginning graceful shutdown");
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "ctrl_c handler failed");
            return;
        }
        tracing::info!("ctrl-c received — beginning graceful shutdown");
    }
}
