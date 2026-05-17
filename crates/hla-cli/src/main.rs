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

    // Wire Ctrl-C / SIGTERM to the node's cooperative shutdown token.
    // Once cancelled, every accept loop, per-connection task, and the
    // suspended-session janitor unwinds.
    let shutdown_node = Arc::clone(&node);
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "ctrl_c handler failed; shutdown will not be triggered");
            return;
        }
        tracing::info!("ctrl-c received — beginning graceful shutdown");
        shutdown_node.shutdown();
    });

    // `serve` returns Ok(()) once the shutdown token is cancelled.
    node.run().await?;
    tracing::info!("rtiexec exited cleanly");
    Ok(())
}
