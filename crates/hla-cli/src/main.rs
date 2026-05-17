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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let addr: SocketAddr = args.bind.parse()?;
    let node = Arc::new(RtiNode::new(addr));
    node.run().await?;
    Ok(())
}
