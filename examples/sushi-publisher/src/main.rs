//! Sample publisher federate. Connects to an RTI, creates the
//! `sushi-demo` federation (idempotent), joins as a `Producer`,
//! publishes `HLAobjectRoot.Food.Drink.NumberCups`, registers a single
//! Drink instance, then emits 10 updates one per second.
//!
//! Usage: `cargo run -p sushi-publisher -- --rti rti://127.0.0.1:15164`

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use hla_core::{AttributeHandleSet, AttributeHandleValueMap, ResignAction};
use hla_federate::{FederateAmbassador, RtiAmbassador};

#[derive(Parser, Debug)]
#[command(name = "sushi-publisher")]
struct Args {
    /// RTI URL. Accepts `rti://host:port` or `host:port`.
    #[arg(long, default_value = "rti://127.0.0.1:15164")]
    rti: String,
    /// Federation name.
    #[arg(long, default_value = "sushi-demo")]
    federation: String,
    /// Federate name visible to other federates.
    #[arg(long, default_value = "Producer-1")]
    name: String,
    /// Number of update ticks to emit.
    #[arg(long, default_value_t = 10)]
    ticks: u32,
}

struct Noop;
impl FederateAmbassador for Noop {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let amb = RtiAmbassador::connect(&args.rti, Arc::new(Noop)).await?;
    let _ = amb.create_federation_execution(&args.federation).await; // ignore "already exists"
    amb.join_federation_execution(&args.name, &args.federation).await?;

    let drink = amb.get_object_class_handle("HLAobjectRoot.Food.Drink").await?;
    let cups = amb.get_attribute_handle(drink, "NumberCups").await?;
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(cups);

    amb.publish_object_class_attributes(drink, attrs).await?;
    let instance = amb.register_object_instance(drink).await?;
    tracing::info!(?instance, "registered Drink instance");

    for tick in 1..=args.ticks {
        let mut values = AttributeHandleValueMap::new();
        values.insert(cups, (tick as i32).to_be_bytes().to_vec());
        amb.update_attribute_values(instance, values, b"tick").await?;
        tracing::info!(tick, "sent update");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    amb.delete_object_instance(instance, b"all done").await?;
    amb.resign_federation_execution(ResignAction::DeleteObjects).await?;
    amb.disconnect().await?;
    Ok(())
}
