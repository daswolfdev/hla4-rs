//! Sample subscriber federate. Connects to an RTI, joins the
//! `sushi-demo` federation, subscribes to `HLAobjectRoot.Food.Drink.NumberCups`,
//! and logs every `discoverObjectInstance` and `reflectAttributeValues`
//! callback until the user hits Ctrl-C.
//!
//! Usage: `cargo run -p sushi-subscriber -- --rti rti://127.0.0.1:15164`

use std::sync::Arc;

use clap::Parser;
use hla_core::{
    AttributeHandleSet, AttributeHandleValueMap, FederateHandle, ObjectClassHandle,
    ObjectInstanceHandle, ResignAction,
};
use hla_federate::{FederateAmbassador, RtiAmbassador};

#[derive(Parser, Debug)]
#[command(name = "sushi-subscriber")]
struct Args {
    #[arg(long, default_value = "rti://127.0.0.1:15164")]
    rti: String,
    #[arg(long, default_value = "sushi-demo")]
    federation: String,
    #[arg(long, default_value = "Consumer-1")]
    name: String,
}

struct Logger;

impl FederateAmbassador for Logger {
    async fn discover_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        class: ObjectClassHandle,
        name: String,
        producer: Option<FederateHandle>,
    ) {
        tracing::info!(?instance, ?class, name, ?producer, "discovered");
    }

    async fn reflect_attribute_values(
        &self,
        instance: ObjectInstanceHandle,
        values: AttributeHandleValueMap,
        tag: Vec<u8>,
        _producer: Option<FederateHandle>,
    ) {
        let pretty: Vec<String> = values
            .iter()
            .map(|(a, v)| {
                let parsed = if v.len() == 4 {
                    i32::from_be_bytes(v[..].try_into().unwrap()).to_string()
                } else {
                    format!("{v:?}")
                };
                format!("attr={} value={}", a.raw(), parsed)
            })
            .collect();
        tracing::info!(
            ?instance,
            tag = %String::from_utf8_lossy(&tag),
            "{}",
            pretty.join(", ")
        );
    }

    async fn remove_object_instance(
        &self,
        instance: ObjectInstanceHandle,
        _tag: Vec<u8>,
        _producer: Option<FederateHandle>,
    ) {
        tracing::info!(?instance, "removed");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let amb = RtiAmbassador::connect(&args.rti, Arc::new(Logger)).await?;
    let _ = amb.create_federation_execution(&args.federation).await;
    amb.join_federation_execution(&args.name, &args.federation).await?;

    let drink = amb.get_object_class_handle("HLAobjectRoot.Food.Drink").await?;
    let cups = amb.get_attribute_handle(drink, "NumberCups").await?;
    let mut attrs = AttributeHandleSet::new();
    attrs.insert(cups);
    amb.subscribe_object_class_attributes(drink, attrs).await?;
    tracing::info!("subscribed; awaiting callbacks (Ctrl-C to stop)");

    tokio::signal::ctrl_c().await?;
    amb.resign_federation_execution(ResignAction::NoAction).await?;
    amb.disconnect().await?;
    Ok(())
}
