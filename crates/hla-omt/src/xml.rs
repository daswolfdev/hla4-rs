//! XML deserialization for OMT FOM modules.
//!
//! Schema: IEEE 1516.2-2010 (carried forward largely unchanged in 1516.2-2025).
//! Namespaces are intentionally ignored — serde-via-quick-xml matches element
//! names without prefix qualification.

use serde::Deserialize;

use crate::{
    AttributeDef, FomError, FomModule, InteractionClassDef, ModuleIdentification, ObjectClassDef,
    OrderTypeRef, Ownership, ParameterDef, Sharing, UpdateType,
};

pub(crate) fn parse_fom_module(xml_input: &str) -> Result<FomModule, FomError> {
    let raw: XmlObjectModel =
        quick_xml::de::from_str(xml_input).map_err(|e| FomError::Xml(e.to_string()))?;
    raw.into_fom_module()
}

// -----------------------------------------------------------------------------
// XML wire types (deserialize only)
// -----------------------------------------------------------------------------

#[derive(Deserialize, Debug)]
struct XmlObjectModel {
    #[serde(default, rename = "modelIdentification")]
    model_identification: Option<XmlModelIdentification>,
    #[serde(default)]
    objects: Option<XmlObjects>,
    #[serde(default)]
    interactions: Option<XmlInteractions>,
}

#[derive(Deserialize, Debug, Default)]
struct XmlModelIdentification {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
struct XmlObjects {
    #[serde(default, rename = "objectClass")]
    object_classes: Vec<XmlObjectClass>,
}

#[derive(Deserialize, Debug)]
struct XmlObjectClass {
    name: String,
    #[serde(default)]
    semantics: Option<String>,
    #[serde(default, rename = "attribute")]
    attributes: Vec<XmlAttribute>,
    #[serde(default, rename = "objectClass")]
    children: Vec<XmlObjectClass>,
}

#[derive(Deserialize, Debug)]
struct XmlAttribute {
    name: String,
    #[serde(rename = "dataType")]
    data_type: String,
    #[serde(rename = "updateType")]
    update_type: String,
    #[serde(default)]
    ownership: Option<String>,
    #[serde(default)]
    sharing: Option<String>,
    #[serde(default)]
    transportation: Option<String>,
    #[serde(default)]
    order: Option<String>,
    #[serde(default)]
    dimensions: Option<XmlDimensions>,
}

#[derive(Deserialize, Debug, Default)]
struct XmlDimensions {
    #[serde(default, rename = "dimension")]
    dimensions: Vec<XmlDimensionRef>,
}

#[derive(Deserialize, Debug)]
struct XmlDimensionRef {
    #[serde(rename = "$text", default)]
    text: String,
}

#[derive(Deserialize, Debug, Default)]
struct XmlInteractions {
    #[serde(default, rename = "interactionClass")]
    interaction_classes: Vec<XmlInteractionClass>,
}

#[derive(Deserialize, Debug)]
struct XmlInteractionClass {
    name: String,
    #[serde(default)]
    semantics: Option<String>,
    #[serde(default)]
    transportation: Option<String>,
    #[serde(default)]
    order: Option<String>,
    #[serde(default, rename = "parameter")]
    parameters: Vec<XmlParameter>,
    #[serde(default, rename = "interactionClass")]
    children: Vec<XmlInteractionClass>,
}

#[derive(Deserialize, Debug)]
struct XmlParameter {
    name: String,
    #[serde(rename = "dataType")]
    data_type: String,
}

// -----------------------------------------------------------------------------
// XML → domain
// -----------------------------------------------------------------------------

impl XmlObjectModel {
    fn into_fom_module(self) -> Result<FomModule, FomError> {
        let identification = self
            .model_identification
            .map(|m| ModuleIdentification {
                name: m.name.unwrap_or_default(),
                version: m.version,
                description: m.description,
            })
            .unwrap_or_default();

        let mut object_classes = Vec::new();
        if let Some(objs) = self.objects {
            for top in objs.object_classes {
                flatten_object_class(top, None, &mut object_classes)?;
            }
        }

        let mut interaction_classes = Vec::new();
        if let Some(ixs) = self.interactions {
            for top in ixs.interaction_classes {
                flatten_interaction_class(top, None, &mut interaction_classes)?;
            }
        }

        Ok(FomModule {
            identification,
            object_classes,
            interaction_classes,
            datatypes: Default::default(),
            dimensions: Vec::new(),
        })
    }
}

fn fqn(parent: Option<&str>, leaf: &str) -> String {
    match parent {
        Some(p) => format!("{p}.{leaf}"),
        None => leaf.to_string(),
    }
}

fn flatten_object_class(
    node: XmlObjectClass,
    parent_fqn: Option<&str>,
    out: &mut Vec<ObjectClassDef>,
) -> Result<(), FomError> {
    let my_fqn = fqn(parent_fqn, &node.name);

    let attributes = node
        .attributes
        .into_iter()
        .map(|a| -> Result<AttributeDef, FomError> {
            Ok(AttributeDef {
                name: a.name,
                datatype: a.data_type,
                order: OrderTypeRef::from_xml(a.order.as_deref().unwrap_or("Receive"))?,
                transportation: a.transportation.unwrap_or_else(|| "HLAreliable".into()),
                ownership: Ownership::from_xml(a.ownership.as_deref().unwrap_or("NoTransfer"))?,
                sharing: Sharing::from_xml(a.sharing.as_deref().unwrap_or("Neither"))?,
                update_type: UpdateType::from_xml(a.update_type.as_str())?,
                dimensions: a
                    .dimensions
                    .map(|d| {
                        d.dimensions
                            .into_iter()
                            .map(|r| r.text)
                            .filter(|s| !s.is_empty())
                            .collect()
                    })
                    .unwrap_or_default(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    out.push(ObjectClassDef {
        name: my_fqn.clone(),
        parent: parent_fqn.map(str::to_string),
        attributes,
        semantics: node.semantics,
    });

    for child in node.children {
        flatten_object_class(child, Some(&my_fqn), out)?;
    }
    Ok(())
}

fn flatten_interaction_class(
    node: XmlInteractionClass,
    parent_fqn: Option<&str>,
    out: &mut Vec<InteractionClassDef>,
) -> Result<(), FomError> {
    let my_fqn = fqn(parent_fqn, &node.name);

    let parameters = node
        .parameters
        .into_iter()
        .map(|p| ParameterDef {
            name: p.name,
            datatype: p.data_type,
        })
        .collect();

    out.push(InteractionClassDef {
        name: my_fqn.clone(),
        parent: parent_fqn.map(str::to_string),
        order: OrderTypeRef::from_xml(node.order.as_deref().unwrap_or("Receive"))?,
        transportation: node.transportation.unwrap_or_else(|| "HLAreliable".into()),
        parameters,
        semantics: node.semantics,
    });

    for child in node.children {
        flatten_interaction_class(child, Some(&my_fqn), out)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINI_FOM: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification>
    <name>MiniRestaurant</name>
    <version>1.0</version>
    <description>Tiny test FOM</description>
  </modelIdentification>
  <objects>
    <objectClass>
      <name>HLAobjectRoot</name>
      <sharing>Neither</sharing>
      <objectClass>
        <name>Food</name>
        <sharing>PublishSubscribe</sharing>
        <attribute>
          <name>Color</name>
          <dataType>HLAunicodeString</dataType>
          <updateType>Static</updateType>
          <ownership>NoTransfer</ownership>
          <sharing>PublishSubscribe</sharing>
          <transportation>HLAreliable</transportation>
          <order>Receive</order>
        </attribute>
        <objectClass>
          <name>Drink</name>
          <sharing>PublishSubscribe</sharing>
          <attribute>
            <name>NumberCups</name>
            <dataType>HLAinteger32BE</dataType>
            <updateType>Static</updateType>
            <ownership>NoTransfer</ownership>
            <sharing>PublishSubscribe</sharing>
            <transportation>HLAreliable</transportation>
            <order>Receive</order>
          </attribute>
        </objectClass>
      </objectClass>
    </objectClass>
  </objects>
  <interactions>
    <interactionClass>
      <name>HLAinteractionRoot</name>
      <sharing>Neither</sharing>
      <transportation>HLAreliable</transportation>
      <order>Receive</order>
      <interactionClass>
        <name>FoodServed</name>
        <sharing>PublishSubscribe</sharing>
        <transportation>HLAreliable</transportation>
        <order>Receive</order>
        <parameter>
          <name>FoodType</name>
          <dataType>HLAunicodeString</dataType>
        </parameter>
      </interactionClass>
    </interactionClass>
  </interactions>
</objectModel>"#;

    #[test]
    fn parses_mini_fom_identification() {
        let m = parse_fom_module(MINI_FOM).expect("parse");
        assert_eq!(m.identification.name, "MiniRestaurant");
        assert_eq!(m.identification.version.as_deref(), Some("1.0"));
    }

    #[test]
    fn flattens_object_class_hierarchy() {
        let m = parse_fom_module(MINI_FOM).expect("parse");
        let names: Vec<&str> = m.object_classes.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "HLAobjectRoot",
                "HLAobjectRoot.Food",
                "HLAobjectRoot.Food.Drink",
            ]
        );

        let drink = m
            .object_classes
            .iter()
            .find(|c| c.name == "HLAobjectRoot.Food.Drink")
            .unwrap();
        assert_eq!(drink.parent.as_deref(), Some("HLAobjectRoot.Food"));
        assert_eq!(drink.attributes.len(), 1);
        assert_eq!(drink.attributes[0].name, "NumberCups");
        assert_eq!(drink.attributes[0].datatype, "HLAinteger32BE");
    }

    #[test]
    fn flattens_interaction_class_hierarchy() {
        let m = parse_fom_module(MINI_FOM).expect("parse");
        let names: Vec<&str> = m
            .interaction_classes
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["HLAinteractionRoot", "HLAinteractionRoot.FoodServed"]
        );
        let served = m
            .interaction_classes
            .iter()
            .find(|c| c.name == "HLAinteractionRoot.FoodServed")
            .unwrap();
        assert_eq!(served.parameters.len(), 1);
        assert_eq!(served.parameters[0].name, "FoodType");
    }

    #[test]
    fn parses_attribute_metadata() {
        let m = parse_fom_module(MINI_FOM).expect("parse");
        let food = m
            .object_classes
            .iter()
            .find(|c| c.name == "HLAobjectRoot.Food")
            .unwrap();
        let color = &food.attributes[0];
        assert_eq!(color.order, OrderTypeRef::Receive);
        assert_eq!(color.ownership, Ownership::NoTransfer);
        assert_eq!(color.sharing, Sharing::PublishSubscribe);
        assert_eq!(color.update_type, UpdateType::Static);
        assert_eq!(color.transportation, "HLAreliable");
    }

    #[test]
    fn rejects_unknown_enum_value() {
        let bad = MINI_FOM.replace(
            "<updateType>Static</updateType>",
            "<updateType>Bogus</updateType>",
        );
        let err = parse_fom_module(&bad).unwrap_err();
        assert!(matches!(err, FomError::InvalidEnum(_)));
    }
}
