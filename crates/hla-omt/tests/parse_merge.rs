//! End-to-end FOM parse + merge + handle lookup integration tests.

use hla_omt::{FomError, FomModule, MergedFom};

const SUSHI_LITE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<objectModel xmlns="http://standards.ieee.org/IEEE1516-2010">
  <modelIdentification>
    <name>SushiLite</name>
    <version>1.0</version>
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
          <objectClass>
            <name>Soda</name>
            <sharing>PublishSubscribe</sharing>
            <attribute>
              <name>Brand</name>
              <dataType>HLAunicodeString</dataType>
              <updateType>Static</updateType>
              <ownership>NoTransfer</ownership>
              <sharing>PublishSubscribe</sharing>
              <transportation>HLAreliable</transportation>
              <order>Receive</order>
            </attribute>
          </objectClass>
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
fn parse_then_merge_single_module() {
    let module = FomModule::parse(SUSHI_LITE).unwrap();
    let fom = MergedFom::merge(vec![module]).unwrap();

    // Depth-first handle assignment from HLAobjectRoot
    let root = fom.object_class_handle("HLAobjectRoot").unwrap();
    let food = fom.object_class_handle("HLAobjectRoot.Food").unwrap();
    let drink = fom.object_class_handle("HLAobjectRoot.Food.Drink").unwrap();
    let soda = fom
        .object_class_handle("HLAobjectRoot.Food.Drink.Soda")
        .unwrap();
    assert_eq!(root.raw(), 1);
    assert_eq!(food.raw(), 2);
    assert_eq!(drink.raw(), 3);
    assert_eq!(soda.raw(), 4);

    // Attributes get sequential handles in parent → child traversal order.
    let color = fom.attribute_handle(food, "Color").unwrap();
    let cups = fom.attribute_handle(drink, "NumberCups").unwrap();
    let brand = fom.attribute_handle(soda, "Brand").unwrap();
    assert_eq!(color.raw(), 1);
    assert_eq!(cups.raw(), 2);
    assert_eq!(brand.raw(), 3);

    // Attributes don't leak across classes.
    assert!(fom.attribute_handle(root, "Color").is_none());
    assert!(fom.attribute_handle(food, "NumberCups").is_none());
}

#[test]
fn inheritance_chain_walks_toward_root() {
    let fom = MergedFom::merge(vec![FomModule::parse(SUSHI_LITE).unwrap()]).unwrap();
    let soda = fom
        .object_class_handle("HLAobjectRoot.Food.Drink.Soda")
        .unwrap();
    let chain: Vec<u32> = fom.inheritance_chain(soda).map(|h| h.raw()).collect();
    let drink = fom.object_class_handle("HLAobjectRoot.Food.Drink").unwrap();
    let food = fom.object_class_handle("HLAobjectRoot.Food").unwrap();
    let root = fom.object_class_handle("HLAobjectRoot").unwrap();
    assert_eq!(chain, vec![soda.raw(), drink.raw(), food.raw(), root.raw()]);
}

#[test]
fn interaction_class_and_parameter_lookup() {
    let fom = MergedFom::merge(vec![FomModule::parse(SUSHI_LITE).unwrap()]).unwrap();
    let root = fom.interaction_class_handle("HLAinteractionRoot").unwrap();
    let served = fom
        .interaction_class_handle("HLAinteractionRoot.FoodServed")
        .unwrap();
    assert_eq!(root.raw(), 1);
    assert_eq!(served.raw(), 2);

    let food_type = fom.parameter_handle(served, "FoodType").unwrap();
    assert_eq!(food_type.raw(), 1);
    assert!(fom.parameter_handle(served, "Nonexistent").is_none());
}

#[test]
fn modular_merge_unions_attributes_on_same_class() {
    let base = r#"<?xml version="1.0"?>
<objectModel>
  <modelIdentification><name>Base</name></modelIdentification>
  <objects>
    <objectClass>
      <name>HLAobjectRoot</name>
      <objectClass>
        <name>Vehicle</name>
        <attribute>
          <name>Position</name>
          <dataType>HLAfloat64BE</dataType>
          <updateType>Periodic</updateType>
          <ownership>NoTransfer</ownership>
          <sharing>PublishSubscribe</sharing>
          <transportation>HLAreliable</transportation>
          <order>Receive</order>
        </attribute>
      </objectClass>
    </objectClass>
  </objects>
</objectModel>"#;
    let extension = r#"<?xml version="1.0"?>
<objectModel>
  <modelIdentification><name>Extension</name></modelIdentification>
  <objects>
    <objectClass>
      <name>HLAobjectRoot</name>
      <objectClass>
        <name>Vehicle</name>
        <attribute>
          <name>Velocity</name>
          <dataType>HLAfloat64BE</dataType>
          <updateType>Periodic</updateType>
          <ownership>NoTransfer</ownership>
          <sharing>PublishSubscribe</sharing>
          <transportation>HLAreliable</transportation>
          <order>Receive</order>
        </attribute>
      </objectClass>
    </objectClass>
  </objects>
</objectModel>"#;

    let m1 = FomModule::parse(base).unwrap();
    let m2 = FomModule::parse(extension).unwrap();
    let fom = MergedFom::merge(vec![m1, m2]).unwrap();

    let vehicle = fom.object_class_handle("HLAobjectRoot.Vehicle").unwrap();
    assert!(fom.attribute_handle(vehicle, "Position").is_some());
    assert!(fom.attribute_handle(vehicle, "Velocity").is_some());
    // And the two attribute handles must be distinct.
    assert_ne!(
        fom.attribute_handle(vehicle, "Position"),
        fom.attribute_handle(vehicle, "Velocity")
    );
}

#[test]
fn modular_merge_conflicting_attribute_types_errors() {
    let m1 = r#"<?xml version="1.0"?>
<objectModel>
  <modelIdentification><name>A</name></modelIdentification>
  <objects>
    <objectClass><name>HLAobjectRoot</name>
      <objectClass><name>Thing</name>
        <attribute>
          <name>X</name>
          <dataType>HLAinteger32BE</dataType>
          <updateType>Static</updateType>
        </attribute>
      </objectClass>
    </objectClass>
  </objects>
</objectModel>"#;
    let m2 = r#"<?xml version="1.0"?>
<objectModel>
  <modelIdentification><name>B</name></modelIdentification>
  <objects>
    <objectClass><name>HLAobjectRoot</name>
      <objectClass><name>Thing</name>
        <attribute>
          <name>X</name>
          <dataType>HLAfloat64BE</dataType>
          <updateType>Static</updateType>
        </attribute>
      </objectClass>
    </objectClass>
  </objects>
</objectModel>"#;

    let modules = vec![FomModule::parse(m1).unwrap(), FomModule::parse(m2).unwrap()];
    let err = MergedFom::merge(modules).unwrap_err();
    assert!(matches!(err, FomError::MergeConflict(_)), "{err:?}");
}

#[test]
fn duplicate_identical_attribute_across_modules_is_ok() {
    let s = r#"<?xml version="1.0"?>
<objectModel>
  <modelIdentification><name>X</name></modelIdentification>
  <objects>
    <objectClass><name>HLAobjectRoot</name>
      <objectClass><name>Item</name>
        <attribute>
          <name>Id</name>
          <dataType>HLAinteger32BE</dataType>
          <updateType>Static</updateType>
          <ownership>NoTransfer</ownership>
          <sharing>PublishSubscribe</sharing>
          <transportation>HLAreliable</transportation>
          <order>Receive</order>
        </attribute>
      </objectClass>
    </objectClass>
  </objects>
</objectModel>"#;
    let m = FomModule::parse(s).unwrap();
    // Same module twice — every field identical → merges cleanly, no conflict.
    let fom = MergedFom::merge(vec![m.clone(), m]).unwrap();
    let item = fom.object_class_handle("HLAobjectRoot.Item").unwrap();
    assert!(fom.attribute_handle(item, "Id").is_some());
}
