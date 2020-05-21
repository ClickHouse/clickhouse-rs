pub use reflection::Reflection;
use reflection::{Member, Schemas, Type};
pub use reflection_derive::Reflection;
use smallvec::SmallVec;

fn is_wrapper(schemas: &Schemas) -> bool {
    if schemas.degree() != 1 {
        return false;
    }

    match &schemas.first().unwrap().data {
        Member::Field(field) if field.id == "0" || field.id == "_" => true,
        _ => false,
    }
}

/// Collects all field names in depth and joins them with comma.
pub fn join_field_names<T: Reflection>() -> String {
    fn add_part(result: &mut String, path: &[&str], name: &str) {
        if !result.is_empty() {
            result.push(',');
        }

        for part in path {
            result.push_str(part);
            result.push('.');
        }

        if name.is_empty() {
            result.pop();
        } else {
            result.push_str(name);
        }
    }

    type Path<'a> = SmallVec<[&'a str; 3]>;

    fn collect(path: &mut Path<'_>, nodes: Schemas, result: &mut String) {
        let is_wrapper = is_wrapper(&nodes);

        for node in nodes {
            match &node.data {
                Member::Field(field)
                    if field.ty == Type::Struct
                        || field.ty == Type::Rc
                        || field.ty == Type::Box =>
                {
                    let nested = field.expander.unwrap()();

                    if is_wrapper {
                        collect(path, nested, result);
                    } else {
                        path.push(field.id);
                        collect(path, nested, result);
                        path.pop();
                    }
                }
                Member::Field(_) if is_wrapper => add_part(result, path, ""),
                Member::Field(field) => add_part(result, path, field.id),
                Member::Variant(_) => unimplemented!(),
            }
        }
    }

    let mut result = String::new();
    collect(&mut Path::new(), T::members(), &mut result);
    result
}

#[test]
fn it_grabs_simple_struct() {
    #[derive(Reflection)]
    #[allow(dead_code)]
    struct Simple1 {
        one: u32,
    }

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct Simple2 {
        one: u32,
        two: u32,
    }

    assert_eq!(join_field_names::<Simple1>(), "one");
    assert_eq!(join_field_names::<Simple2>(), "one,two");
}

#[test]
fn it_handles_nested_struct() {
    #[derive(Reflection)]
    #[allow(dead_code)]
    struct TopLevel {
        one: u32,
        nested: Nested1,
    }

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct Nested1 {
        two: u32,
        three: u32,
        nested: Nested2,
    }

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct Nested2 {
        four: u32,
    }

    assert_eq!(
        join_field_names::<TopLevel>(),
        "one,nested.two,nested.three,nested.nested.four"
    );
}

#[test]
fn it_unwraps_newtype() {
    #[derive(Reflection)]
    #[allow(dead_code)]
    struct TopLevelWrapper(TopLevel);

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct TopLevel {
        one: One1,
        two: Two1,
    }

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct One1(One2);

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct One2(u32);

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct Two1(Two2);

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct Two2(Two3);

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct Two3(i32);

    assert_eq!(join_field_names::<TopLevel>(), "one,two");
    assert_eq!(join_field_names::<TopLevelWrapper>(), "one,two");
}

#[test]
fn it_unwraps_rc() {
    use std::rc::Rc;

    type TopLevelWrapper = Rc<TopLevel>;

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct TopLevel {
        one: Rc<One1>,
    }

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct One1(Box<One2>);

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct One2(Rc<One3>);

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct One3 {
        foo: u32,
    }

    assert_eq!(join_field_names::<TopLevel>(), "one.foo");
    assert_eq!(join_field_names::<TopLevelWrapper>(), "one.foo");
}

#[test]
fn it_handles_arrays_as_terminals() {
    use reflection::{terminal, Id, Schema};

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct TopLevel {
        one: Vec<Sample>,
        two: Bytes,
    }

    #[derive(Reflection)]
    #[allow(dead_code)]
    struct Sample {
        lol: u8,
        kek: u32,
    }

    struct Bytes(Vec<u8>);

    impl Reflection for Bytes {
        fn schema(id: Id) -> Schema {
            terminal(id, Type::Array)
        }
    }

    assert_eq!(join_field_names::<TopLevel>(), "one,two");
}

#[test]
fn it_supports_renaming() {
    use serde::Serialize;

    #[derive(Reflection, Serialize)]
    #[allow(dead_code)]
    struct TopLevel {
        #[serde(rename = "some.one")]
        one: u32,
    }

    assert_eq!(join_field_names::<TopLevel>(), "some.one");
}
