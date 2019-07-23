use std::convert::Into;

use serde_json::map::Map;
use serde_json::Value;

use cdrs::error::Result;
use cdrs::frame::frame_result::{ColType, RowsMetadata};
use cdrs::types::rows::Row;
use cdrs::types::IntoRustByIndex;

pub trait ToJsonValue {
    fn to_json(self) -> Value;
}

impl<T: Into<Value>> ToJsonValue for T {
    fn to_json(self) -> Value {
        self.into()
    }
}

fn column_to_json<R, T>(i: usize, row: &T) -> Result<Value>
where
    R: ToJsonValue,
    T: IntoRustByIndex<R>,
{
    let value = row.get_by_index(i)?;
    match value {
        Some(value) => Ok(value.to_json()),
        None => Ok(Value::Null),
    }
}

pub fn row_to_json(meta: &RowsMetadata, row: &Row) -> Result<Value> {
    let mut i = 0;
    let mut obj = Map::with_capacity(meta.columns_count as usize);

    for col in &meta.col_specs {
        let name = col.name.as_plain();
        let value = match &col.col_type.id {
            // strings
            ColType::Varchar | ColType::Ascii => column_to_json::<String, _>(i, row)?,
            // integers
            ColType::Tinyint => column_to_json::<i8, _>(i, row)?,
            ColType::Smallint => column_to_json::<i16, _>(i, row)?,
            ColType::Int => column_to_json::<i32, _>(i, row)?,
            ColType::Bigint | ColType::Counter => column_to_json::<i64, _>(i, row)?,
            // floats
            ColType::Float => column_to_json::<f32, _>(i, row)?,
            ColType::Double => column_to_json::<f64, _>(i, row)?,
            // bool
            ColType::Boolean => column_to_json::<bool, _>(i, row)?,
            // TODO format date time according to CLI option
            ColType::Time | ColType::Timestamp => column_to_json::<i64, _>(i, row)?,
            // null
            ColType::Null => Value::Null,
            //TODO Implement other types: Blob, Udt etc
            _ => Value::Null,
        };
        obj.insert(name, value);
        i = i + 1;
    }
    Ok(obj.into())
}

#[cfg(test)]
mod tests {

    use super::*;

    struct MockRow<R> {
        val: R,
    }

    impl<R> MockRow<R> {
        pub fn new(v: R) -> MockRow<R> {
            MockRow { val: v }
        }
    }

    impl<R: Clone> IntoRustByIndex<R> for MockRow<R> {
        fn get_by_index(&self, _: usize) -> Result<Option<R>> {
            Ok(Some(self.val.clone()))
        }

        fn get_r_by_index(&self, _: usize) -> Result<R> {
            Ok(self.val.clone())
        }
    }

    #[test]
    pub fn string_to_json() {
        let s = "hello world";
        let mock_row = MockRow::new(String::from(s));
        let json_res =
            column_to_json::<String, _>(0, &mock_row).expect("failed to convert to json");
        assert_eq!(Value::from(s), json_res);
    }
}
