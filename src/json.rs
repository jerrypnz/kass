use std::convert::{Into, identity};

use cdrs::frame::frame_result::{ColType, RowsMetadata};
use cdrs::types::rows::Row;
use cdrs::types::IntoRustByIndex;
use serde_json::map::Map;
use serde_json::Value;

use crate::convert;
use crate::errors::AppResult;

fn convert_col_to_json<R, S, T, F>(i: usize, row: &T, f: F) -> AppResult<Value>
where
    S: Into<Value>,
    T: IntoRustByIndex<R>,
    F: Fn(R) -> S,
{
    let value = row.get_by_index(i)?;
    match value {
        Some(value) => Ok(f(value).into()),
        None => Ok(Value::Null),
    }
}

fn col_to_json<S: Into<Value>, T: IntoRustByIndex<S>>(i: usize, row: &T) -> AppResult<Value> {
    convert_col_to_json(i, row, identity)
}

pub fn row_to_json(meta: &RowsMetadata, row: &Row) -> AppResult<String> {
    let mut i = 0;
    let mut obj = Map::with_capacity(meta.columns_count as usize);

    for col in &meta.col_specs {
        let name = col.name.as_plain();
        let value = match &col.col_type.id {
            // strings
            ColType::Varchar | ColType::Ascii => col_to_json::<String, _>(i, row)?,
            // integers
            ColType::Tinyint => col_to_json::<i8, _>(i, row)?,
            ColType::Smallint => col_to_json::<i16, _>(i, row)?,
            ColType::Int => col_to_json::<i32, _>(i, row)?,
            ColType::Bigint | ColType::Counter => col_to_json::<i64, _>(i, row)?,
            // floats
            ColType::Float => col_to_json::<f32, _>(i, row)?,
            ColType::Double => col_to_json::<f64, _>(i, row)?,
            // bool
            ColType::Boolean => col_to_json::<bool, _>(i, row)?,
            // date time
            ColType::Date => convert_col_to_json(i, row, convert::to_date_str)?,
            ColType::Time => convert_col_to_json(i, row, convert::to_time_str)?,
            ColType::Timestamp => convert_col_to_json(i, row, convert::to_date_time_str)?,
            // IP
            ColType::Inet => convert_col_to_json(i, row, convert::to_ip_str)?,
            // UUID
            ColType::Uuid => convert_col_to_json(i, row, convert::to_uuid_str)?,
            // null
            ColType::Null => Value::Null,
            //TODO Implement other types: Blob, Udt etc
            _ => Value::Null,
        };
        obj.insert(name, value);
        i = i + 1;
    }

    let row_obj: Value = obj.into();
    serde_json::to_string(&row_obj).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {

    use super::*;
    use cdrs::error::Result;

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
        let json_res = col_to_json::<String, _>(0, &mock_row).expect("failed to convert to json");
        assert_eq!(Value::from(s), json_res);
    }
}
