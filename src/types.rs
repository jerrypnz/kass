//use std::collections::HashMap;
use std::convert::TryInto;
use std::net::IpAddr;

use cdrs::error::Result as CDRSResult;
use cdrs::frame::frame_result::{ColSpec, ColType};
use cdrs::types::data_serialization_types::*;
use cdrs::types::CBytes;
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use serde::{Serialize, Serializer};
use uuid;

// Uuid wrapper struct for implementing `Serialize` trait
pub struct Uuid {
    uuid: uuid::Uuid,
}

impl Serialize for Uuid {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let s = self.uuid.to_hyphenated_ref().to_string();
        serializer.serialize_str(s.as_str())
    }
}

// More concice version of Cassandra data types.
#[derive(Serialize)]
#[serde(untagged)]
pub enum ColValue {
    Null,
    Int(i64),
    Double(f64),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(DateTime<Utc>),
    Inet(IpAddr),
    Uuid(Uuid),
    Boolean(bool),
    String(String),
    //Seq(Vec<ColValue>),
    //Map(HashMap<String, ColValue>),
}

pub fn to_time(t: i64) -> NaiveTime {
    let secs: u32 = (t / 1000_000_000).try_into().expect("Value out of range");
    let nano: u32 = (t % 1000_000_000).try_into().expect("Value out of range");
    NaiveTime::from_num_seconds_from_midnight(secs, nano)
}

fn to_date(d: i32) -> NaiveDate {
    let ts: i64 = i64::from(d) * 24 * 60 * 60 * 1000;
    Utc.timestamp_millis(ts).naive_utc().date()
}

fn to_datetime(t: i64) -> DateTime<Utc> {
    Utc.timestamp_millis(t)
}

pub fn decode_value(spec: &ColSpec, data: &CBytes) -> CDRSResult<ColValue> {
    if let Some(ref bytes) = data.as_plain() {
        let value = match &spec.col_type.id {
            // strings
            ColType::Varchar => ColValue::String(decode_varchar(bytes)?),
            ColType::Ascii => ColValue::String(decode_ascii(bytes)?),
            ColType::Custom => ColValue::String(decode_custom(bytes)?),
            // integers
            ColType::Tinyint => ColValue::Int(decode_tinyint(bytes)? as i64),
            ColType::Smallint => ColValue::Int(decode_smallint(bytes)? as i64),
            ColType::Int => ColValue::Int(decode_int(bytes)? as i64),
            ColType::Bigint => ColValue::Int(decode_bigint(bytes)?),
            ColType::Counter => ColValue::Int(decode_bigint(bytes)?),
            // floats
            ColType::Float => ColValue::Double(decode_float(bytes)? as f64),
            ColType::Double => ColValue::Double(decode_double(bytes)?),
            // bool
            ColType::Boolean => ColValue::Boolean(decode_boolean(bytes)?),
            // date time
            ColType::Date => ColValue::Date(to_date(decode_date(bytes)?)),
            ColType::Time => ColValue::Time(to_time(decode_time(bytes)?)),
            ColType::Timestamp => ColValue::Timestamp(to_datetime(decode_timestamp(bytes)?)),
            // IP
            ColType::Inet => ColValue::Inet(decode_inet(bytes)?),
            // UUID
            ColType::Uuid | ColType::Timeuuid => ColValue::Uuid(Uuid {
                uuid: decode_timeuuid(bytes)?,
            }),
            // null
            ColType::Null => ColValue::Null,
            //TODO Implement other types: Blob, Udt etc
            _ => ColValue::String(String::from("__UNSUPPORTED TYPE__")),
        };
        Ok(value)
    } else {
        Ok(ColValue::Null)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::mem;

    #[test]
    pub fn test_col_value_size() {
        assert_eq!(64, mem::size_of::<ColValue>());
    }
}
