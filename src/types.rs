use std::collections::HashMap;
use std::convert::TryInto;
use std::net::IpAddr;

use cdrs::error::{Error as CDRSError, Result as CDRSResult};
use cdrs::frame::frame_result::{ColType, ColTypeOption, ColTypeOptionValue};
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
    Seq(Vec<ColValue>),
    Map(HashMap<String, ColValue>),
}

impl ColValue {
    fn as_map_key(self) -> CDRSResult<String> {
        match self {
            ColValue::String(x) => Ok(x),
            ColValue::Int(x) => Ok(format!("{}", x)),
            ColValue::Boolean(x) => Ok(format!("{}", x)),
            ColValue::Double(x) => Ok(format!("{}", x)),
            ColValue::Date(x) => Ok(format!("{}", x)),
            ColValue::Time(x) => Ok(format!("{}", x)),
            ColValue::Timestamp(x) => Ok(format!("{}", x)),
            ColValue::Inet(x) => Ok(format!("{}", x)),
            ColValue::Uuid(x) => Ok(format!("{}", x.uuid)),
            _ => Err(CDRSError::General("Unexpected map key type".into())),
        }
    }
}

pub fn to_time(t: i64) -> NaiveTime {
    let secs: u32 = (t / 1000_000_000).try_into().unwrap_or(0);
    let nano: u32 = (t % 1000_000_000).try_into().unwrap_or(0);
    NaiveTime::from_num_seconds_from_midnight(secs, nano)
}

fn to_date(d: i32) -> NaiveDate {
    let ts: i64 = i64::from(d) * 24 * 60 * 60 * 1000;
    Utc.timestamp_millis(ts).naive_utc().date()
}

fn to_datetime(t: i64) -> DateTime<Utc> {
    Utc.timestamp_millis(t)
}

fn to_seq(meta: &Option<ColTypeOptionValue>, data: &Vec<CBytes>) -> CDRSResult<Vec<ColValue>> {
    match meta {
        Some(ColTypeOptionValue::CList(elem_type)) | Some(ColTypeOptionValue::CSet(elem_type)) => {
            data.iter().map(|x| decode_value(elem_type, x)).collect()
        }
        _ => Err(CDRSError::General("Error converting list/set".into())),
    }
}

fn to_tuple(meta: &Option<ColTypeOptionValue>, bytes: &[u8]) -> CDRSResult<Vec<ColValue>> {
    if let Some(ColTypeOptionValue::TupleType(tuple_meta)) = meta {
        let data = decode_tuple(bytes, tuple_meta.types.len())?;
        tuple_meta
            .types
            .iter()
            .zip(data.iter())
            .map(|(t, x)| decode_value(t, x))
            .collect()
    } else {
        Err(CDRSError::General("Error converting tuple".into()))
    }
}

fn to_udt(
    meta: &Option<ColTypeOptionValue>,
    bytes: &[u8],
) -> CDRSResult<HashMap<String, ColValue>> {
    if let Some(ColTypeOptionValue::UdtType(udt_meta)) = meta {
        let data = decode_udt(bytes, udt_meta.descriptions.len())?;
        udt_meta
            .descriptions
            .iter()
            .zip(data.iter())
            .map(|((name, t), x)| decode_value(t, x).map(|v| (name.as_plain(), v)))
            .collect()
    } else {
        Err(CDRSError::General("Error converting UDT".into()))
    }
}

pub fn decode_value(col_type: &ColTypeOption, data: &CBytes) -> CDRSResult<ColValue> {
    if let Some(ref bytes) = data.as_plain() {
        let value = match &col_type.id {
            // strings
            ColType::Varchar => ColValue::String(decode_varchar(bytes)?),
            ColType::Ascii => ColValue::String(decode_ascii(bytes)?),
            ColType::Custom => ColValue::String(decode_custom(bytes)?),
            // integers
            ColType::Tinyint => ColValue::Int(decode_tinyint(bytes)? as i64),
            ColType::Smallint => ColValue::Int(decode_smallint(bytes)? as i64),
            ColType::Int => ColValue::Int(decode_int(bytes)? as i64),
            ColType::Bigint => ColValue::Int(decode_bigint(bytes)?),
            ColType::Varint => ColValue::Int(decode_varint(bytes)?),
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
            // List / Set
            ColType::List => ColValue::Seq(to_seq(&col_type.value, &decode_list(bytes)?)?),
            ColType::Set => ColValue::Seq(to_seq(&col_type.value, &decode_set(bytes)?)?),
            // Tuple
            ColType::Tuple => ColValue::Seq(to_tuple(&col_type.value, bytes)?),
            // UDT
            ColType::Udt => ColValue::Map(to_udt(&col_type.value, bytes)?),
            // null
            ColType::Null => ColValue::Null,
            //TODO Implement other types: Blob, Udt etc
            ColType::Decimal | ColType::Blob | ColType::Map => {
                ColValue::String(String::from("__UNSUPPORTED TYPE__"))
            }
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
