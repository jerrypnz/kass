use std::convert::{TryInto};
use std::net::IpAddr;

use chrono::{NaiveTime, SecondsFormat, TimeZone, Utc};
use uuid::Uuid;

pub fn to_date_time_str(ts: i64) -> String {
    let ts = Utc.timestamp_millis(ts);
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn to_time_str(t: i64) -> String {
    let secs: u32 = (t / 1000_000_000).try_into().expect("Value out of range");
    let nano: u32 = (t % 1000_000_000).try_into().expect("Value out of range");
    let tm = NaiveTime::from_num_seconds_from_midnight(secs, nano);
    tm.format("%H:%M:%S%.3f").to_string()
}

pub fn to_date_str(d: i32) -> String {
    let ts: i64 = i64::from(d) * 24 * 60 * 60 * 1000;
    let ts = Utc.timestamp_millis(ts);
    ts.date().format("%Y-%m-%d").to_string()
}

pub fn to_ip_str(ip: IpAddr) -> String {
    format!("{}", ip)
}

pub fn to_uuid_str(uuid: Uuid) -> String {
    uuid.to_hyphenated_ref().to_string()
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    pub fn test_to_date_time_str() {
        assert_eq!(
            String::from("1970-01-01T00:00:00.000Z"),
            to_date_time_str(0)
        );
        assert_eq!(
            String::from("2019-08-21T10:32:10.471Z"),
            to_date_time_str(1566383530471)
        );
    }

    #[test]
    pub fn test_to_time_str() {
        assert_eq!(String::from("00:00:00.000"), to_time_str(0));
        assert_eq!(String::from("00:00:00.123"), to_time_str(123_000_000));
        assert_eq!(
            String::from("21:35:59.456"),
            to_time_str(77_759_456_000_000)
        );
    }

    #[test]
    pub fn test_to_date_str() {
        assert_eq!(String::from("1970-01-01"), to_date_str(0));
        assert_eq!(String::from("2018-06-06"), to_date_str(17688));
    }

    #[test]
    pub fn test_to_ip_str() {
        assert_eq!(
            String::from("127.0.0.1"),
            to_ip_str(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))
        );
        assert_eq!(
            String::from("10.67.23.123"),
            to_ip_str(IpAddr::V4(Ipv4Addr::new(10, 67, 23, 123)))
        );
    }
}
