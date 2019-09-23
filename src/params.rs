use crate::errors::{AppError, AppResult};

use cdrs::types::value::Value;
use core::ops::Range;
use itertools::Itertools;
use regex::Regex;
use std::iter::Iterator;

enum QueryValues<'a> {
    IntRange(Range<i32>),
    Strings(Vec<&'a str>),
}

lazy_static! {
    static ref INT_RANGE: Regex = Regex::new(r"^(\d+)\.\.(\d+)(?:/(\d+)(?:/(int|smallint|tinyint|bigint))?)?$").unwrap();
    static ref DATE_RANGE: Regex = Regex::new(r"^(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})(?:/(\d+)([mdw])(?:/([a-zA-Z%\-/]+))?)?$").unwrap();
    static ref DATE_TIME_RANGE: Regex = Regex::new(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.\.(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:/(\d+)([mdwHMS])(?:/([a-zA-Z%\-/:]+))?)?$").unwrap();
    //static ref COMMA_SEPARATED: Regex = Regex::new(r#"(?:^|,)(?=[^"]|(")?)"?((?(1)[^"]*|[^,"]*))"?(?=,|$)"#).unwrap();
}

pub type Values = Vec<Value>;

trait GenQueryValues {
    fn get_values(self) -> Values;
}

impl<T: Into<Value>, L: Iterator<Item = T>> GenQueryValues for L {
    fn get_values(self) -> Values {
        self.map(|x| x.into()).collect()
    }
}

fn parse_int_range(s: &str) -> AppResult<Range<i32>> {
    let from_to: Vec<&str> = s.split('-').collect();
    if from_to.len() != 2 {
        Err(AppError::General(format!("invalid range {}", s)))
    } else {
        let from = from_to[0].parse::<i32>()?;
        let to = from_to[1].parse::<i32>()?;
        if from >= to {
            Err(AppError::General(format!(
                "range start {} is greater/equal to range end {}",
                from, to
            )))
        } else {
            Ok(from..to)
        }
    }
}

fn parse_strings<'a>(s: &'a str) -> Vec<&'a str> {
    s.split(',').collect()
}

fn parse_query_values<'a>(s: &'a str) -> QueryValues<'a> {
    match parse_int_range(s).map(QueryValues::IntRange) {
        Ok(x) => x,
        Err(_) => QueryValues::Strings(parse_strings(s)),
    }
}

fn to_cdrs_values(vals: QueryValues) -> Values {
    match vals {
        QueryValues::IntRange(r) => r.get_values(),
        QueryValues::Strings(xs) => xs.into_iter().get_values(),
    }
}

pub fn parse_args<'a>(args: impl Iterator<Item = &'a str>) -> AppResult<Vec<Values>> {
    let results: Vec<Values> = args
        .map(|arg| to_cdrs_values(parse_query_values(arg)))
        .collect();

    Ok(results.into_iter().multi_cartesian_product().collect())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_int_range_valid_ranges() {
        assert_eq!(parse_int_range("1-10").unwrap(), 1..10);
        assert_eq!(parse_int_range("1-2").unwrap(), 1..2);
    }

    #[test]
    fn test_parse_int_range_invalid_ranges() {
        let ss = ["abc", "100"];
        for s in ss.iter() {
            if let Err(AppError::General(msg)) = parse_int_range(s) {
                assert_eq!(msg, format!("invalid range {}", s));
            } else {
                panic!("didn't get expected error")
            }
        }
    }

    fn capture_groups(re: &Regex, s: &'static str) -> Option<Vec<&'static str>> {
        re.captures(s).map(|x| {
            x.iter()
                .skip(1)
                .map(|y| y.map(|z| z.as_str()).unwrap_or(""))
                .collect()
        })
    }

    #[test]
    fn test_int_range_regex() {
        assert_eq!(
            Some(vec!["1", "10", "2", "tinyint"]),
            capture_groups(&INT_RANGE, "1..10/2/tinyint")
        );
        assert_eq!(
            Some(vec!["1", "10", "", ""]),
            capture_groups(&INT_RANGE, "1..10")
        );
        assert_eq!(
            Some(vec!["1", "10", "3", ""]),
            capture_groups(&INT_RANGE, "1..10/3")
        );
        assert_eq!(None, capture_groups(&INT_RANGE, "1..10/int"));
    }

    #[test]
    fn test_date_range_regex() {
        assert_eq!(
            Some(vec!["2019-09-01", "2019-12-01", "", "", ""]),
            capture_groups(&DATE_RANGE, "2019-09-01..2019-12-01")
        );
        assert_eq!(
            Some(vec!["2019-09-01", "2019-12-01", "2", "w", ""]),
            capture_groups(&DATE_RANGE, "2019-09-01..2019-12-01/2w")
        );
        assert_eq!(
            Some(vec!["2019-09-01", "2019-12-01", "10", "d", ""]),
            capture_groups(&DATE_RANGE, "2019-09-01..2019-12-01/10d")
        );
        assert_eq!(
            Some(vec!["2019-09-01", "2019-12-01", "1", "m", ""]),
            capture_groups(&DATE_RANGE, "2019-09-01..2019-12-01/1m")
        );
        assert_eq!(
            Some(vec!["2019-09-01", "2019-12-01", "2", "w", "%Y%m%d"]),
            capture_groups(&DATE_RANGE, "2019-09-01..2019-12-01/2w/%Y%m%d")
        );
        assert_eq!(
            Some(vec!["2019-09-01", "2019-12-01", "2", "w", "%d/%m/%Y"]),
            capture_groups(&DATE_RANGE, "2019-09-01..2019-12-01/2w/%d/%m/%Y")
        );
    }

    #[test]
    fn test_date_time_range_regex() {
        assert_eq!(
            Some(vec![
                "2019-09-01T10:13:12",
                "2019-12-01T14:35:22",
                "",
                "",
                ""
            ]),
            capture_groups(&DATE_TIME_RANGE, "2019-09-01T10:13:12..2019-12-01T14:35:22")
        );
        assert_eq!(
            Some(vec![
                "2019-09-01T10:13:12",
                "2019-12-01T14:35:22",
                "5",
                "H",
                ""
            ]),
            capture_groups(
                &DATE_TIME_RANGE,
                "2019-09-01T10:13:12..2019-12-01T14:35:22/5H"
            )
        );
        assert_eq!(
            Some(vec![
                "2019-09-01T10:13:12",
                "2019-12-01T14:35:22",
                "5",
                "M",
                ""
            ]),
            capture_groups(
                &DATE_TIME_RANGE,
                "2019-09-01T10:13:12..2019-12-01T14:35:22/5M"
            )
        );
        assert_eq!(
            Some(vec![
                "2019-09-01T10:13:12",
                "2019-12-01T14:35:22",
                "30",
                "S",
                ""
            ]),
            capture_groups(
                &DATE_TIME_RANGE,
                "2019-09-01T10:13:12..2019-12-01T14:35:22/30S"
            )
        );
        assert_eq!(
            Some(vec![
                "2019-09-01T10:13:12",
                "2019-12-01T14:35:22",
                "30",
                "S",
                "%Y%m%d%H%M"
            ]),
            capture_groups(
                &DATE_TIME_RANGE,
                "2019-09-01T10:13:12..2019-12-01T14:35:22/30S/%Y%m%d%H%M"
            )
        );
        assert_eq!(
            Some(vec![
                "2019-09-01T10:13:12",
                "2019-12-01T14:35:22",
                "30",
                "S",
                "%H:%M:%S-%d/%d/%Y"
            ]),
            capture_groups(
                &DATE_TIME_RANGE,
                "2019-09-01T10:13:12..2019-12-01T14:35:22/30S/%H:%M:%S-%d/%d/%Y"
            )
        );
    }

    // #[test]
    // fn test_comma_separated_values() {
    //     let test_list = r#"a,,b,c,123,"hello, world",foo:123"#;
    //     let items: Vec<&'static str> = COMMA_SEPARATED
    //         .captures_iter(test_list)
    //         .map(|x| x.get(3).unwrap().as_str())
    //         .collect();
    //     assert_eq!(
    //         vec!["a", "", "b", "c", "123", "hello, world", "foo:123"],
    //         items
    //     );
    // }

}
