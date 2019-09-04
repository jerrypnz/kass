use crate::errors::{AppError, AppResult};

use cdrs::types::value::Value;
use core::ops::Range;
use itertools::Itertools;
use std::iter::Iterator;

enum QueryValues<'a> {
    IntRange(Range<i32>),
    Strings(Vec<&'a str>),
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

pub fn parse_args<'a>(args: impl Iterator<Item=&'a str>) -> AppResult<Vec<Values>> {
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

}
