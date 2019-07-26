use crate::errors::{AppError, AppResult};

use cdrs::types::value::Value;
use core::ops::Range;
use itertools::Itertools;
use std::iter::Iterator;

type Values = Vec<Value>;

trait GenQueryValues {
    fn get_values(self) -> Values;
}

impl GenQueryValues for Range<i32> {
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

pub fn parse_args(args: Vec<&str>) -> AppResult<Vec<Values>> {
    let results: Vec<Values> = args
        .iter()
        .map(|arg| parse_int_range(arg).map(|x| x.get_values()))
        .collect::<AppResult<Vec<Values>>>()?;

    let all_args = results.into_iter().multi_cartesian_product().collect();
    Ok(all_args)
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
            assert_matches!(parse_int_range(s), Err(AppError::General(_)))
        }
    }

}
