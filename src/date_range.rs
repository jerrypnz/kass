// Refer to https://github.com/kosta/date-iterator/blob/master/src/calendar_duration.rs#L144
use crate::errors::{AppError, AppResult};

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use std::cmp::min;

pub const DATE_FORMAT: &str = "%Y-%m-%d";
pub const DATE_TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";

#[derive(Debug, PartialEq)]
pub enum DateTimeRange {
    FixedStep(FixedInterval),
    MonthlyStep(MonthlyInterval),
}

impl DateTimeRange {
    fn new_date_time_range(
        start: NaiveDateTime,
        end: NaiveDateTime,
        step: &str,
        unit: &str,
    ) -> AppResult<DateTimeRange> {
        let step_n: u32 = step.parse()?;

        let range = if unit == "m" {
            let current_date = Some(start.date());
            let time_of_day = start.time();
            DateTimeRange::MonthlyStep(MonthlyInterval {
                current_date,
                time_of_day,
                end,
                months: step_n,
            })
        } else {
            let step = match unit {
                "S" => Duration::seconds(step_n as i64),
                "M" => Duration::minutes(step_n as i64),
                "H" => Duration::hours(step_n as i64),
                "d" => Duration::days(step_n as i64),
                "w" => Duration::weeks(step_n as i64),
                _ => return Err(AppError::new("Invalid step unit")),
            };
            DateTimeRange::FixedStep(FixedInterval { start, end, step })
        };

        Ok(range)
    }

    pub fn parse_date_strs(
        start: &str,
        end: &str,
        step: &str,
        unit: &str,
    ) -> AppResult<DateTimeRange> {
        let start = NaiveDate::parse_from_str(start, DATE_FORMAT)?.and_hms(0, 0, 0);
        let end = NaiveDate::parse_from_str(end, DATE_FORMAT)?.and_hms(0, 0, 0);
        DateTimeRange::new_date_time_range(start, end, step, unit)
    }

    pub fn parse_date_time_strs(
        start: &str,
        end: &str,
        step: &str,
        unit: &str,
    ) -> AppResult<DateTimeRange> {
        let start = NaiveDateTime::parse_from_str(start, DATE_TIME_FORMAT)?;
        let end = NaiveDateTime::parse_from_str(end, DATE_TIME_FORMAT)?;
        DateTimeRange::new_date_time_range(start, end, step, unit)
    }
}

impl Iterator for DateTimeRange {
    type Item = NaiveDateTime;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DateTimeRange::FixedStep(x) => x.next(),
            DateTimeRange::MonthlyStep(x) => x.next(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct FixedInterval {
    start: NaiveDateTime,
    end: NaiveDateTime,
    step: Duration,
}

impl FixedInterval {
    fn next(&mut self) -> Option<NaiveDateTime> {
        if self.start >= self.end {
            None
        } else {
            let current = self.start;
            self.start += self.step;
            Some(current)
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct MonthlyInterval {
    current_date: Option<NaiveDate>,
    time_of_day: NaiveTime,
    end: NaiveDateTime,
    months: u32,
}

impl MonthlyInterval {
    fn next(&mut self) -> Option<NaiveDateTime> {
        if let Some(current_date) = self.current_date {
            let current = NaiveDateTime::new(current_date, self.time_of_day);
            if current >= self.end {
                None
            } else {
                self.current_date = add_months_naive_date(current_date, self.months);
                Some(current)
            }
        } else {
            None
        }
    }
}

fn last_day_of_month_0(year: i32, month_0: u32) -> u32 {
    last_day_of_month(year, month_0 + 1)
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    NaiveDate::from_ymd_opt(year, month + 1, 1)
        .unwrap_or_else(|| NaiveDate::from_ymd(year + 1, 1, 1))
        .pred()
        .day()
}

fn add_months_naive_date(date: NaiveDate, months: u32) -> Option<NaiveDate> {
    let next_month_0 = (date.month0() as i64).checked_add(months as i64)?;
    let additional_years = next_month_0 / 12;
    let next_month_0 = (next_month_0 % 12) as u32;
    let additional_years = if additional_years >= (i32::max_value() as i64) {
        return None;
    } else {
        additional_years as i32
    };
    let next_year = (date.year().checked_add(additional_years))?;
    let next_day = min(date.day(), last_day_of_month_0(next_year, next_month_0));
    NaiveDate::from_ymd_opt(next_year, next_month_0 + 1, next_day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    pub fn date_time(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> NaiveDateTime {
        NaiveDate::from_ymd(y, m, d).and_hms(hh, mm, ss)
    }

    #[test]
    pub fn test_fixed_interval_range_weeks() {
        assert_eq!(
            vec![
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 9, 15, 10, 32, 20),
                date_time(2019, 9, 29, 10, 32, 20),
                date_time(2019, 10, 13, 10, 32, 20),
            ],
            DateTimeRange::parse_date_time_strs(
                "2019-09-01T10:32:20",
                "2019-10-15T10:32:20",
                "2",
                "w",
            )
            .unwrap()
            .collect::<Vec<NaiveDateTime>>()
        )
    }

    #[test]
    pub fn test_fixed_interval_range_days() {
        assert_eq!(
            vec![
                date_time(2019, 9, 1, 0, 0, 0),
                date_time(2019, 9, 2, 0, 0, 0),
                date_time(2019, 9, 3, 0, 0, 0),
                date_time(2019, 9, 4, 0, 0, 0),
            ],
            DateTimeRange::parse_date_strs("2019-09-01", "2019-09-05", "1", "d",)
                .unwrap()
                .collect::<Vec<NaiveDateTime>>()
        )
    }

    #[test]
    pub fn test_fixed_interval_range_hours() {
        assert_eq!(
            vec![
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 9, 1, 16, 32, 20),
                date_time(2019, 9, 1, 22, 32, 20),
                date_time(2019, 9, 2, 4, 32, 20),
            ],
            DateTimeRange::parse_date_time_strs(
                "2019-09-01T10:32:20",
                "2019-09-02T10:31:20",
                "6",
                "H",
            )
            .unwrap()
            .collect::<Vec<NaiveDateTime>>()
        )
    }

    #[test]
    pub fn test_fixed_interval_range_minutes() {
        assert_eq!(
            vec![
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 9, 1, 10, 37, 20),
                date_time(2019, 9, 1, 10, 42, 20),
                date_time(2019, 9, 1, 10, 47, 20),
                date_time(2019, 9, 1, 10, 52, 20),
                date_time(2019, 9, 1, 10, 57, 20),
            ],
            DateTimeRange::parse_date_time_strs(
                "2019-09-01T10:32:20",
                "2019-09-01T11:00:10",
                "5",
                "M",
            )
            .unwrap()
            .collect::<Vec<NaiveDateTime>>()
        )
    }

    #[test]
    pub fn test_fixed_interval_range_edge_cases() {
        assert_eq!(
            None,
            DateTimeRange::parse_date_time_strs(
                "2019-09-01T10:32:20",
                "2019-09-01T10:32:20",
                "1",
                "H",
            )
            .unwrap()
            .next()
        );

        assert_eq!(
            None,
            DateTimeRange::parse_date_time_strs(
                "2019-09-01T10:32:20",
                "2019-09-01T10:30:20",
                "1",
                "H",
            )
            .unwrap()
            .next()
        )
    }

    #[test]
    pub fn test_monthly_interval_range() {
        assert_eq!(
            vec![
                date_time(2019, 9, 2, 10, 32, 20),
                date_time(2019, 10, 2, 10, 32, 20),
                date_time(2019, 11, 2, 10, 32, 20),
                date_time(2019, 12, 2, 10, 32, 20),
                date_time(2020, 1, 2, 10, 32, 20),
                date_time(2020, 2, 2, 10, 32, 20),
            ],
            DateTimeRange::parse_date_time_strs(
                "2019-09-02T10:32:20",
                "2020-02-03T09:00:10",
                "1",
                "m",
            )
            .unwrap()
            .collect::<Vec<NaiveDateTime>>()
        )
    }
}
