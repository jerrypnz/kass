// Refer to https://github.com/kosta/date-iterator/blob/master/src/calendar_duration.rs#L144

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use std::cmp::min;

struct FixedInterval {
    start: NaiveDateTime,
    end: NaiveDateTime,
    step: Duration,
}

impl FixedInterval {
    pub fn new(start: NaiveDateTime, end: NaiveDateTime, step: Duration) -> Self {
        FixedInterval { start, end, step }
    }
}

impl Iterator for FixedInterval {
    type Item = NaiveDateTime;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            None
        } else {
            let current = self.start.clone();
            self.start += self.step;
            Some(current)
        }
    }
}

struct MonthlyInterval {
    current_date: Option<NaiveDate>,
    time_of_day: NaiveTime,
    end: NaiveDateTime,
    months: u32,
}

impl MonthlyInterval {
    pub fn new(start: NaiveDateTime, end: NaiveDateTime, months: u32) -> Self {
        let current_date = Some(start.date());
        let time_of_day = start.time();
        MonthlyInterval {
            current_date,
            time_of_day,
            end,
            months,
        }
    }
}

impl Iterator for MonthlyInterval {
    type Item = NaiveDateTime;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current_date) = self.current_date {
            let current = NaiveDateTime::new(current_date, self.time_of_day);
            if current >= self.end {
                None
            } else {
                self.current_date = add_months_naive_date(&current_date, self.months);
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

fn add_months_naive_date(date: &NaiveDate, months: u32) -> Option<NaiveDate> {
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
            FixedInterval::new(
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 10, 15, 10, 32, 20),
                Duration::weeks(2),
            )
            .collect::<Vec<NaiveDateTime>>()
        )
    }

    #[test]
    pub fn test_fixed_interval_range_days() {
        assert_eq!(
            vec![
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 9, 2, 10, 32, 20),
                date_time(2019, 9, 3, 10, 32, 20),
                date_time(2019, 9, 4, 10, 32, 20),
            ],
            FixedInterval::new(
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 9, 5, 10, 32, 20),
                Duration::days(1),
            )
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
            FixedInterval::new(
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 9, 2, 10, 31, 20),
                Duration::hours(6)
            )
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
            FixedInterval::new(
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 9, 1, 11, 00, 10),
                Duration::minutes(5)
            )
            .collect::<Vec<NaiveDateTime>>()
        )
    }

    #[test]
    pub fn test_fixed_interval_range_edge_cases() {
        assert_eq!(
            None,
            FixedInterval::new(
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 9, 1, 10, 32, 20),
                Duration::hours(1),
            )
            .next()
        );

        assert_eq!(
            None,
            FixedInterval::new(
                date_time(2019, 9, 1, 10, 32, 20),
                date_time(2019, 9, 1, 10, 30, 20),
                Duration::hours(1),
            )
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
            MonthlyInterval::new(
                date_time(2019, 9, 2, 10, 32, 20),
                date_time(2020, 2, 3, 9, 0, 10),
                1,
            )
            .collect::<Vec<NaiveDateTime>>()
        )
    }

}
