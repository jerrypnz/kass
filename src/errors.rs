extern crate cdrs;

use cdrs::error::Error as CDRSError;
use serde_json::Error as JsonError;
use std::convert::From;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::num::ParseIntError;
use std::result::Result;
use chrono::ParseError as DateTimeParseError;

#[derive(Debug, Clone)]
pub struct AppError(String);

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn general<T: Into<String>>(msg: T) -> AppError {
        AppError(msg.into())
    }
}

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        self.0.fmt(f)
    }
}

impl Error for AppError {}

impl From<ParseIntError> for AppError {
    fn from(_: ParseIntError) -> Self {
        AppError(String::from("Error parsing integer"))
    }
}

impl From<CDRSError> for AppError {
    fn from(err: CDRSError) -> Self {
        AppError(format!("Error in Cassandra driver: {}", err))
    }
}

impl From<JsonError> for AppError {
    fn from(err: JsonError) -> Self {
        AppError(format!("Error generating JSON results: {}", err))
    }
}

impl From<DateTimeParseError> for AppError {
    fn from(err: DateTimeParseError) -> Self {
        AppError(format!("Error parsing date time: {}", err))
    }
}
