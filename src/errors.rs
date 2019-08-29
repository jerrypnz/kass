extern crate cdrs;

use cdrs::error::Error as CDRSError;
use serde_json::Error as JsonError;
use std::convert::From;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::num::ParseIntError;
use std::result::Result;

#[derive(Debug)]
pub enum AppError {
    General(String),
    InvalidInt(ParseIntError),
    QueryFailure(CDRSError),
    JsonError(JsonError),
}

pub type AppResult<T> = Result<T, AppError>;

// impl AppError {
//     pub fn general(msg: &str) -> AppError {
//         AppError::General(String::from(msg))
//     }
// }

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match *self {
            AppError::InvalidInt(ref err) => write!(f, "Invalid integer: {}", err),
            AppError::QueryFailure(ref err) => write!(f, "Query failed: {}", err),
            AppError::JsonError(ref err) => write!(f, "JSON error: {}", err),
            AppError::General(ref msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl Error for AppError {
    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            AppError::InvalidInt(ref err) => Some(err),
            AppError::QueryFailure(ref err) => Some(err),
            AppError::JsonError(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<ParseIntError> for AppError {
    fn from(err: ParseIntError) -> Self {
        AppError::InvalidInt(err)
    }
}

impl From<CDRSError> for AppError {
    fn from(err: CDRSError) -> Self {
        AppError::QueryFailure(err)
    }
}

impl From<JsonError> for AppError {
    fn from(err: JsonError) -> Self {
        AppError::JsonError(err)
    }
}
