extern crate cdrs;

use cdrs::error::{Error as CDRSError};
use std::convert::From;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::num::ParseIntError;
use std::result::Result;

#[derive(Debug)]
pub enum AppError {
    InvalidRange(String),
    InvalidInt(ParseIntError),
    QueryFailure(CDRSError),
}

pub type AppResult<T> = Result<T, AppError>;

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match *self {
            AppError::InvalidRange(ref msg) => write!(f, "Invalid range: {}", msg),
            AppError::InvalidInt(ref err) => write!(f, "Invalid integer: {}", err),
            AppError::QueryFailure(ref err) => write!(f, "Query failed: {}", err),
        }
    }
}

impl Error for AppError {
    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            AppError::InvalidInt(ref err) => Some(err),
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
