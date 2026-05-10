use thiserror::Error;

pub type Result<T> = std::result::Result<T, CloudError>;

#[derive(Debug, Error)]
pub enum CloudError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
}
