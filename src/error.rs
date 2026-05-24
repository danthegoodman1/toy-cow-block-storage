use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    InvalidArgument { reason: String },
    NotFound { kind: &'static str, id: String },
    Conflict { reason: String },
    Unavailable { reason: String },
    Corrupt { reason: String },
    Unsupported { reason: String },
}

impl StorageError {
    pub fn invalid_argument(reason: impl Into<String>) -> Self {
        Self::InvalidArgument {
            reason: reason.into(),
        }
    }

    pub fn not_found(kind: &'static str, id: impl Into<String>) -> Self {
        Self::NotFound {
            kind,
            id: id.into(),
        }
    }

    pub fn conflict(reason: impl Into<String>) -> Self {
        Self::Conflict {
            reason: reason.into(),
        }
    }

    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable {
            reason: reason.into(),
        }
    }

    pub fn corrupt(reason: impl Into<String>) -> Self {
        Self::Corrupt {
            reason: reason.into(),
        }
    }

    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::Unsupported {
            reason: reason.into(),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument { reason } => write!(f, "invalid argument: {reason}"),
            Self::NotFound { kind, id } => write!(f, "{kind} not found: {id}"),
            Self::Conflict { reason } => write!(f, "conflict: {reason}"),
            Self::Unavailable { reason } => write!(f, "unavailable: {reason}"),
            Self::Corrupt { reason } => write!(f, "corrupt: {reason}"),
            Self::Unsupported { reason } => write!(f, "unsupported: {reason}"),
        }
    }
}

impl Error for StorageError {}
