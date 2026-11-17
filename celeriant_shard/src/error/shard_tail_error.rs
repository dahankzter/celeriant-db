use celeriant_rotating_log::errors::scan_error::ScanError;
use celeriant_wire::disk::disk_format_error::DiskFormatError;

use crate::error::fetch_datablock_error::FetchDatablockError;

#[derive(Debug, Clone)]
pub enum ShardTailError {
    ReadFromDisk(ScanError<DiskFormatError>),
    FetchDatablock(FetchDatablockError),
    /// The cursor is past this node's read cursor. Retryable while the node lags or
    /// after a failover. If it persists, or `tip_wal_seq` is far below the cursor, the
    /// log the consumer followed is gone (wiped, restored, another cluster, or a
    /// diverged node truncated) and the consumer must rebuild.
    TailCursorAhead { tip_wal_seq: u64 },
    SemaphoreClosed,
}

impl ShardTailError {
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::ReadFromDisk(_) => "read_from_disk_error",
            Self::FetchDatablock(_) => "fetch_datablocks_error",
            Self::TailCursorAhead { .. } => "tail_cursor_ahead",
            Self::SemaphoreClosed => "list_semaphore_closed",
        }
    }
}

impl From<ScanError<DiskFormatError>> for ShardTailError {
    fn from(e: ScanError<DiskFormatError>) -> Self {
        Self::ReadFromDisk(e)
    }
}

impl From<FetchDatablockError> for ShardTailError {
    fn from(e: FetchDatablockError) -> Self {
        Self::FetchDatablock(e)
    }
}
