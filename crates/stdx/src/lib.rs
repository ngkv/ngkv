pub mod cow_arc;
pub mod crc32_io;
pub mod varint_io;
pub mod binary_search;
pub mod bound;

// TODO: support miri

// Ported from sled.
#[cfg(all(unix, not(miri)))]
mod parallel_io_unix;
#[cfg(all(windows, not(miri)))]
mod parallel_io_windows;

pub mod parallel_io {
    #[cfg(all(unix, not(miri)))]
    pub use crate::parallel_io_unix::*;
    #[cfg(all(windows, not(miri)))]
    pub use crate::parallel_io_windows::*;
}