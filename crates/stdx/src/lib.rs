pub mod cow_arc;
pub mod crc32_io;
pub mod varint_io;
pub mod binary_search;
pub mod bound;

// TODO: support miri

// Ported from sled.
#[cfg(all(unix, not(miri)))]
mod io_ext_unix;
#[cfg(all(windows, not(miri)))]
mod io_ext_windows;

pub mod io_ext {
    #[cfg(all(unix, not(miri)))]
    pub use crate::io_ext_unix::*;
    #[cfg(all(windows, not(miri)))]
    pub use crate::io_ext_windows::*;
}