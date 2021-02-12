use std::convert::TryFrom;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub struct DirFsync {
    dir_fd: File,
}

impl DirFsync {
    pub fn new(dir: &Path) -> io::Result<Self> {
        Ok(Self {
            dir_fd: OpenOptions::new().read(true).open(dir)?,
        })
    }

    pub fn fsync(&self) -> io::Result<()> {
        self.dir_fd.sync_all()?;
        Ok(())
    }
}

pub fn pread_exact_or_eof(file: &File, mut buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let mut total = 0_usize;
    while !buf.is_empty() {
        match file.read_at(buf, offset + u64::try_from(total).unwrap()) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

pub fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    file.read_exact_at(buf, offset)
}

pub fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    file.write_all_at(buf, offset)
}
