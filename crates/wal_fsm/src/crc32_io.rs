use std::io::{Read, Write};

pub struct Crc32Read<R> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R: Read> Crc32Read<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: crc32fast::Hasher::new(),
        }
    }

    pub fn finalize(self) -> u32 {
        self.hasher.finalize()
    }
}

impl<R: Read> Read for Crc32Read<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[0..n]);
        Ok(n)
    }
}

pub struct Crc32Write<W> {
    inner: W,
    hasher: crc32fast::Hasher,
}

impl<W: Write> Crc32Write<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: crc32fast::Hasher::new(),
        }
    }

    pub fn finalize(self) -> u32 {
        self.hasher.finalize()
    }
}

impl<W: Write> Write for Crc32Write<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[0..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
