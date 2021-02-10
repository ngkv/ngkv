use std::io::{self, Read, Write};

pub(crate) struct ByteCountedWrite<W> {
    inner: W,
    count: usize,
}

impl<W> ByteCountedWrite<W>
where
    W: Write,
{
    pub fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    pub fn bytes_written(&self) -> usize {
        self.count
    }
}

impl<W> Write for ByteCountedWrite<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = self.inner.write(buf);
        if let Ok(size) = res {
            self.count += size
        }
        res
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) struct ByteCountedRead<R> {
    inner: R,
    count: usize,
}

impl<R> ByteCountedRead<R>
where
    R: Read,
{
    pub fn new(inner: R) -> Self {
        Self { inner, count: 0 }
    }

    pub fn into_inner(self) -> R {
        self.inner
    }

    pub fn bytes_read(&self) -> usize {
        self.count
    }
}

impl<R> Read for ByteCountedRead<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count += n;
        Ok(n)
    }
}
