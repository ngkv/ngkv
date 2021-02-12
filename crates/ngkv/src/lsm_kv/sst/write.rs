use std::{
    cmp::min,
    fs,
    io::{self, BufWriter, Write},
    ops::Deref,
    sync::Arc,
};

use crate::{
    lsm_kv::{
        serialization,
        sst::{
            sst_path, BlockHandle, DataBlockFooter, FilterBlock, Footer, IndexBlock, InternalKey,
            ShortInternalKey, SstRecord, StatBlock,
        },
        BloomFilterBuilder, ByteCountedWrite, CompressionType, Options,
    },
    Result,
};

use byteorder::{LittleEndian, WriteBytesExt};
use serialization::FixedSizeSerializable;
use stdx::crc32_io::Crc32Write;

fn common_prefix_length(v1: &[u8], v2: &[u8]) -> usize {
    let min_len = min(v1.len(), v2.len());
    for i in 0..min_len {
        if v1[i] != v2[i] {
            return i;
        }
    }
    return min_len;
}

struct DataBlockBuildState {
    records_since_restart: u32,
    bytes_written: u32,
    bytes_offset: u64,
    filter_builder: BloomFilterBuilder,

    /// Offset of restart points, relative to the beginning of data block.
    restart_array: Vec<u32>,
}

struct FileState {
    fd: fs::File,
    writer: BufWriter<fs::File>,
    bytes_written: u64,
}

pub struct SstWriter {
    opt: Arc<Options>,
    index_block: IndexBlock,
    filter_block: FilterBlock,
    cur_data_block: Option<DataBlockBuildState>,
    prev_key: Option<InternalKey<'static>>,
    file: FileState,
    internal_key_buf: Vec<u8>,
}

impl BlockHandle {
    fn from_written(w: Written) -> Self {
        Self {
            offset: w.offset,
            size: w.size,
        }
    }
}

struct Written {
    size: u64,
    offset: u64,
}

impl SstWriter {
    pub fn new(opt: Arc<Options>, id: u32) -> Result<Self> {
        let path = sst_path(&opt.dir, id);
        let file = fs::OpenOptions::new().create(true).write(true).open(path)?;
        Ok(Self {
            opt,
            file: FileState {
                fd: file.try_clone()?,
                writer: BufWriter::new(file),
                bytes_written: 0,
            },
            cur_data_block: None,
            index_block: Default::default(),
            filter_block: Default::default(),
            // prev_data_block_end_key: None,
            prev_key: None,
            internal_key_buf: vec![],
        })
    }

    #[inline(always)]
    fn with_writer_checksum<F>(file: &mut FileState, crc: u32, f: F) -> Result<(u32, Written)>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        let mut new_crc = 0;
        let handle = Self::with_writer(file, |w| {
            let mut crc_writer = Crc32Write::new_with_initial(w, crc);
            f(&mut crc_writer)?;
            new_crc = crc_writer.finalize();
            Ok(())
        })?;
        Ok((new_crc, handle))
    }

    #[inline(always)]
    fn with_writer<F>(file: &mut FileState, f: F) -> Result<Written>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        let offset = file.bytes_written;
        let mut byte_counter = ByteCountedWrite::new(&mut file.writer);
        f(&mut byte_counter)?;
        let size = byte_counter.bytes_written() as u64;
        file.bytes_written += size;

        Ok(Written { offset, size })
    }

    /// Finish a data block when there exists an active data block, and its size
    /// is larger or equal to `DATA_BLOCK_SIZE`. However, if `force` is set, the
    /// size constraint is ignored.
    fn maybe_finish_data_block(&mut self, force: bool) -> Result<()> {
        if let Some(cur_data_block) = &self.cur_data_block {
            if force || cur_data_block.bytes_written >= self.opt.data_block_size {
                let mut cur_data_block = self.cur_data_block.take().unwrap();

                // Write data block restart array & footer.
                let written = Self::with_writer(&mut self.file, {
                    // TODO: support compression.
                    // TODO: support checksum.
                    let compression = self.opt.compression;
                    assert!(compression == CompressionType::NoCompression);

                    let restart_array_offset = cur_data_block.bytes_written;
                    let restart_array = &cur_data_block.restart_array;

                    move |w| {
                        for &p in restart_array {
                            w.write_u32::<LittleEndian>(p)?;
                        }
                        let footer = DataBlockFooter {
                            compression,
                            restart_array_offset,
                        };
                        footer.serialize_into(&mut *w)?;
                        Ok(())
                    }
                })?;

                assert!(written.size <= u32::MAX as u64);
                cur_data_block.bytes_written += written.size as u32;

                let handle = BlockHandle {
                    offset: cur_data_block.bytes_offset,
                    size: cur_data_block.bytes_written as u64,
                };

                // NOTE: index_block.start_keys is pushed earlier (at the time
                // the data block is started in push()).
                self.index_block.data_handles.push(handle);

                self.filter_block
                    .blooms
                    .push(cur_data_block.filter_builder.build());
            }
        }

        Ok(())
    }

    // Start a data block if not present.
    fn maybe_start_data_block(&mut self, rec: &SstRecord) -> bool {
        if self.cur_data_block.is_none() {
            // If there exists a previous data block, we shorten (i.e. removing
            // suffix) the start key of current data block, making it only
            // sufficient to perform indexing.
            let short_key = ShortInternalKey::new(&rec.internal_key, self.prev_key.as_ref());

            self.index_block.start_keys.push(short_key);

            self.cur_data_block = Some(DataBlockBuildState {
                bytes_offset: self.file.bytes_written,
                bytes_written: 0,
                restart_array: vec![],
                records_since_restart: 0,
                filter_builder: BloomFilterBuilder::new(self.opt.bloom_bits_per_key),
            });

            true
        } else {
            false
        }
    }

    /// Push a record into SST. Records must be pushed in order.
    pub fn push(&mut self, rec: &SstRecord) -> Result<()> {
        // Check record order.
        if let Some(prev_key) = &self.prev_key {
            assert!(&rec.internal_key > prev_key, "record out of order");
        }

        let mut restart = false;

        if self.maybe_start_data_block(rec) {
            restart = true;
        }

        let cur_data_block = self.cur_data_block.as_mut().unwrap();

        // Add key to bloom filter.
        cur_data_block
            .filter_builder
            .add_key(rec.internal_key.key());

        if cur_data_block.records_since_restart >= self.opt.data_block_restart_interval {
            restart = true;
        }

        let ikey_buf = rec.internal_key.buf();
        let ikey_shared_len: u32;
        let ikey_delta: &[u8];

        // Perform common prefix removal.
        if restart {
            cur_data_block.records_since_restart = 0;
            // TODO: insert restart point to block footer
            ikey_shared_len = 0;
            ikey_delta = ikey_buf;
        } else {
            cur_data_block.records_since_restart += 1;
            let prev_key = self.prev_key.as_ref().unwrap();
            let prefix_len = common_prefix_length(&rec.internal_key.buf(), prev_key.buf());
            ikey_shared_len = prefix_len as u32;
            ikey_delta = &ikey_buf[prefix_len..];
        }

        // Update prev_key.
        if let Some(prev_key) = &mut self.prev_key {
            prev_key.apply_delta(ikey_shared_len as u32, ikey_delta);
        } else {
            self.prev_key = Some(rec.internal_key.clone());
        }

        // Write current record.
        let handle = Self::with_writer(&mut self.file, {
            let mut ikey_delta = ikey_delta;
            let ikey_delta_len: u32 = ikey_delta.len() as u32;
            let mut value = rec.value.deref();
            let value_len: u32 = value.len() as u32;

            move |w| {
                serialization::serialize_into(&mut *w, &ikey_shared_len)?;
                serialization::serialize_into(&mut *w, &ikey_delta_len)?;
                serialization::serialize_into(&mut *w, &value_len)?;
                io::copy(&mut ikey_delta, &mut *w)?;
                io::copy(&mut value, &mut *w)?;
                Ok(())
            }
        })?;

        cur_data_block.bytes_written = cur_data_block
            .bytes_written
            .checked_add(handle.size as u32)
            .expect("data block too large");

        self.maybe_finish_data_block(false)?;
        Ok(())
    }

    /// Write the SST onto disk. When the function returns, file content is
    /// fully persisted on disk. However, the directory holding the file may not
    /// be persisted yet.
    pub fn write(mut self) -> Result<()> {
        self.maybe_finish_data_block(true)?;

        let filter_handle = BlockHandle::from_written(Self::with_writer(&mut self.file, {
            let blk = &self.filter_block;
            move |w| serialization::serialize_into(w, blk)
        })?);

        let index_handle = BlockHandle::from_written(Self::with_writer(&mut self.file, {
            let blk = &self.index_block;
            move |w| serialization::serialize_into(w, blk)
        })?);

        let stat_handle = BlockHandle::from_written({
            // TODO: stat
            let stat = StatBlock {};
            Self::with_writer(&mut self.file, |w| serialization::serialize_into(w, &stat))?
        });

        // Write footer.
        Self::with_writer(&mut self.file, |w| {
            Footer {
                filter_handle,
                index_handle,
                stat_handle,
            }
            .serialize_into(w)
        })?;

        self.file.writer.flush()?;
        self.file.fd.sync_all()?;

        Ok(())
    }
}
