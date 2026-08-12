use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::Arc,
};

use memmap::MmapOptions;

use crate::segment::{config::SegmentConfig, metadata::SegmentView};
use proto::batch_iter::BatchIter;
use proto::record_batch::{BATCH_HEADER_PREFIX, RecordBatch};

const INDEX_ENTRY_SIZE: usize = 16;

pub struct LogSegment {
    segment: Arc<SegmentView>,

    log_file: File,
    index_file: memmap::MmapMut,
    time_index_file: memmap::MmapMut,

    index_write_pos: usize,

    pub size: usize,
    index_count: usize,

    bytes_since_last_index: usize,
    index_threshold_bytes: usize,
}

impl LogSegment {
    // Love needed.
    pub fn open(opts: SegmentConfig) -> io::Result<Self> {
        let base_path = Path::new(&opts.base_dir);

        fs::create_dir_all(base_path)?;

        let log_path = base_path.join(format!("{:020}.log", opts.base_offset));
        let index_path = base_path.join(format!("{:020}.index", opts.base_offset));
        let time_index_path = base_path.join(format!("{:020}.timeindex", opts.base_offset));

        let exists = log_path.exists();
        let existing_size = if exists {
            let meta = std::fs::metadata(&log_path)?;
            meta.len() as usize
        } else {
            0
        };
        let fresh = !exists || existing_size == 0;

        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(fresh)
            .open(&log_path)?;

        let index_file_handle = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(fresh)
            .open(&index_path)?;

        let time_index_file_handle = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(fresh)
            .open(&time_index_path)?;

        let max_entries = opts.segment_bytes / opts.index_interval_bytes + 1;
        let index_size = max_entries * INDEX_ENTRY_SIZE;

        index_file_handle.set_len(index_size as u64)?;
        time_index_file_handle.set_len(index_size as u64)?;

        let index_file = unsafe {
            MmapOptions::new()
                .len(index_size)
                .map_mut(&index_file_handle)?
        };
        let time_index_file = unsafe {
            MmapOptions::new()
                .len(index_size)
                .map_mut(&time_index_file_handle)?
        };

        let (index_count, size, bytes_since_last_index) = if fresh {
            (0, 0, opts.index_interval_bytes)
        } else {
            let mut count = 0;
            for i in 0..max_entries {
                let base = i * INDEX_ENTRY_SIZE;
                let offset_bytes: [u8; 8] = index_file[base..base + 8].try_into().unwrap();
                let pos_bytes: [u8; 8] = index_file[base + 8..base + 16].try_into().unwrap();
                if i > 0 && offset_bytes == [0; 8] && pos_bytes == [0; 8] {
                    break;
                }
                count += 1;
            }
            (count, existing_size, 0)
        };

        let index_write_pos = index_count * INDEX_ENTRY_SIZE;

        let segment = Arc::new(SegmentView::new(
            opts.base_offset,
            log_file.try_clone()?,
            index_file_handle.try_clone()?,
        ));

        Ok(Self {
            segment,
            log_file,
            index_file,
            time_index_file,
            index_write_pos,
            index_count,
            size,
            bytes_since_last_index,
            index_threshold_bytes: opts.index_interval_bytes,
        })
    }

    pub fn records_count(&self) -> io::Result<u64> {
        let file = Arc::new(self.log_file.try_clone()?);
        let iter = BatchIter { file, pos: 0, end: self.size as u64 };
        let mut total = 0u64;
        for batch in iter {
            let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}")))?;
            total += batch.records_count as u64;
        }
        Ok(total)
    }

    pub fn append_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        let log_pos = self.log_file.metadata()?.len();

        self.log_file.write_all(&batch.encode_header())?;
        self.log_file.write_all(&batch.records)?;

        let batch_on_disk = BATCH_HEADER_PREFIX + batch.batch_length as usize;
        self.bytes_since_last_index += batch_on_disk;

        if self.bytes_since_last_index >= self.index_threshold_bytes
            && self.index_write_pos + INDEX_ENTRY_SIZE <= self.index_file.len()
        {
            let pos = self.index_write_pos;

            self.index_file[pos..pos + 8].copy_from_slice(&batch.base_offset.to_be_bytes());
            self.index_file[pos + 8..pos + 16].copy_from_slice(&log_pos.to_be_bytes());

            // FIXME: update this after having full record batch message format: https://kafka.apache.org/43/implementation/message-format/
            self.time_index_file[pos..pos + 8].copy_from_slice(&batch.base_offset.to_be_bytes());
            self.time_index_file[pos + 8..pos + 16]
                .copy_from_slice(&batch.base_offset.to_be_bytes());

            self.index_write_pos += INDEX_ENTRY_SIZE;
            self.index_count += 1;
            self.bytes_since_last_index = 0;
        }

        self.size += batch_on_disk;
        Ok(())
    }

    pub fn publish(&mut self) -> Arc<SegmentView> {
        let new = self.segment.with_metadata(self.index_count, self.size);
        self.segment = new.clone();
        new
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::segment::config::SegmentConfigBuilder;
    use proto::record_batch::RecordBatch;

    use super::*;

    fn make_seg(dir: &tempdir::TempDir, base_offset: u64, segment_bytes: usize) -> LogSegment {
        let cfg = SegmentConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .base_offset(base_offset)
            .segment_bytes(segment_bytes)
            .build()
            .unwrap();
        LogSegment::open(cfg).unwrap()
    }

    fn make_batch(base_offset: u64, records_count: u32, payload: &[u8]) -> RecordBatch {
        let records = Bytes::copy_from_slice(payload);
        let batch_length = 4 + records.len() as u32;
        RecordBatch::from_compact(base_offset, batch_length, records_count, records)
    }

    #[test]
    fn creates_log_and_index_at_offset_zero() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        make_seg(&dir, 0, 1 << 20);

        let names: Vec<_> = dir
            .path()
            .read_dir()
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(names.contains(&"00000000000000000000.log".to_string()));
        assert!(names.contains(&"00000000000000000000.index".to_string()));
    }

    #[test]
    fn size_matches_bytes_written_to_log_file() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        let mut seg = make_seg(&dir, 0, 1 << 20);

        let batch = make_batch(0, 1, b"hello");
        seg.append_batch(&batch).unwrap();

        assert_eq!(seg.size, BATCH_HEADER_PREFIX + batch.batch_length as usize);
    }

    #[test]
    fn publish_reflects_current_size_and_index_count() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        let mut seg = make_seg(&dir, 0, 1 << 20);

        let batch = make_batch(0, 2, b"xy");
        seg.append_batch(&batch).unwrap();
        let view = seg.publish();

        assert_eq!(view.size, seg.size);
        assert_eq!(view.index_count, seg.index_count);
    }
}
