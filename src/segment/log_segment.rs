use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::Arc,
};

use memmap::MmapOptions;

use crate::{
    segment::{config::SegmentConfig, metadata::SegmentView},
    storage::record_batch::RecordBatch,
};

const INDEX_ENTRY_SIZE: usize = 16; // (u64 offset + u64 position)

pub struct LogSegment {
    segment: Arc<SegmentView>,

    log_file: File,
    index_file: memmap::MmapMut,

    index_write_pos: usize,

    pub size: usize,
    index_count: usize,

    bytes_since_last_index: usize,
    index_threshold_bytes: usize,
}

impl LogSegment {
    pub fn new(opts: SegmentConfig) -> io::Result<Self> {
        let base_path = Path::new(&opts.base_dir);

        fs::create_dir_all(base_path)?;

        let log_path = base_path.join(format!("{:020}.log", opts.base_offset));
        let index_path = base_path.join(format!("{:020}.index", opts.base_offset));

        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(log_path)?;

        let index_file_handle = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(index_path)?;

        let max_entries = opts.segment_bytes / opts.index_interval_bytes + 1;
        let index_size = max_entries * INDEX_ENTRY_SIZE;

        index_file_handle.set_len(index_size as u64)?;
        let index_file = unsafe {
            MmapOptions::new()
                .len(index_size)
                .map_mut(&index_file_handle)?
        };

        // alternatively, we can create new handles in the segment new function, because those should be read-only
        let segment = Arc::new(SegmentView::new(
            opts.base_offset,
            log_file.try_clone()?,
            index_file_handle.try_clone()?,
        ));

        Ok(Self {
            segment,
            log_file,
            index_file,
            index_write_pos: 0,
            index_count: 0,
            size: 0,
            bytes_since_last_index: opts.index_interval_bytes,
            index_threshold_bytes: opts.index_interval_bytes,
        })
    }

    pub fn append_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        let log_pos = self.log_file.metadata()?.len();

        // batch is already encoded
        self.log_file.write_all(&batch.encode_header())?;
        self.log_file.write_all(&batch.records)?;

        // check if there's a new index
        // 8 (base_offset) + 4 (batch_length) + batch_length bytes are written
        self.bytes_since_last_index += 12 + batch.batch_length as usize;

        if self.bytes_since_last_index >= self.index_threshold_bytes {
            let pos = self.index_write_pos;

            self.index_file[pos..pos + 8].copy_from_slice(&batch.base_offset.to_le_bytes());
            self.index_file[pos + 8..pos + 16].copy_from_slice(&log_pos.to_le_bytes());

            self.index_write_pos += INDEX_ENTRY_SIZE;
            self.index_count += 1;
            self.bytes_since_last_index = 0;
        }

        self.size += 12 + batch.batch_length as usize;
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
    use crate::storage::record_batch::RecordBatch;

    use super::*;

    fn make_seg(dir: &tempdir::TempDir, base_offset: u64, segment_bytes: usize) -> LogSegment {
        let cfg = SegmentConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .base_offset(base_offset)
            .segment_bytes(segment_bytes)
            .build()
            .unwrap();
        LogSegment::new(cfg).unwrap()
    }

    fn make_batch(base_offset: u64, records_count: u32, payload: &[u8]) -> RecordBatch {
        RecordBatch {
            base_offset,
            batch_length: 4 + payload.len() as u32,
            records_count,
            records: Bytes::copy_from_slice(payload),
        }
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
    fn creates_log_and_index_at_nonzero_offset() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        make_seg(&dir, 1230, 1 << 20);

        let names: Vec<_> = dir
            .path()
            .read_dir()
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(names.contains(&"00000000000000001230.log".to_string()));
        assert!(names.contains(&"00000000000000001230.index".to_string()));
    }

    #[test]
    fn size_matches_bytes_written_to_log_file() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        let mut seg = make_seg(&dir, 0, 1 << 20);

        let batch = make_batch(0, 1, b"hello");
        seg.append_batch(&batch).unwrap();

        let log_path = dir
            .path()
            .read_dir()
            .unwrap()
            .find(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .into_string()
                    .unwrap()
                    .ends_with(".log")
            })
            .unwrap()
            .unwrap()
            .path();

        let file_len = std::fs::metadata(&log_path).unwrap().len() as usize;
        assert_eq!(
            seg.size, file_len,
            "size ({}) must equal actual file length ({})",
            seg.size, file_len
        );
        // Explicit check: 12 + batch_length = 12 + 4 + 5 = 21
        assert_eq!(seg.size, 12 + batch.batch_length as usize);
    }

    #[test]
    fn size_accumulates_across_multiple_batches() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        let mut seg = make_seg(&dir, 0, 1 << 20);

        let b1 = make_batch(0, 1, b"aaa");
        let b2 = make_batch(1, 1, b"bbbbbb");
        seg.append_batch(&b1).unwrap();
        seg.append_batch(&b2).unwrap();

        let expected = (12 + b1.batch_length as usize) + (12 + b2.batch_length as usize);
        assert_eq!(seg.size, expected);
    }

    #[test]
    fn index_entry_written_on_first_append() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        let mut seg = make_seg(&dir, 0, 1 << 20);

        let batch = make_batch(0, 1, b"data");
        seg.append_batch(&batch).unwrap();

        assert_eq!(
            seg.index_count, 1,
            "first append must produce one index entry"
        );
    }

    #[test]
    fn index_entry_records_correct_offset_and_position() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        let mut seg = make_seg(&dir, 42, 1 << 20);

        let batch = make_batch(42, 1, b"payload");
        seg.append_batch(&batch).unwrap();

        let offset = u64::from_le_bytes(seg.index_file[0..8].try_into().unwrap());
        let pos = u64::from_le_bytes(seg.index_file[8..16].try_into().unwrap());

        assert_eq!(offset, 42);
        assert_eq!(pos, 0, "first batch written at file position 0");
    }

    #[test]
    fn second_index_entry_at_correct_file_position() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        let cfg = SegmentConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .base_offset(0)
            .segment_bytes(1 << 20)
            .index_interval_bytes(1)
            .build()
            .unwrap();
        let mut seg = LogSegment::new(cfg).unwrap();

        let b1 = make_batch(0, 1, b"first");
        let b2 = make_batch(1, 1, b"second");
        seg.append_batch(&b1).unwrap();
        seg.append_batch(&b2).unwrap();

        assert_eq!(seg.index_count, 2);

        let pos = u64::from_le_bytes(seg.index_file[24..32].try_into().unwrap());
        let expected_pos = (12 + b1.batch_length) as u64;
        assert_eq!(pos, expected_pos);
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

    #[test]
    fn publish_twice_returns_updated_view() {
        let dir = tempdir::TempDir::new("seg-test").unwrap();
        let mut seg = make_seg(&dir, 0, 1 << 20);

        let b1 = make_batch(0, 1, b"a");
        seg.append_batch(&b1).unwrap();
        let v1 = seg.publish();

        let b2 = make_batch(1, 1, b"bb");
        seg.append_batch(&b2).unwrap();
        let v2 = seg.publish();

        assert!(v2.size > v1.size);
    }
}
