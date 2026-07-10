use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Read, Write},
    os::unix::fs::FileExt,
    path::Path,
    sync::Arc,
};

use bytes::Bytes;
use memmap::MmapOptions;

use crate::{
    batch::Batch,
    message::{
        consumer::{FetchPartition, PartitionResponse},
        produce::ProducePartition,
    },
    partition::Partition,
    record::Record,
    segment::{metadata::Segment, options::SegmentConfig},
};

const INDEX_ENTRY_SIZE: usize = 16; // (u64 offset + u64 position)

pub struct ActiveSegment {
    segment: Arc<Segment>,

    log_file: File,
    index_file: memmap::MmapMut,

    index_write_pos: usize,

    pub size: usize,
    index_count: usize,

    bytes_since_last_index: usize,
    index_threshold_bytes: usize,
}

impl ActiveSegment {
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
        let segment = Arc::new(Segment::new(
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

    pub fn append_batch(&mut self, batch: Batch) -> io::Result<()> {
        let log_pos = self.log_file.metadata()?.len();

        // batch is already encoded
        self.log_file.write_all(&batch.encode_header())?;
        self.log_file.write_all(&batch.records)?;

        // check if there's a new index
        self.bytes_since_last_index += batch.batch_length as usize; // FIXME: does it include baseOffset and batchLength 32 + 64?

        if self.bytes_since_last_index >= self.index_threshold_bytes {
            let pos = self.index_write_pos;

            self.index_file[pos..pos + 8].copy_from_slice(&batch.base_offset.to_le_bytes());
            self.index_file[pos + 8..pos + 16].copy_from_slice(&log_pos.to_le_bytes());

            self.index_write_pos += INDEX_ENTRY_SIZE;
            self.index_count += 1;
            self.bytes_since_last_index = 0;
        }

        self.size += batch.batch_length as usize;
        Ok(())
    }

    pub fn fetch(&self, partition_request: &FetchPartition) -> Vec<Batch> {
        self.segment.clone().fetch(partition_request)
    }

    #[deprecated(note = "use append_batch instead")]
    pub fn append(&mut self, record: Record) -> io::Result<usize> {
        let buf = record.encode();

        let log_pos = self.log_file.metadata()?.len();

        self.log_file.write_all(&(buf.len() as u32).to_le_bytes())?;
        self.log_file.write_all(&buf)?;
        self.log_file.flush()?;

        self.bytes_since_last_index += 4 + buf.len();

        if self.bytes_since_last_index >= self.index_threshold_bytes {
            let offset = record.offset.expect("record must have offset");

            let pos = self.index_write_pos;

            self.index_file[pos..pos + 8].copy_from_slice(&offset.to_le_bytes());
            self.index_file[pos + 8..pos + 16].copy_from_slice(&log_pos.to_le_bytes());
            self.index_file.flush()?; // TODO: maybe needs to be async or not there at all

            self.index_write_pos += INDEX_ENTRY_SIZE;
            self.index_count += 1;
            self.bytes_since_last_index = 0;
        }

        // TODO: should return record size or total?
        self.size += buf.len() + 4;
        Ok(buf.len() + 4)
    }

    pub fn find_pos(&self, target_offset: u64) -> Option<u64> {
        self.segment.clone().find_pos(target_offset)
    }

    pub fn publish(&mut self) -> Arc<Segment> {
        let new = self.segment.with_metadata(self.index_count, self.size);
        self.segment = new.clone();
        new
    }
}

#[cfg(test)]
mod tests {
    use std::fs::read;

    use crate::segment::options::SegmentConfigBuilder;

    use super::*;

    #[test]
    fn creates_correct_file_0() {
        let dir = tempdir::TempDir::new("./random").unwrap();

        let cfg = SegmentConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .base_offset(0)
            .build()
            .unwrap();

        let _ = ActiveSegment::new(cfg).unwrap();

        let mut files = dir.path().read_dir().unwrap();
        assert!(files.any(|f| f.unwrap().file_name() == "00000000000000000000.log"));
        assert!(files.any(|f| f.unwrap().file_name() == "00000000000000000000.index"));
    }

    #[test]
    fn creates_correct_file_offset() {
        let dir = tempdir::TempDir::new("./random").unwrap();
        let cfg = SegmentConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .base_offset(1230)
            .build()
            .unwrap();
        let _ = ActiveSegment::new(cfg).unwrap();

        let mut files = dir.path().read_dir().unwrap();
        assert!(files.any(|f| f.unwrap().file_name() == "00000000000000001230.log"));
        assert!(files.any(|f| f.unwrap().file_name() == "00000000000000001230.index"));
    }

    #[test]
    fn appends_size_to_empty_log_file() {
        let dir = tempdir::TempDir::new("./random").unwrap();
        let cfg = SegmentConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .base_offset(0)
            .build()
            .unwrap();

        let mut seg = ActiveSegment::new(cfg).unwrap();

        let mut record = Record::new(b"hello", b"world");
        record.add_offset(1);
        let appended_size = seg.append(record).unwrap();

        let mut read_dir = dir.path().read_dir().unwrap();
        let log_file = read_dir
            .find(|f| {
                *f.as_ref().unwrap().file_name().into_string().unwrap()
                    == "00000000000000000000.log".to_string()
            })
            .unwrap()
            .unwrap();
        assert_eq!(log_file.metadata().unwrap().len() as usize, appended_size);
    }

    #[test]
    fn appends_creates_index_file_at_start() {
        let dir = tempdir::TempDir::new("./random").unwrap();
        let cfg = SegmentConfigBuilder::default()
            .base_dir(dir.path().to_str().unwrap().to_string())
            .base_offset(0)
            .build()
            .unwrap();

        let mut seg = ActiveSegment::new(cfg).unwrap();

        let mut record = Record::new(b"hello", b"world");
        record.add_offset(1);
        let _ = seg.append(record).unwrap();
        drop(seg);

        let mut read_dir = dir.path().read_dir().unwrap();
        let index_file = read_dir
            .find(|f| {
                *f.as_ref().unwrap().file_name().into_string().unwrap()
                    == "00000000000000000000.index".to_string()
            })
            .unwrap()
            .unwrap();

        let mut index_file = File::open(index_file.path()).unwrap();
        let mut u64_buf = [0u8; 8];
        index_file.read_exact(&mut u64_buf).unwrap();
        assert_eq!(u64::from_le_bytes(u64_buf), 1);
        index_file.read_exact(&mut u64_buf).unwrap();
        assert_eq!(u64::from_le_bytes(u64_buf), 0);
    }
}
