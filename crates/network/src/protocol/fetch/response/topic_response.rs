use core::fmt;

use crate::protocol::fetch::response::partition_response::PartitionResponse;

#[derive(Debug)]
pub struct TopicResponse {
    pub topic: String,
    pub partitions: Vec<PartitionResponse>,
}

impl fmt::Display for TopicResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for p in &self.partitions {
            for batch in &p.records {
                let records: Vec<proto::record::Record> = batch.clone().into();
                for r in records {
                    writeln!(
                        f,
                        "[{}-{}:{}] {}",
                        self.topic,
                        p.partition_index,
                        String::from_utf8_lossy(&r.key),
                        String::from_utf8_lossy(&r.value)
                    )?;
                }
            }
        }
        Ok(())
    }
}

impl TopicResponse {
    pub fn get_size(&self) -> u32 {
        2 + self.topic.len() as u32 + 4 + self.partitions.iter().map(|p| p.get_size()).sum::<u32>()
    }
}

#[cfg(test)]
mod tests {
    use proto::{error_codes::ErrorCode, record::Record, record_batch::RecordBatch};

    use super::*;

    fn partition(index: u32, batches: Vec<RecordBatch>) -> PartitionResponse {
        PartitionResponse {
            partition_index: index,
            error_code: ErrorCode::None,
            high_watermark: 0,
            log_start_offset: 0,
            records: batches,
        }
    }

    fn topic(name: &str, partitions: Vec<PartitionResponse>) -> TopicResponse {
        TopicResponse {
            topic: name.to_string(),
            partitions,
        }
    }

    #[test]
    fn display_single_record() {
        let record = Record::new(0, b"k1", b"hello");
        let batch: RecordBatch = vec![record].into();
        let t = topic("my-topic", vec![partition(0, vec![batch])]);
        let output = format!("{t}");
        assert!(output.contains("[my-topic-0:k1] hello"), "got: {output}");
    }

    #[test]
    fn display_multiple_records_in_one_batch() {
        let records = vec![
            Record::new(0, b"key-a", b"val-a"),
            Record::new(1, b"key-b", b"val-b"),
        ];
        let batch: RecordBatch = records.into();
        let t = topic("t", vec![partition(2, vec![batch])]);
        let output = format!("{t}");
        assert!(output.contains("[t-2:key-a] val-a"), "got: {output}");
        assert!(output.contains("[t-2:key-b] val-b"), "got: {output}");
    }

    #[test]
    fn display_multiple_batches() {
        let b1: RecordBatch = vec![Record::new(0, b"k1", b"v1")].into();
        let b2: RecordBatch = vec![Record::new(0, b"k2", b"v2")].into();
        let t = topic("t", vec![partition(0, vec![b1, b2])]);
        let output = format!("{t}");
        assert!(output.contains("[t-0:k1] v1"), "got: {output}");
        assert!(output.contains("[t-0:k2] v2"), "got: {output}");
    }

    #[test]
    fn display_multiple_partitions() {
        let p0 = partition(0, vec![vec![Record::new(0, b"k0", b"v0")].into()]);
        let p1 = partition(1, vec![vec![Record::new(0, b"k1", b"v1")].into()]);
        let t = topic("top", vec![p0, p1]);
        let output = format!("{t}");
        assert!(output.contains("[top-0:k0] v0"), "got: {output}");
        assert!(output.contains("[top-1:k1] v1"), "got: {output}");
    }

    #[test]
    fn display_empty_partitions_produces_no_output() {
        let t = topic("t", vec![partition(0, vec![])]);
        assert_eq!(format!("{t}"), "");
    }

    #[test]
    fn display_no_partitions_produces_no_output() {
        let t = topic("t", vec![]);
        assert_eq!(format!("{t}"), "");
    }
}
