#[derive(Debug, Clone, Copy)]
pub struct BatchAttributes(pub u16);

/// Source: <https://kafka.apache.org/43/implementation/message-format/>
///
/// - bit 0~2: compression (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd)
/// - bit 3: timestampType
/// - bit 4: isTransactional (0 means not transactional)
/// - bit 5: isControlBatch (0 means not a control batch)
/// - bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
/// - bit 7~15: unused
impl BatchAttributes {
    const COMPRESSION_MASK: u16 = 0b111;
    #[allow(dead_code)]
    const TIMESTAMP_TYPE: u16 = 1 << 3;
    const TRANSACTIONAL: u16 = 1 << 4;
    const CONTROL: u16 = 1 << 5;
    #[allow(dead_code)]
    const DELETE_HORIZON: u16 = 1 << 6;

    pub fn compression(self) -> Compression {
        Compression::from_bits(self.0 & Self::COMPRESSION_MASK)
    }

    pub fn is_transactional(self) -> bool {
        self.0 & Self::TRANSACTIONAL != 0
    }

    pub fn is_control(self) -> bool {
        self.0 & Self::CONTROL != 0
    }
}

// TODO: should encode/decode methods be exposed from this struct?
pub enum Compression {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

// TODO: this could also be a From trait
impl Compression {
    pub fn from_bits(c: u16) -> Self {
        match c {
            0 => Self::None,
            1 => Self::Gzip,
            2 => Self::Snappy,
            3 => Self::Lz4,
            4 => Self::Zstd,
            _ => panic!("implement result here"),
        }
    }
}
