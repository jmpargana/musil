use std::time::Duration;

use raft::{FetchRequest, FetchResponse, LogEntry, RaftLog};
use tokio::time;

use crate::transport::{RaftMessage, Transport};

pub trait MetadataApplier: Send {
    fn apply(&mut self, entries: &[LogEntry]);
}

pub struct Observer<L: RaftLog, T: Transport, M: MetadataApplier> {
    log: L,
    transport: T,
    applier: M,
    controller_ids: Vec<u16>,
    current_leader: Option<u16>,
    fetch_offset: u64,
    last_fetched_epoch: u32,
    high_watermark: u64,
    fetch_interval_ms: u64,
}

impl<L: RaftLog + Send + 'static, T: Transport + 'static, M: MetadataApplier + 'static>
    Observer<L, T, M>
{
    pub fn new(
        log: L,
        transport: T,
        applier: M,
        controller_ids: Vec<u16>,
        fetch_interval_ms: u64,
    ) -> Self {
        let fetch_offset = log.log_end_offset();
        let last_fetched_epoch = log.last_epoch();

        Self {
            log,
            transport,
            applier,
            controller_ids,
            current_leader: None,
            fetch_offset,
            last_fetched_epoch,
            high_watermark: 0,
            fetch_interval_ms,
        }
    }

    pub async fn run(mut self) {
        let mut interval = time::interval(Duration::from_millis(self.fetch_interval_ms));

        loop {
            interval.tick().await;

            let target = match self.current_leader {
                Some(leader) => leader,
                None => {
                    if self.controller_ids.is_empty() {
                        continue;
                    }
                    self.controller_ids[0]
                }
            };

            let req = FetchRequest {
                epoch: 0,
                fetch_offset: self.fetch_offset,
                last_fetched_epoch: self.last_fetched_epoch,
            };

            self.transport
                .send(target, RaftMessage::FetchRequest(req))
                .await;

            let input = self.transport.recv().await;
            let resp = match input {
                crate::transport::RunnerInput::NetworkEvent(raft::Event::FetchResponse {
                    from: _,
                    resp,
                }) => resp,
                _ => continue,
            };

            self.handle_fetch_response(resp).await;
        }
    }

    async fn handle_fetch_response(&mut self, resp: FetchResponse) {
        if resp.epoch > 0 {
            self.current_leader = Some(resp.epoch as u16);
        }

        if let Some(diverging) = resp.diverging {
            self.log.truncate(diverging.end_offset).await;
            self.fetch_offset = diverging.end_offset;
            if diverging.end_offset == 0 {
                self.last_fetched_epoch = 0;
            }
            return;
        }

        for entry in &resp.entries {
            self.log.append(entry.clone()).await;
            self.last_fetched_epoch = entry.epoch;
            self.fetch_offset = entry.offset + 1;
        }

        if resp.high_watermark > self.high_watermark {
            let old_hwm = self.high_watermark;
            self.high_watermark = resp.high_watermark;
            let committed = self.log.entries(old_hwm, resp.high_watermark);
            if !committed.is_empty() {
                self.applier.apply(&committed);
            }
        }
    }
}
