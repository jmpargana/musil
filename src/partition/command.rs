use tokio::sync::oneshot;

use crate::record::Record;

pub enum Command {
    Append {
        record: Record,
        done: oneshot::Sender<()>,
    },
    Shutdown,
}
