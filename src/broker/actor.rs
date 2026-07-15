use std::sync::{Arc, mpsc};

use arc_swap::ArcSwap;

use crate::broker::{command::MetadataCommand, state::MetadataState};

pub struct MetadataActor {
    rx: mpsc::Receiver<MetadataCommand>,
    snapshot: Arc<ArcSwap<MetadataState>>,
}
