use std::collections::HashSet;

use tokio::{sync::mpsc, time};

type NodeId = u16;

trait Cluster {
    fn peers_of(id: NodeId) -> HashSet<NodeId>;
    fn has_majority(votes: u16) -> bool;
}

enum Command {}
enum Action {}
enum Event {}

struct Node<L, C>
where
    L: Log,
    C: Cluster,
{
    // persistent
    voted_for: Option<NodeId>,
    log: L,

    // volatile

    // Configuration
    cluster: Cluster,
}

impl Node<L, C>
where
    L: Log,
    C: Cluster,
{
    fn handle(action: Event) -> Vec<Action> {
        vec![]
    }
}

struct Runner<T, L>
where
    T: Transport,
    L: Log,
{
    node: Node<L>,
    rx: mpsc::Receiver<Command>,
    transport: T,
    election_timer: time::Duration,
}

impl Runner<T, L>
where
    T: Transport,
    L: Log,
{
    async fn run(&mut self) {
        loop {
            tokio::select! {
                command = self.rx.recv().await => {
                    let actions = self.node.handle(command.into());
                    // execute(actions)
                }
                message = self.transport.recv().await => {

                }

                _ = self.election_timer => {

                }
            }
        }
    }

    fn execute(actions: Vec<Action>) {
        actions.iter().for_each(|action| match action {});
    }
}

#[async_trait]
trait Transport {
    async fn send(e: Event);
    async fn recv() -> Event;
}

/// TODO: what should exactly Entry entail?
struct Entry;

trait Log {
    fn append(entry: Entry);
}
