pub struct Node {
    // Identity

    // Persistent

    // Volatile

    // Leader-only

    // Timers
}

impl Node {
    pub async fn run(&mut self) {
        loop {
            tokio::select! {}
        }
    }

    async fn handle_event(&mut self, event: RaftEvent) {
        match (&self.role, event) {}
    }
}
