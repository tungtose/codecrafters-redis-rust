use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use tokio::{
    sync::Notify,
    time::{self, Instant},
};

#[derive(Debug, Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    background_task: Notify,
}

impl Shared {
    fn remove_expired_keys(&self) -> Option<Instant> {
        println!("Exec remove expired key");
        let mut state = self.state.lock().unwrap();

        let now = Instant::now();

        let state = &mut *state;

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }

            // remove
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }
}

#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,
    expirations: BTreeSet<(Instant, String)>,
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations.iter().next().map(|ex| ex.0)
    }
}

#[derive(Debug)]
struct Entry {
    data: Bytes,
    expires_at: Option<Instant>,
}

impl Db {
    pub fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
            }),
            background_task: Notify::new(),
        });

        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    pub fn set(&self, key: &str, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        let mut notify = false;

        let expires_at = expire.map(|duration| {
            let when = Instant::now() + duration;

            notify = state.next_expiration().map(|ex| ex > when).unwrap_or(true);

            state.expirations.insert((when, key.to_string()));

            when
        });

        let prev = state.entries.insert(
            key.to_string(),
            Entry {
                data: value,
                expires_at,
            },
        );

        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, key.to_string()));
            }
        }

        // Release mutex before notify the background task
        drop(state);

        println!("Notify: {}", notify);

        if notify {
            self.shared.background_task.notify_one();
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();

        state.entries.get(key).map(|entry| entry.data.clone())
    }
}

async fn purge_expired_tasks(shared: Arc<Shared>) {
    loop {
        if let Some(when) = shared.remove_expired_keys() {
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            shared.background_task.notified().await;
        }
    }
}
