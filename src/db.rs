use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,
}

#[derive(Debug)]
struct Entry {
    data: Bytes,
}

impl Db {
    pub fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
            }),
        });

        Db { shared }
    }

    pub fn set(&self, key: &str, value: Bytes) {
        let mut state = self.shared.state.lock().unwrap();

        state.entries.insert(key.to_string(), Entry { data: value });
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();

        state.entries.get(key).map(|entry| entry.data.clone())
    }
}
