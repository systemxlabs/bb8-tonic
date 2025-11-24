use std::{
    fmt::Debug,
    sync::atomic::{AtomicU32, Ordering},
};

use tonic::transport::Endpoint;

pub trait Choose: Debug + Send + Sync {
    fn choose<'a>(&self, endpoints: &'a [Endpoint]) -> &'a Endpoint;
}

#[derive(Debug)]
pub struct First;

impl Choose for First {
    fn choose<'a>(&self, endpoints: &'a [Endpoint]) -> &'a Endpoint {
        endpoints.first().expect("No endpoints provided")
    }
}

#[derive(Debug)]
pub struct RoundRobin {
    index: AtomicU32,
}

impl RoundRobin {
    pub fn new() -> Self {
        Self {
            index: AtomicU32::new(0),
        }
    }
}

impl Choose for RoundRobin {
    fn choose<'a>(&self, endpoints: &'a [Endpoint]) -> &'a Endpoint {
        let index = self.index.fetch_add(1, Ordering::Relaxed) % endpoints.len() as u32;
        &endpoints[index as usize]
    }
}
