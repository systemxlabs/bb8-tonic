mod choose;

pub use choose::*;

use bb8::ManageConnection;
use bytes::Bytes;
use std::sync::Arc;
use tonic::{
    client::GrpcService,
    transport::{Channel, Endpoint},
};

#[derive(Debug, Clone)]
pub struct TonicChannelManager {
    endpoints: Vec<Endpoint>,
    choose: Arc<dyn Choose>,
}

impl TonicChannelManager {
    pub fn new(endpoints: Vec<Endpoint>, choose: Arc<dyn Choose>) -> Self {
        Self { endpoints, choose }
    }

    pub fn new_round_robin(endpoints: Vec<Endpoint>) -> Self {
        Self::new(endpoints, Arc::new(RoundRobin::new()))
    }

    pub fn new_single(endpoint: Endpoint) -> Self {
        Self {
            endpoints: vec![endpoint],
            choose: Arc::new(First),
        }
    }

    pub fn from_static_single(uri: &'static str) -> Self {
        Self::new_single(Endpoint::from_static(uri))
    }

    pub fn from_shared_single(uri: impl Into<Bytes>) -> Result<Self, tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(uri)?;
        Ok(Self::new_single(endpoint))
    }

    fn choose_one(&self) -> &Endpoint {
        self.choose.choose(&self.endpoints)
    }
}

impl ManageConnection for TonicChannelManager {
    type Connection = Channel;
    type Error = tonic::transport::Error;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send {
        async { self.choose_one().connect().await }
    }

    fn is_valid(
        &self,
        conn: &mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        futures::future::poll_fn(|cx| conn.poll_ready(cx))
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_choose_first() {
        let endpoints = vec![
            Endpoint::from_static("http://localhost:8080"),
            Endpoint::from_static("http://localhost:8081"),
        ];
        let manager = TonicChannelManager::new(endpoints.clone(), Arc::new(First));
        assert_eq!(manager.choose_one().uri(), (&endpoints[0]).uri());
        assert_eq!(manager.choose_one().uri(), (&endpoints[0]).uri());
    }

    #[test]
    fn test_choose_round_robin() {
        let endpoints = vec![
            Endpoint::from_static("http://localhost:8080"),
            Endpoint::from_static("http://localhost:8081"),
        ];
        let manager = TonicChannelManager::new_round_robin(endpoints.clone());
        assert_eq!(manager.choose_one().uri(), (&endpoints[0]).uri());
        assert_eq!(manager.choose_one().uri(), (&endpoints[1]).uri());
        assert_eq!(manager.choose_one().uri(), (&endpoints[0]).uri());
        assert_eq!(manager.choose_one().uri(), (&endpoints[1]).uri());
    }
}
