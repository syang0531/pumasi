//! The dashboard's one connection to the control plane. Browsers never
//! reach the cluster themselves (they cannot speak iroh QUIC): every page
//! and API call becomes an admin call made here, with the secret of the
//! ticket the browser signed in with, so the control plane applies that
//! ticket's role exactly as it does for the CLI.

use std::sync::Arc;

use anyhow::{Context, Result};
use pumasi_cluster::AdminClient;
use pumasi_proto::{
    Secret,
    msg::{AdminOp, AdminResponse},
    net::NetMode,
    ticket::AdminTicket,
};
use tokio::sync::Mutex;

pub struct Backend {
    ticket: AdminTicket,
    net: NetMode,
    client: Mutex<Option<Arc<AdminClient>>>,
}

impl Backend {
    pub fn new(ticket: AdminTicket, net: NetMode) -> Self {
        Self {
            ticket,
            net,
            client: Mutex::new(None),
        }
    }

    /// The control plane this dashboard talks to.
    pub fn cluster_id(&self) -> iroh::EndpointId {
        self.ticket.cluster.id
    }

    /// The secret of the ticket `pumasi ui` was started with.
    pub fn own_secret(&self) -> Secret {
        self.ticket.secret
    }

    /// The shared connection, opened (again) when it is missing or closed.
    async fn base(&self) -> Result<Arc<AdminClient>> {
        let mut guard = self.client.lock().await;
        if let Some(c) = guard.as_ref()
            && !c.is_closed()
        {
            return Ok(c.clone());
        }
        let client = match guard.take() {
            Some(old) => old.reconnect().await,
            None => AdminClient::connect(&self.ticket, self.net).await,
        }
        .context("cannot reach the control plane")?;
        let client = Arc::new(client);
        *guard = Some(client.clone());
        Ok(client)
    }

    /// A client that calls with `secret` over the shared connection, for
    /// calls that need more than one round trip (store transfers).
    pub async fn client(&self, secret: Secret) -> Result<AdminClient> {
        Ok(self.base().await?.with_secret(secret))
    }

    /// One admin call as the holder of `secret`.
    pub async fn call(&self, secret: Secret, op: AdminOp) -> Result<AdminResponse> {
        self.client(secret).await?.call(op).await
    }
}
