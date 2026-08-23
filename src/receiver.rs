use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::protocol::{DecisionStatus, DownloadStatus, Offer};
use anyhow::{Context, Result};
use iroh::{
    Endpoint,
    endpoint::{Connection, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use iroh_blobs::{api::downloader::DownloadProgressItem, store::mem::MemStore};
use n0_error::e;
use n0_future::StreamExt;
use tokio::{
    io::AsyncWriteExt,
    sync::{mpsc, oneshot, watch},
};

pub enum OfferDecision {
    Accept(PathBuf),
    Decline,
}

pub type OfferRequest = (Offer, oneshot::Sender<OfferDecision>);

#[derive(Debug, Clone)]
pub struct OfferProtocol {
    pub endpoint: Endpoint,
    pub store: MemStore,
    pub offer_tx: mpsc::Sender<OfferRequest>,
    pub progress_tx: watch::Sender<u64>,
}

impl OfferProtocol {
    pub fn new(
        endpoint: &Endpoint,
        store: &MemStore,
        offer_tx: mpsc::Sender<OfferRequest>,
        progress_tx: watch::Sender<u64>,
    ) -> Self {
        Self {
            endpoint: endpoint.clone(),
            store: store.clone(),
            offer_tx,
            progress_tx,
        }
    }
}

impl ProtocolHandler for OfferProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (send, mut recv) = connection.accept_bi().await?;

        let offer = Offer::read_from(&mut recv).await.map_err(accept_error)?;

        if offer.ticket.addr().id != connection.remote_id() {
            return Err(e!(AcceptError::NotAllowed));
        }

        let (decision_tx, decision_rx) = oneshot::channel();
        self.offer_tx
            .send((offer.clone(), decision_tx))
            .await
            .context("failed to send offer to UI")
            .map_err(accept_error)?;

        // receiver times out afer not receiving accept from local ui within 60 seconds.
        let decision_ui = tokio::time::timeout(Duration::from_secs(10), decision_rx)
            .await
            .ok()
            .and_then(Result::ok);

        match decision_ui {
            Some(OfferDecision::Accept(download_dir)) => {
                accept_decision(
                    &download_dir,
                    connection,
                    send,
                    &self.progress_tx,
                    offer,
                    &self.endpoint,
                    &self.store,
                )
                .await
            }
            _ => decline_decision(connection, send).await,
        }
    }
}

async fn decline_decision(connection: Connection, mut send: SendStream) -> Result<(), AcceptError> {
    println!("No transfer executed.");
    let _ = send.write_u8(DecisionStatus::Declined as u8).await;
    let _ = send.finish();
    connection.closed().await;
    Err(e!(AcceptError::NotAllowed))
}

async fn accept_decision(
    download_dir: &Path,
    connection: Connection,
    mut send: SendStream,
    progress_tx: &watch::Sender<u64>,
    offer: Offer,
    endpoint: &Endpoint,
    store: &MemStore,
) -> Result<(), AcceptError> {
    send.write_u8(DecisionStatus::Accepted as u8).await?;

    if let Err(e) = download(&mut send, progress_tx, endpoint, store, download_dir, offer).await {
        DownloadStatus::Failed
            .write_to(&mut send)
            .await
            .map_err(accept_error)?;
        let _ = send.finish();
        connection.closed().await;
        return Err(accept_error(e));
    }

    DownloadStatus::Completed
        .write_to(&mut send)
        .await
        .map_err(accept_error)?;
    send.finish()?;
    connection.closed().await;
    Ok(())
}

fn accept_error(error: anyhow::Error) -> AcceptError {
    AcceptError::from_boxed(error.into_boxed_dyn_error())
}

pub async fn download(
    send: &mut SendStream,
    progress_tx: &watch::Sender<u64>,
    endpoint: &Endpoint,
    store: &MemStore,
    download_dir: &Path,
    offer: Offer,
) -> Result<()> {
    let filename = Path::new(&offer.filename)
        .file_name()
        .context("offer contains no filename")?;

    let target = download_dir.join(filename);
    if target.try_exists()? {
        anyhow::bail!("file {} already exists", target.display())
    }

    println!("Starting download.");
    let downloader = store.downloader(endpoint);
    let mut stream = downloader
        .download(offer.ticket.hash(), Some(offer.ticket.addr().id))
        .stream()
        .await
        .context("failed to download")?;

    while let Some(item) = stream.next().await {
        match item {
            DownloadProgressItem::Progress(bytes) => {
                send_progress(send, progress_tx, bytes).await?
            }
            DownloadProgressItem::Error(error) => anyhow::bail!("download failed {error}"),
            DownloadProgressItem::DownloadError => anyhow::bail!("download failed"),
            _ => (),
        }
    }

    println!("Finished download.");

    println!("Copying to destination.");

    store
        .blobs()
        .export(offer.ticket.hash(), target)
        .await
        .context("failed to export")?;

    progress_tx.send_replace(offer.filesize);

    println!("Finished copying.");
    Ok(())
}

async fn send_progress(
    send: &mut SendStream,
    progress_tx: &watch::Sender<u64>,
    bytes: u64,
) -> Result<()> {
    // to ui (locally)
    progress_tx.send(bytes)?;
    // to sender (network)
    DownloadStatus::Progress(bytes).write_to(send).await?;
    Ok(())
}
