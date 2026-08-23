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
        let decision_ui = tokio::time::timeout(Duration::from_secs(60), decision_rx)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sender::{SendOutcome, run_sender};
    use anyhow::anyhow;
    use iroh::{EndpointAddr, endpoint::presets, protocol::Router};
    use iroh_blobs::BlobsProtocol;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::time;

    const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/small.txt");

    async fn send_and_decide(
        endpoint: &Endpoint,
        store: &MemStore,
        receiver_addr: EndpointAddr,
        offer_rx: &mut mpsc::Receiver<OfferRequest>,
        decision: OfferDecision,
    ) -> Result<SendOutcome> {
        let endpoint = endpoint.clone();
        let store = store.clone();
        let (progress_tx, _progress_rx) = watch::channel(0);
        let sender = tokio::spawn(async move {
            run_sender(progress_tx, FIXTURE, &endpoint, &store, receiver_addr).await
        });

        let (offer, decision_tx) = time::timeout(Duration::from_secs(5), offer_rx.recv())
            .await?
            .ok_or_else(|| anyhow!("offer channel closed"))?;
        assert_eq!(offer.filename, "small.txt");
        assert_eq!(offer.filesize, std::fs::metadata(FIXTURE)?.len());
        decision_tx
            .send(decision)
            .map_err(|_| anyhow!("receiver stopped waiting for the decision"))?;

        time::timeout(Duration::from_secs(15), sender).await??
    }

    #[tokio::test]
    async fn transfers_declines_and_protects_existing_files() -> Result<()> {
        let receiver_endpoint = Endpoint::bind(presets::Minimal).await?;
        let receiver_store = MemStore::new();
        let (offer_tx, mut offer_rx) = mpsc::channel(1);
        let (receiver_progress_tx, receiver_progress_rx) = watch::channel(0);
        let receiver_router = Router::builder(receiver_endpoint.clone())
            .accept(iroh_blobs::ALPN, BlobsProtocol::new(&receiver_store, None))
            .accept(
                crate::protocol::ALPN,
                OfferProtocol::new(
                    &receiver_endpoint,
                    &receiver_store,
                    offer_tx,
                    receiver_progress_tx,
                ),
            )
            .spawn();

        let sender_endpoint = Endpoint::bind(presets::Minimal).await?;
        let sender_store = MemStore::new();
        let sender_router = Router::builder(sender_endpoint.clone())
            .accept(iroh_blobs::ALPN, BlobsProtocol::new(&sender_store, None))
            .spawn();

        let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let download_dir = std::env::temp_dir().join(format!("iroh-share-{unique}"));
        std::fs::create_dir(&download_dir)?;

        let receiver_addr = receiver_endpoint.addr();
        assert_eq!(
            send_and_decide(
                &sender_endpoint,
                &sender_store,
                receiver_addr.clone(),
                &mut offer_rx,
                OfferDecision::Decline,
            )
            .await?,
            SendOutcome::Declined
        );
        assert!(!download_dir.join("small.txt").exists());

        assert_eq!(
            send_and_decide(
                &sender_endpoint,
                &sender_store,
                receiver_addr.clone(),
                &mut offer_rx,
                OfferDecision::Accept(download_dir.clone()),
            )
            .await?,
            SendOutcome::Completed
        );
        assert_eq!(
            std::fs::read(download_dir.join("small.txt"))?,
            std::fs::read(FIXTURE)?
        );
        assert_eq!(
            *receiver_progress_rx.borrow(),
            std::fs::metadata(FIXTURE)?.len()
        );

        let error = send_and_decide(
            &sender_endpoint,
            &sender_store,
            receiver_addr,
            &mut offer_rx,
            OfferDecision::Accept(download_dir.clone()),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("download failed"));

        sender_router.shutdown().await?;
        receiver_router.shutdown().await?;
        std::fs::remove_dir_all(download_dir)?;
        Ok(())
    }
}
