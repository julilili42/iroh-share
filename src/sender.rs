use std::{path::Path, time::Duration};

use crate::protocol::{self, DecisionStatus, DownloadStatus, Offer, transfer_decision};
use anyhow::{Result, anyhow, bail};
use iroh::{Endpoint, EndpointAddr, endpoint::RecvStream};
use iroh_blobs::{store::mem::MemStore, ticket::BlobTicket};
use n0_error::StackResultExt;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    Completed,
    Declined,
}

pub async fn run_sender(
    progress_tx: watch::Sender<u64>,
    filename: &str,
    endpoint: &Endpoint,
    store: &MemStore,
    endpoint_addr: EndpointAddr,
) -> Result<SendOutcome> {
    let file_path = Path::new(filename);
    let safe_name = file_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("invalid filename")?;

    let abs_path = std::path::absolute(file_path)?;

    let metadata = tokio::fs::metadata(&abs_path).await?;
    if !metadata.is_file() {
        bail!("{} is not a regular file", abs_path.display());
    }

    let filesize = metadata.len();

    println!("Hashing file.");

    // When we import a blob, we get back a "tag" that refers to said blob in the store
    // and allows us to control when/if it gets garbage-collected
    let tag = store.blobs().add_path(abs_path).await?;
    let ticket = BlobTicket::new(endpoint.id().into(), tag.hash, tag.format);

    println!("File hashed.");

    let offer = Offer::new(safe_name, filesize, &ticket);

    let conn = endpoint.connect(endpoint_addr, protocol::ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    offer.write_to(&mut send).await?;

    // if receiver does not send accept byte within 60 seconds sender times out.
    let decision = tokio::time::timeout(Duration::from_secs(60), transfer_decision(&mut recv))
        .await
        .map_err(|_| anyhow!("receiver did not respond within 60 seconds"))??;

    match decision {
        DecisionStatus::Accepted => println!("Accepted offer."),
        DecisionStatus::Declined => {
            println!("Declined offer.");
            return Ok(SendOutcome::Declined);
        }
    }

    downloaded(&mut recv, progress_tx).await
}

pub async fn downloaded(
    recv: &mut RecvStream,
    progress_tx: watch::Sender<u64>,
) -> Result<SendOutcome> {
    loop {
        match DownloadStatus::read_from(recv).await? {
            DownloadStatus::Progress(bytes) => {
                progress_tx.send(bytes)?;
            }
            DownloadStatus::Completed => break,
            DownloadStatus::Failed => {
                bail!("download failed")
            }
        }
    }
    Ok(SendOutcome::Completed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::endpoint::presets;

    #[tokio::test]
    async fn rejects_directory_sources() -> Result<()> {
        let endpoint = Endpoint::bind(presets::Minimal).await?;
        let store = MemStore::new();
        let (progress_tx, _) = watch::channel(0);

        let error = run_sender(
            progress_tx,
            env!("CARGO_MANIFEST_DIR"),
            &endpoint,
            &store,
            endpoint.addr(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("is not a regular file"));
        endpoint.close().await;
        Ok(())
    }
}
