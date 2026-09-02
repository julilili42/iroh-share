use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    cli::{Command, confirm, parse_arguments, select_receiver},
    receiver::{OfferDecision, OfferProtocol, OfferRequest},
    sender::run_sender,
};
use anyhow::Result;
use iroh::{Endpoint, EndpointAddr, endpoint::presets, endpoint_info::UserData, protocol::Router};
use iroh_blobs::{
    BlobsProtocol,
    api::Store,
    store::{
        GcConfig,
        fs::{
            FsStore,
            options::{InlineOptions, Options},
        },
    },
};
use iroh_tickets::endpoint::EndpointTicket;
use tokio::sync::{mpsc, watch};

mod app;
mod cli;
mod mdns;
mod protocol;
mod receiver;
mod sender;

#[derive(Debug)]
struct Runtime {
    endpoint: Endpoint,
    store: Store,
    store_dir: PathBuf,
    router: Router,
    ticket: EndpointTicket,
    offer_rx: mpsc::Receiver<OfferRequest>,
    peer_rx: watch::Receiver<Vec<(UserData, EndpointAddr)>>,
    progress_rx: watch::Receiver<u64>,
    progress_tx: watch::Sender<u64>,
}

async fn start_iroh(store_dir: &Path) -> Result<Runtime> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let endpoint = Endpoint::bind(presets::N0).await?;
    let store = FsStore::load_with_opts(
        store_dir.join("blobs.db"),
        Options {
            inline: InlineOptions::NO_INLINE,
            gc: Some(GcConfig {
                interval: Duration::from_secs(60),
                add_protected: None,
            }),
            ..Options::new(store_dir)
        },
    )
    .await?
    .into();

    let store_dir = store_dir.to_owned();

    // startup time
    if tokio::time::timeout(
        std::time::Duration::from_secs(iroh::NET_REPORT_TIMEOUT),
        endpoint.online(),
    )
    .await
    .is_err()
    {
        eprintln!("Relay unavailable ticket may only work on the local network.");
    }

    let ticket = EndpointTicket::new(endpoint.addr());

    println!("Ticket:");
    println!("{ticket}");

    let (offer_tx, offer_rx) = mpsc::channel(10);
    let (progress_tx, progress_rx) = watch::channel(0_u64);
    let blobs_handler = BlobsProtocol::new(&store, None);
    let offer_handler = OfferProtocol::new(&endpoint, &store, offer_tx, progress_tx.clone());
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs_handler)
        .accept(protocol::ALPN, offer_handler)
        .spawn();

    let device_name = whoami::devicename().or_else(|_| whoami::hostname())?;
    let mdns = mdns::enable(&endpoint, &device_name)?;
    let (peer_tx, peer_rx) = watch::channel(Vec::new());
    tokio::spawn(mdns::discover(mdns, peer_tx));

    Ok(Runtime {
        endpoint,
        store,
        store_dir,
        router,
        ticket,
        offer_rx,
        peer_rx,
        progress_rx,
        progress_tx,
    })
}

pub async fn desktop_main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = parse_arguments(args.iter().map(String::as_str).collect())?;

    if let Command::Version | Command::Help = command {
        return Ok(());
    }

    let store_dir = std::env::temp_dir().join(format!("iroh-share-{}", std::process::id()));
    let mut runtime = start_iroh(&store_dir).await?;
    if matches!(command, Command::Ui) {
        return app::run(runtime);
    }

    let router = runtime.router.clone();
    let result = async {
        match command {
            Command::Send {
                filename,
                endpoint_addr,
            } => {
                let endpoint_addr = match endpoint_addr {
                    Some(addr) => addr,
                    None => select_receiver(runtime.peer_rx.clone()).await?,
                };
                run_sender(
                    runtime.progress_tx,
                    &filename,
                    &runtime.endpoint,
                    &runtime.store,
                    endpoint_addr,
                )
                .await
                .map(|_| ())
            }
            Command::Receive { download_dir } => loop {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => return result.map_err(Into::into),
                    request = runtime.offer_rx.recv() => {
                        let Some((offer, tx)) = request else { break Ok(()) };
                        let decision = if confirm(&offer).await? {
                            OfferDecision::Accept(download_dir.clone())
                        } else {
                            OfferDecision::Decline
                        };
                        let _ = tx.send(decision);
                    }
                }
            },
            _ => unreachable!(),
        }
    }
    .await;

    let shutdown_result = router.shutdown().await;
    let cleanup_result = std::fs::remove_dir_all(runtime.store_dir);
    result?;
    shutdown_result?;
    cleanup_result?;
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt::init();
    let store_dir = std::env::temp_dir().join(format!("iroh-share-{}", std::process::id()));
    let runtime =
        tauri::async_runtime::block_on(start_iroh(&store_dir)).expect("failed to start Iroh");
    app::run(runtime).expect("failed to run Iroh Share");
}
