use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    Runtime,
    receiver::{OfferDecision, OfferRequest},
    sender::{SendOutcome, run_sender},
};
use anyhow::Result;
use iroh::{Endpoint, EndpointId, endpoint_info::UserData};
use iroh_blobs::store::mem::MemStore;
use iroh_tickets::{Ticket, endpoint::EndpointTicket};
use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc, watch};

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct Peer {
    id: String,
    name: String,
    ticket: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct IncomingOffer {
    id: u64,
    filename: String,
    filesize: u64,
    sender: String,
}

struct PendingOffer {
    view: IncomingOffer,
    decision: tokio::sync::oneshot::Sender<OfferDecision>,
}

#[derive(Clone)]
struct AppState {
    endpoint: Endpoint,
    store: MemStore,
    ticket: String,
    display_name: String,
    peers: Arc<Mutex<Vec<Peer>>>,
    pending_offer: Arc<Mutex<Option<PendingOffer>>>,
    offer_answered: Arc<Notify>,
    send_lock: Arc<AsyncMutex<()>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InitialState {
    display_name: String,
    ticket: String,
    mobile: bool,
    peers: Vec<Peer>,
    incoming_offer: Option<IncomingOffer>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct TransferProgress {
    downloaded: u64,
    total: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SendStarted {
    filename: String,
    total: u64,
}

fn peer_list(peers: &[(UserData, iroh::EndpointAddr)]) -> Vec<Peer> {
    peers
        .iter()
        .map(|(name, address)| Peer {
            id: address.id.to_string(),
            name: name.to_string(),
            ticket: EndpointTicket::new(address.clone()).to_string(),
        })
        .collect()
}

fn sender_name(
    peer_rx: &watch::Receiver<Vec<(UserData, iroh::EndpointAddr)>>,
    id: EndpointId,
) -> String {
    peer_rx
        .borrow()
        .iter()
        .find(|(_, address)| address.id == id)
        .map(|(name, _)| name.to_string())
        .unwrap_or_else(|| "Unknown device".to_owned())
}

#[tauri::command]
fn initial_state(state: State<'_, AppState>) -> Result<InitialState, String> {
    let peers = state
        .peers
        .lock()
        .map_err(|_| "peer state is unavailable")?
        .clone();
    let incoming_offer = state
        .pending_offer
        .lock()
        .map_err(|_| "offer state is unavailable")?
        .as_ref()
        .map(|pending| pending.view.clone());

    Ok(InitialState {
        display_name: state.display_name.clone(),
        ticket: state.ticket.clone(),
        mobile: cfg!(mobile),
        peers,
        incoming_offer,
    })
}

#[tauri::command]
fn validate_ticket(ticket: String) -> Result<Peer, String> {
    let ticket = EndpointTicket::decode_string(ticket.trim()).map_err(|_| "Invalid ticket")?;
    Ok(Peer {
        id: ticket.endpoint_addr().id.to_string(),
        name: "Ticket device".to_owned(),
        ticket: ticket.to_string(),
    })
}

#[tauri::command]
fn current_ticket(state: State<'_, AppState>) -> String {
    EndpointTicket::new(state.endpoint.addr()).to_string()
}

#[tauri::command]
fn file_name(app: AppHandle, path: String) -> Result<String, String> {
    app.path()
        .file_name(&path)
        .ok_or_else(|| "The selected file has no name".to_owned())
}

#[tauri::command]
fn respond_offer(
    app: AppHandle,
    state: State<'_, AppState>,
    id: u64,
    accept: bool,
    download_dir: Option<String>,
) -> Result<(), String> {
    let decision = if accept {
        let directory = match download_dir {
            Some(path) => PathBuf::from(path),
            None => app
                .path()
                .document_dir()
                .map_err(|error| error.to_string())?,
        };
        std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
        if !directory.is_dir() {
            return Err("The download location is not a directory".to_owned());
        }
        OfferDecision::Accept(directory)
    } else {
        OfferDecision::Decline
    };

    let pending = {
        let mut pending = state
            .pending_offer
            .lock()
            .map_err(|_| "offer state is unavailable")?;
        if pending.as_ref().is_none_or(|pending| pending.view.id != id) {
            return Err("This offer is no longer available".to_owned());
        }
        pending.take().unwrap()
    };
    let result = pending.decision.send(decision);
    state.offer_answered.notify_one();
    result.map_err(|_| "The sender stopped waiting".to_owned())
}

#[tauri::command]
async fn send_file(
    app: AppHandle,
    state: State<'_, AppState>,
    path: String,
    target_ticket: String,
) -> Result<String, String> {
    let _sending = state
        .send_lock
        .try_lock()
        .map_err(|_| "A transfer is already running")?;
    let target = EndpointTicket::decode_string(target_ticket.trim())
        .map_err(|_| "Invalid receiver ticket")?
        .endpoint_addr()
        .clone();
    let file_path = Path::new(&path);
    let metadata = tokio::fs::metadata(file_path)
        .await
        .map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err("The selected path is not a file".to_owned());
    }
    let filename = file_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| "The selected file has no name".to_owned())?;
    let total = metadata.len();
    app.emit("send-started", SendStarted { filename, total })
        .map_err(|error| error.to_string())?;

    let (progress_tx, mut progress_rx) = watch::channel(0_u64);
    let progress_app = app.clone();
    tauri::async_runtime::spawn(async move {
        while progress_rx.changed().await.is_ok() {
            let _ = progress_app.emit(
                "send-progress",
                TransferProgress {
                    downloaded: *progress_rx.borrow_and_update(),
                    total,
                },
            );
        }
    });

    match run_sender(progress_tx, &path, &state.endpoint, &state.store, target)
        .await
        .map_err(|error| error.to_string())?
    {
        SendOutcome::Completed => Ok("completed".to_owned()),
        SendOutcome::Declined => Ok("declined".to_owned()),
    }
}

async fn forward_peers(
    app: AppHandle,
    mut peer_rx: watch::Receiver<Vec<(UserData, iroh::EndpointAddr)>>,
    peers: Arc<Mutex<Vec<Peer>>>,
) {
    while peer_rx.changed().await.is_ok() {
        let current = peer_list(&peer_rx.borrow_and_update());
        if let Ok(mut stored) = peers.lock() {
            *stored = current.clone();
        }
        let _ = app.emit("peers", current);
    }
}

async fn forward_offers(
    app: AppHandle,
    mut offer_rx: mpsc::Receiver<OfferRequest>,
    peer_rx: watch::Receiver<Vec<(UserData, iroh::EndpointAddr)>>,
    state: AppState,
) {
    let mut next_id = 0_u64;
    while let Some((offer, decision)) = offer_rx.recv().await {
        next_id += 1;
        let view = IncomingOffer {
            id: next_id,
            filename: offer.filename,
            filesize: offer.filesize,
            sender: sender_name(&peer_rx, offer.ticket.addr().id),
        };
        if let Ok(mut pending) = state.pending_offer.lock() {
            *pending = Some(PendingOffer {
                view: view.clone(),
                decision,
            });
        }
        let _ = app.emit("incoming-offer", view);

        if tokio::time::timeout(Duration::from_secs(60), state.offer_answered.notified())
            .await
            .is_err()
        {
            if let Ok(mut pending) = state.pending_offer.lock()
                && let Some(pending) = pending.take()
            {
                let _ = pending.decision.send(OfferDecision::Decline);
            }
            let _ = app.emit("offer-expired", next_id);
        }
    }
}

async fn forward_receive_progress(app: AppHandle, mut progress_rx: watch::Receiver<u64>) {
    while progress_rx.changed().await.is_ok() {
        let _ = app.emit("receive-progress", *progress_rx.borrow_and_update());
    }
}

pub fn run(runtime: Runtime) -> Result<()> {
    let Runtime {
        endpoint,
        store,
        router,
        ticket,
        offer_rx,
        peer_rx,
        progress_rx,
        ..
    } = runtime;
    let peers = Arc::new(Mutex::new(peer_list(&peer_rx.borrow())));
    let state = AppState {
        endpoint,
        store,
        ticket: ticket.to_string(),
        display_name: whoami::devicename().or_else(|_| whoami::hostname())?,
        peers: peers.clone(),
        pending_offer: Arc::new(Mutex::new(None)),
        offer_answered: Arc::new(Notify::new()),
        send_lock: Arc::new(AsyncMutex::new(())),
    };
    let task_state = state.clone();

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            initial_state,
            validate_ticket,
            current_ticket,
            file_name,
            respond_offer,
            send_file
        ])
        .setup(move |app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(forward_peers(handle.clone(), peer_rx.clone(), peers));
            tauri::async_runtime::spawn(forward_offers(
                handle.clone(),
                offer_rx,
                peer_rx,
                task_state,
            ));
            tauri::async_runtime::spawn(forward_receive_progress(handle, progress_rx));
            Ok(())
        })
        .run(tauri::generate_context!())
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    tauri::async_runtime::block_on(router.shutdown())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::endpoint::presets;

    #[tokio::test]
    async fn exposes_peer_as_reusable_ticket() -> Result<()> {
        let endpoint = Endpoint::bind(presets::Minimal).await?;
        let peers = peer_list(&[(UserData::try_from("Phone".to_owned())?, endpoint.addr())]);

        assert_eq!(peers[0].name, "Phone");
        assert_eq!(
            EndpointTicket::decode_string(&peers[0].ticket)?
                .endpoint_addr()
                .id,
            endpoint.id()
        );
        endpoint.close().await;
        Ok(())
    }
}
