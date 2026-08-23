const { core, dialog, event, fs, path: tauriPath, webviewWindow } = window.__TAURI__;
const { invoke } = core;

const elements = {
  content: document.querySelector("#content"),
  title: document.querySelector("#title"),
  subtitle: document.querySelector("#subtitle"),
  radar: document.querySelector("#radar"),
  back: document.querySelector("#back"),
  headerActions: document.querySelector("#header-actions"),
  copyTicket: document.querySelector("#copy-ticket"),
  useTicket: document.querySelector("#use-ticket"),
  ticketDialog: document.querySelector("#ticket-dialog"),
  ticketInput: document.querySelector("#ticket-input"),
  ticketContinue: document.querySelector("#ticket-continue"),
  ticketError: document.querySelector("#ticket-error"),
  offerDialog: document.querySelector("#offer-dialog"),
  offerSender: document.querySelector("#offer-sender"),
  offerName: document.querySelector("#offer-name"),
  offerSize: document.querySelector("#offer-size"),
  offerError: document.querySelector("#offer-error"),
  acceptOffer: document.querySelector("#accept-offer"),
  declineOffer: document.querySelector("#decline-offer"),
  dropOverlay: document.querySelector("#drop-overlay"),
  toast: document.querySelector("#toast"),
};

const state = {
  displayName: "",
  ticket: "",
  mobile: false,
  peers: [],
  selectedPeer: null,
  transfer: null,
  receive: null,
  incomingOffer: null,
};

const icon = (kind) => {
  const paths = {
    devices: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="5" width="14" height="11" rx="2"/><path d="M8 20h4m-2-4v4m9-8h2v7h-6v-1"/></svg>',
    upload: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 16V4m0 0L7 9m5-5 5 5"/><path d="M5 15v5h14v-5"/></svg>',
    success: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m5 12 4 4L19 6"/></svg>',
    warning: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6 6 18"/></svg>',
    failure: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 8v5m0 3h.01"/><circle cx="12" cy="12" r="9"/></svg>',
    receive: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 4v12m0 0 5-5m-5 5-5-5"/><path d="M5 20h14"/></svg>',
  };
  return paths[kind];
};

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 1000) return `${bytes || 0} B`;
  const units = ["KB", "MB", "GB", "TB"];
  const unit = Math.min(Math.floor(Math.log(bytes) / Math.log(1000)) - 1, units.length - 1);
  return `${(bytes / 1000 ** (unit + 1)).toFixed(unit ? 1 : 0)} ${units[unit]}`;
}

function progress(downloaded, total) {
  return total > 0 ? Math.min(100, Math.round((downloaded / total) * 100)) : 0;
}

function showToast(message) {
  elements.toast.textContent = message;
  elements.toast.hidden = false;
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => (elements.toast.hidden = true), 1800);
}

function setHeader() {
  const selected = state.selectedPeer;
  elements.title.textContent = selected ? "Sending" : "Nearby";
  elements.subtitle.textContent = selected ? `To “${selected.name}”` : `As “${state.displayName}”`;
  elements.radar.hidden = Boolean(selected);
  elements.back.hidden = !selected;
  elements.back.disabled = state.transfer?.status === "sending";
  elements.headerActions.hidden = Boolean(selected);
}

function button(label, onClick) {
  const element = document.createElement("button");
  element.className = "button primary";
  element.type = "button";
  element.textContent = label;
  element.addEventListener("click", onClick);
  return element;
}

function renderPeers() {
  if (!state.peers.length) {
    elements.content.innerHTML = `<div class="empty"><span class="empty-icon">${icon("devices")}</span><h2>No devices found</h2><p>Keep both devices nearby and on the same network.</p></div>`;
    return;
  }

  const list = document.createElement("div");
  list.className = "peer-list";
  list.setAttribute("aria-label", "Nearby devices");
  for (const peer of state.peers) {
    const item = document.createElement("button");
    item.className = "peer";
    item.type = "button";
    item.setAttribute("aria-label", `Send to ${peer.name}`);
    const avatar = document.createElement("span");
    avatar.className = "avatar";
    avatar.textContent = peer.name.trim().charAt(0).toUpperCase() || "?";
    const name = document.createElement("span");
    name.className = "peer-name";
    name.textContent = peer.name;
    item.append(avatar, name);
    item.addEventListener("click", () => {
      state.selectedPeer = peer;
      state.transfer = null;
      render();
    });
    list.append(item);
  }
  elements.content.replaceChildren(list);
}

function renderPicker() {
  elements.content.innerHTML = `<div class="drop-zone"><span class="upload-icon">${icon("upload")}</span><h2>Choose a file</h2><p>${state.mobile ? "Select a file from your device." : "Drop a file here or select one from your device."}</p></div>`;
  elements.content.querySelector(".drop-zone").append(button("Choose file", chooseFile));
}

function renderProgress(container, item, verb) {
  const percent = progress(item.downloaded, item.total);
  const labels = document.createElement("div");
  labels.className = "progress-label";
  const detail = document.createElement("span");
  detail.textContent = `${formatBytes(item.downloaded)} of ${formatBytes(item.total)}`;
  const percentage = document.createElement("span");
  percentage.textContent = `${percent}%`;
  labels.append(detail, percentage);
  const bar = document.createElement("progress");
  bar.max = 100;
  bar.value = percent;
  bar.setAttribute("aria-label", `${verb} ${percent}%`);
  container.append(labels, bar);
}

function renderTransfer() {
  const transfer = state.transfer;
  const container = document.createElement("div");
  container.className = "status";

  if (transfer.status === "sending") {
    container.innerHTML = `<span class="status-icon">${icon("upload")}</span><h2>Transferring</h2>`;
    const filename = document.createElement("p");
    filename.textContent = transfer.filename;
    container.append(filename);
    renderProgress(container, transfer, "Sending");
  } else {
    const copy = {
      completed: ["success", "Transfer complete", `“${transfer.filename}” was sent successfully.`, "Send another file"],
      declined: ["warning", "Transfer declined", "The receiver declined this file.", "Choose another file"],
      failed: ["failure", "Transfer failed", transfer.error || "The file could not be sent.", "Try another file"],
    }[transfer.status];
    container.innerHTML = `<span class="status-icon ${copy[0]}">${icon(copy[0])}</span><h2>${copy[1]}</h2>`;
    const detail = document.createElement("p");
    detail.textContent = copy[2];
    container.append(detail, button(copy[3], () => {
      state.transfer = null;
      render();
    }));
  }
  elements.content.replaceChildren(container);
}

function renderReceive() {
  const receive = state.receive;
  const container = document.createElement("div");
  container.className = "receive";
  container.innerHTML = `<span class="status-icon ${receive.complete ? "success" : ""}">${icon(receive.complete ? "success" : "receive")}</span><h2>${receive.complete ? "Received" : "Receiving"}</h2>`;
  const filename = document.createElement("p");
  filename.textContent = receive.complete ? `“${receive.filename}” was saved.` : receive.filename;
  container.append(filename);
  if (!receive.complete) renderProgress(container, receive, "Receiving");
  elements.content.replaceChildren(container);
}

function render() {
  setHeader();
  if (state.receive) renderReceive();
  else if (!state.selectedPeer) renderPeers();
  else if (!state.transfer) renderPicker();
  else renderTransfer();
}

async function materializeFile(selectedPath) {
  const filename = await invoke("file_name", { path: selectedPath });
  if (selectedPath.startsWith("file://")) {
    return { path: decodeURIComponent(new URL(selectedPath).pathname), filename, cleanup: null };
  }
  if (!selectedPath.startsWith("content://")) {
    return { path: selectedPath, filename, cleanup: null };
  }

  const staging = await tauriPath.join(await tauriPath.tempDir(), `iroh-share-${crypto.randomUUID()}`);
  await fs.mkdir(staging);
  const localPath = await tauriPath.join(staging, filename);
  await fs.copyFile(selectedPath, localPath);
  return { path: localPath, filename, cleanup: staging };
}

async function sendSelected(selectedPath) {
  if (!state.selectedPeer || state.transfer?.status === "sending") return;
  let selected;
  try {
    selected = await materializeFile(selectedPath);
    state.transfer = { status: "sending", filename: selected.filename, downloaded: 0, total: 0 };
    render();
    const outcome = await invoke("send_file", {
      path: selected.path,
      targetTicket: state.selectedPeer.ticket,
    });
    state.transfer.status = outcome;
  } catch (error) {
    state.transfer ??= { filename: "Selected file", downloaded: 0, total: 0 };
    state.transfer.status = "failed";
    state.transfer.error = String(error);
    console.error(error);
  } finally {
    if (selected?.cleanup) await fs.remove(selected.cleanup, { recursive: true }).catch(console.error);
    render();
  }
}

async function chooseFile() {
  const selected = await dialog.open({
    multiple: false,
    directory: false,
    fileAccessMode: "copy",
    pickerMode: "document",
  });
  if (selected) await sendSelected(selected);
}

function showIncoming(offer) {
  state.incomingOffer = offer;
  elements.offerSender.textContent = `From “${offer.sender}”`;
  elements.offerName.textContent = offer.filename;
  elements.offerSize.textContent = formatBytes(offer.filesize);
  elements.offerError.textContent = "";
  if (!elements.offerDialog.open) elements.offerDialog.showModal();
}

async function answerOffer(accept) {
  const offer = state.incomingOffer;
  if (!offer) return;
  try {
    let downloadDir = null;
    if (accept && !state.mobile) {
      downloadDir = await dialog.open({ directory: true, multiple: false });
      if (!downloadDir) return;
    }
    await invoke("respond_offer", { id: offer.id, accept, downloadDir });
    state.incomingOffer = null;
    elements.offerDialog.close();
    if (accept) {
      state.receive = {
        filename: offer.filename,
        downloaded: 0,
        total: offer.filesize,
        complete: false,
      };
      render();
    }
  } catch (error) {
    elements.offerError.textContent = String(error);
  }
}

async function connectEvents() {
  await Promise.all([
    event.listen("peers", ({ payload }) => {
      state.peers = payload;
      if (state.selectedPeer && !state.selectedPeer.manual && !payload.some((peer) => peer.id === state.selectedPeer.id)) {
        state.selectedPeer = null;
        state.transfer = null;
      }
      render();
    }),
    event.listen("incoming-offer", ({ payload }) => showIncoming(payload)),
    event.listen("offer-expired", ({ payload }) => {
      if (state.incomingOffer?.id === payload) {
        state.incomingOffer = null;
        elements.offerDialog.close();
        showToast("Offer expired");
      }
    }),
    event.listen("send-started", ({ payload }) => {
      if (state.transfer) Object.assign(state.transfer, payload);
      render();
    }),
    event.listen("send-progress", ({ payload }) => {
      if (state.transfer?.status === "sending") Object.assign(state.transfer, payload);
      render();
    }),
    event.listen("receive-progress", ({ payload }) => {
      if (!state.receive) return;
      state.receive.downloaded = payload;
      if (state.receive.total > 0 && payload >= state.receive.total) {
        state.receive.complete = true;
        setTimeout(() => {
          state.receive = null;
          render();
        }, 2000);
      }
      render();
    }),
  ]);
}

elements.back.addEventListener("click", () => {
  if (state.transfer?.status === "sending") return;
  state.selectedPeer = null;
  state.transfer = null;
  render();
});

elements.copyTicket.addEventListener("click", async () => {
  try {
    state.ticket = await invoke("current_ticket");
    await navigator.clipboard.writeText(state.ticket);
    showToast("Ticket copied");
  } catch {
    showToast("Could not copy ticket");
  }
});

elements.useTicket.addEventListener("click", () => {
  elements.ticketInput.value = "";
  elements.ticketError.textContent = "";
  elements.ticketDialog.showModal();
  setTimeout(() => elements.ticketInput.focus(), 0);
});

elements.ticketContinue.addEventListener("click", async (event) => {
  event.preventDefault();
  try {
    const peer = await invoke("validate_ticket", { ticket: elements.ticketInput.value });
    state.selectedPeer = { ...peer, manual: true };
    state.transfer = null;
    elements.ticketDialog.close();
    render();
  } catch (error) {
    elements.ticketError.textContent = String(error);
  }
});

elements.acceptOffer.addEventListener("click", (event) => {
  event.preventDefault();
  answerOffer(true);
});
elements.declineOffer.addEventListener("click", (event) => {
  event.preventDefault();
  answerOffer(false);
});
elements.offerDialog.addEventListener("cancel", (event) => event.preventDefault());

webviewWindow.getCurrentWebviewWindow().onDragDropEvent(({ payload }) => {
  if (!state.selectedPeer || state.transfer?.status === "sending") return;
  elements.dropOverlay.hidden = payload.type !== "over" && payload.type !== "enter";
  if (payload.type === "drop") {
    elements.dropOverlay.hidden = true;
    if (payload.paths[0]) sendSelected(payload.paths[0]);
  }
  if (payload.type === "leave") elements.dropOverlay.hidden = true;
});

async function start() {
  try {
    await connectEvents();
    const initial = await invoke("initial_state");
    Object.assign(state, initial);
    render();
    if (initial.incomingOffer) showIncoming(initial.incomingOffer);
  } catch (error) {
    elements.content.innerHTML = `<div class="empty"><h2>Could not start</h2><p></p></div>`;
    elements.content.querySelector("p").textContent = String(error);
  }
}

start();
