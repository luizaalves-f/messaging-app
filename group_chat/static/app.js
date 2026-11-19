"use strict";

const $ = (selector, root = document) => root.querySelector(selector);
const escapeHTML = value => String(value ?? "").replace(/[&<>"']/g, char => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
}[char]));
let snapshot = null;
let epoch = null;
let busy = false;
let editing = null;
const contexts = new Map();
const signatures = new Map();
const retries = new Map();

async function api(path, data) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch(path, {
      method: data === undefined ? "GET" : "POST",
      headers: { "Content-Type": "application/json" },
      body: data === undefined ? undefined : JSON.stringify(data),
      signal: controller.signal,
    });
    const body = await response.json();
    if (!response.ok) throw new Error(body.error || "Não foi possível concluir o pedido.");
    return body;
  } finally { clearTimeout(timer); }
}

function showError(message) {
  $("#error").textContent = message;
  $("#error").hidden = !message;
}

function controls() {
  const locked = busy || Boolean(snapshot?.demo.running);
  for (const id of ["reset", "mode", "concurrent", "causal"]) {
    $("#" + id).disabled = locked || !snapshot?.managed;
  }
  const modePending = snapshot && $("#mode").value !== snapshot.config.mode;
  for (const id of ["concurrent", "causal"]) {
    $("#" + id).disabled ||= Boolean(modePending);
    $("#" + id).title = modePending ? "Clique em Nova sessão para aplicar o modo selecionado." : "";
  }
  $("#loss").disabled = locked || !snapshot;
  for (const form of document.querySelectorAll(".composer")) {
    const state = snapshot?.states.find(s => s.id === form.dataset.peer);
    const disabled = locked || !state || Boolean(state.unavailable || state.error);
    for (const input of form.querySelectorAll("textarea, button")) input.disabled = disabled;
  }
  if (snapshot) {
    const note = snapshot.config.mode === "total"
      ? `Ordem total: ${snapshot.config.members[0].name} sequencia os comandos. A confirmação ocorre após a entrega ordenada na réplica local.`
      : "FIFO preserva a ordem de cada remetente. A ordem entre participantes e as relações de resposta podem divergir.";
    $("#mode-note").textContent = note + (modePending
      ? " Seleção pendente: clique em Nova sessão para aplicar o outro modo e limpar o histórico." : "");
  }
}

function createCards() {
  $("#replicas").innerHTML = snapshot.config.members.map((member, index) => `
    <article class="replica" data-peer="${escapeHTML(member.id)}" aria-label="Réplica de ${escapeHTML(member.name)}">
      <header class="replica-header"><span class="avatar" aria-hidden="true">${escapeHTML(member.name.slice(0, 1))}</span>
        <div class="identity"><h2>${escapeHTML(member.name)}</h2><p>Réplica independente · P${index}</p></div>
        <span class="badge">${snapshot.config.mode === "total" && index === 0 ? "SEQUENCIADOR" : "PARTICIPANTE"}</span>
      </header>
      <div class="context-warning" hidden></div>
      <div class="chat" role="log" aria-live="polite" aria-label="Mensagens na réplica de ${escapeHTML(member.name)}"></div>
      <form class="composer" data-peer="${escapeHTML(member.id)}">
        <div class="compose-context" hidden><span></span><button type="button" data-action="cancel-reply" aria-label="Cancelar resposta">×</button></div>
        <div class="compose-line"><textarea rows="1" maxlength="1000" aria-label="Mensagem de ${escapeHTML(member.name)}" placeholder="Escreva como ${escapeHTML(member.name)}…" required></textarea>
          <button type="submit" class="button primary send-button" aria-label="Enviar como ${escapeHTML(member.name)}">↑</button></div>
        <p class="compose-hint">Enter envia · Shift + Enter quebra a linha · até 1.000 caracteres</p>
      </form>
      <div class="replica-footer"><span class="message-count">0 mensagens</span><span class="replica-hash mono">hash —</span></div>
    </article>`).join("");
  const count = snapshot.config.members.length;
  $("#replicas").style.gridTemplateColumns = count === 3 ? "" : `repeat(${Math.min(count, 3)}, minmax(0, 1fr))`;
}

function authorName(id) { return snapshot.config.members.find(m => m.id === id)?.name || id; }
function cardFor(id) { return [...document.querySelectorAll(".replica")].find(c => c.dataset.peer === id); }

function messageHTML(message, state) {
  const own = message.author === state.id;
  const original = state.messages.find(m => m.id === message.reply_to);
  const quote = !message.reply_to ? "" : `<div class="quote ${original ? "" : "missing"}">${original
    ? `<strong>${escapeHTML(authorName(original.author))}</strong><br>${escapeHTML(original.deleted ? "Mensagem apagada" : original.text)}`
    : "A mensagem original ainda não chegou a esta réplica."}</div>`;
  const actions = message.pending ? "" : `<div class="message-actions">
    <button data-action="reply" data-id="${escapeHTML(message.id)}">Responder</button>
    ${own && !message.deleted ? `<button data-action="edit" data-id="${escapeHTML(message.id)}">Editar</button><button data-action="delete" data-id="${escapeHTML(message.id)}">Apagar</button>` : ""}</div>`;
  return `<div class="message ${own ? "own" : ""} ${message.deleted ? "deleted" : ""} ${message.pending ? "pending" : ""}">
    <div class="bubble"><div class="message-author">${escapeHTML(authorName(message.author))}</div>${quote}
      <p class="message-text">${escapeHTML(message.deleted ? "Mensagem apagada" : message.text)}</p>
      <div class="message-meta">${message.edited ? "<span>editada</span>" : ""}<span>${message.pending ? "Enviando…" : message.global_seq ? `#${message.global_seq} · confirmada` : "recebida · FIFO"}</span></div>
    </div>${actions}</div>`;
}

function renderCard(state) {
  const card = cardFor(state.id);
  if (!card) return;
  const chat = $(".chat", card);
  if (state.unavailable) {
    const sign = "offline:" + state.unavailable;
    if (signatures.get(state.id) !== sign) chat.innerHTML = `<div class="empty">Participante indisponível.<br>${escapeHTML(state.unavailable)}</div>`;
    signatures.set(state.id, sign);
    $(".replica-hash", card).textContent = "offline";
    return;
  }
  const sign = JSON.stringify([state.messages, state.pending, state.events, state.error]);
  if (signatures.get(state.id) !== sign) {
    const nearBottom = chat.scrollHeight - chat.scrollTop - chat.clientHeight < 70;
    const pending = state.pending.filter(c => c.kind === "send").map(c => ({ ...c, pending: true }));
    chat.innerHTML = [...state.messages, ...pending].map(m => messageHTML(m, state)).join("") ||
      `<div class="empty"><span class="empty-icon" aria-hidden="true">↳</span><strong>A conversa começa por aqui.</strong>Envie uma mensagem ou execute um cenário guiado para observar a rede.</div>`;
    const mutations = state.pending.filter(c => c.kind !== "send");
    if (mutations.length) chat.insertAdjacentHTML("beforeend", `<p class="subtle">${mutations.length} alteração(ões) aguardando confirmação…</p>`);
    if (nearBottom) chat.scrollTop = chat.scrollHeight;
    signatures.set(state.id, sign);
  }
  $(".message-count", card).textContent = `${state.messages.length} mensagens · ${state.events.length} operações`;
  $(".replica-hash", card).textContent = `hash ${state.digest.slice(0, 8)}`;
  $(".replica-hash", card).title = state.digest;
  const warning = $(".context-warning", card);
  warning.hidden = !state.causal_violations.length && !state.error;
  warning.textContent = state.error || `${state.causal_violations.length} resposta(s) entregue(s) antes da mensagem original. O registro da violação permanece no inspetor.`;
}

function renderInspector() {
  if (!$("#inspector").open || !snapshot) return;
  $("#inspection").innerHTML = snapshot.states.map(state => {
    if (state.unavailable) return `<section><h3>${escapeHTML(state.name)}</h3><p class="error">Indisponível</p></section>`;
    const n = state.network;
    const metrics = [["Datagramas DATA enviados", n.sent || 0], ["Retransmissões", n.retransmitted || 0],
      ["Pacotes descartados", n.dropped || 0], ["Duplicatas suprimidas", n.duplicates || 0],
      ["Aguardando ACK", n.pending_acks], ["No buffer FIFO", n.buffered],
      ["Esperando aplicação", n.queued_deliveries], ["Violações de contexto", state.causal_violations.length]];
    return `<section><h3>${escapeHTML(state.name)} <span class="mono">${escapeHTML(state.id)}</span></h3>
      <div class="metrics">${metrics.map(([name, value]) => `<span>${name}</span><strong>${value}</strong>`).join("")}</div>
      <pre>Perda: ${Math.round(n.loss * 100)}%\nAtrasos de saída (ms): ${escapeHTML(JSON.stringify(n.delays))}\nPróximo esperado por remetente: ${escapeHTML(JSON.stringify(n.expected))}</pre>
      <h3>Log de execução</h3><ol class="event-log">${state.events.map(event => `<li class="${event.accepted ? "" : "rejected"}">
        ${event.global_seq ? `global #${event.global_seq}` : `local #${event.position}`} · ${escapeHTML(event.command.author)} · ${escapeHTML(event.command.kind)}<br>
        ${escapeHTML(event.command.text || event.command.message_id)}<br><span class="subtle">${escapeHTML(event.id.slice(0, 22))} · ${event.accepted ? "aceita" : escapeHTML(event.reason)}</span></li>`).join("")}</ol>
      ${state.causal_violations.length ? `<h3>Dependências ausentes na entrega</h3><pre>${escapeHTML(JSON.stringify(state.causal_violations, null, 2))}</pre>` : ""}</section>`;
  }).join("");
}

function render() {
  if (epoch !== snapshot.config.epoch) {
    epoch = snapshot.config.epoch;
    contexts.clear(); signatures.clear(); retries.clear();
    $("#edit-dialog").close(); editing = null;
    createCards();
    $("#mode").value = snapshot.config.mode;
    // Preserve arbitrary CLI loss settings, not just the presets.
    const loss = String(snapshot.config.loss);
    if (![...$("#loss").options].some(option => option.value === loss)) {
      $("#loss").add(new Option(`${Math.round(snapshot.config.loss * 100)}% · personalizada`, loss));
    }
    $("#loss").value = loss;
  }
  $(".statusbar").dataset.status = snapshot.comparison.status;
  $("#comparison").innerHTML = `<span class="dot"></span>${escapeHTML(snapshot.comparison.label)}`;
  $("#session").textContent = `sessão ${epoch.slice(0, 8)} · ${snapshot.config.mode.toUpperCase()}`;
  $("#notice").textContent = snapshot.demo.note;
  $("#notice").hidden = !snapshot.demo.note;
  snapshot.states.forEach(renderCard);
  renderInspector(); controls();
}

async function refresh() {
  try { snapshot = await api("/api/state"); render(); }
  catch (error) { showError(`Não foi possível atualizar o painel: ${error.message}`); }
}

async function action(work) {
  if (busy || snapshot?.demo.running) return;
  busy = true; controls(); showError("");
  try { await work(); await refresh(); }
  catch (error) { showError(error.name === "AbortError" ? "Pedido sem resposta. Tente novamente; o mesmo comando será reutilizado quando possível." : error.message); }
  finally { busy = false; controls(); }
}

async function submit(peerId, command) {
  const fingerprint = JSON.stringify(command);
  const key = peerId + ":" + fingerprint;
  // getRandomValues also works on HTTP LAN origins, unlike randomUUID.
  const randomId = () => [...crypto.getRandomValues(new Uint8Array(16))]
    .map(byte => byte.toString(16).padStart(2, "0")).join("");
  const id = retries.get(key) || `${peerId}:${randomId()}`;
  retries.set(key, id);
  const result = await api(`/api/peers/${encodeURIComponent(peerId)}/command`, { ...command, id });
  retries.delete(key);
  return result;
}

$("#reset").addEventListener("click", () => action(async () => {
  await api("/api/reset", { mode: $("#mode").value, loss: Number($("#loss").value) });
}));
$("#mode").addEventListener("change", controls);
$("#loss").addEventListener("change", () => action(async () => {
  try { await api("/api/network", { loss: Number($("#loss").value) }); }
  catch (error) { $("#loss").value = String(snapshot.config.loss); throw error; }
}));
for (const scenario of ["concurrent", "causal"]) {
  $("#" + scenario).addEventListener("click", () => action(() => api("/api/demo", { scenario })));
}
$("#inspector").addEventListener("toggle", renderInspector);
$("#replicas").addEventListener("submit", event => {
  const form = event.target.closest(".composer");
  if (!form) return;
  event.preventDefault();
  const peer = form.dataset.peer;
  const input = $("textarea", form);
  if (!input.value.trim()) return;
  action(async () => {
    await submit(peer, { kind: "send", text: input.value, reply_to: contexts.get(peer) || null });
    input.value = ""; contexts.delete(peer); $(".compose-context", form).hidden = true;
    // Focus is restored after controls are re-enabled.
    setTimeout(() => input.focus(), 0);
  });
});
$("#replicas").addEventListener("keydown", event => {
  if (event.target.matches("textarea") && event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault(); event.target.closest("form").requestSubmit();
  }
});
$("#replicas").addEventListener("click", event => {
  const button = event.target.closest("button[data-action]");
  if (!button || busy || snapshot.demo.running) return;
  const card = button.closest(".replica");
  const peer = card.dataset.peer;
  if (button.dataset.action === "cancel-reply") {
    contexts.delete(peer); $(".compose-context", card).hidden = true; return;
  }
  const state = snapshot.states.find(s => s.id === peer);
  const message = state.messages.find(m => m.id === button.dataset.id);
  if (!message) return;
  if (button.dataset.action === "reply") {
    contexts.set(peer, message.id);
    $(".compose-context", card).hidden = false;
    $(".compose-context span", card).textContent = `Respondendo a ${authorName(message.author)}: ${message.deleted ? "Mensagem apagada" : message.text}`;
    $("textarea", card).focus();
  } else if (button.dataset.action === "edit") {
    editing = { peer, id: message.id };
    $("#edit-text").value = message.text; $("#edit-error").hidden = true;
    $("#edit-dialog").showModal(); $("#edit-text").focus();
  } else if (button.dataset.action === "delete") {
    action(() => submit(peer, { kind: "delete", message_id: message.id }));
  }
});
$("#edit-cancel").addEventListener("click", () => $("#edit-dialog").close());
$("#edit-form").addEventListener("submit", event => {
  event.preventDefault();
  action(async () => {
    try {
      await submit(editing.peer, { kind: "edit", message_id: editing.id, text: $("#edit-text").value });
      $("#edit-dialog").close(); editing = null;
    } catch (error) {
      $("#edit-error").textContent = error.message; $("#edit-error").hidden = false; throw error;
    }
  });
});

(async function poll() {
  await refresh();
  setTimeout(poll, 450);
})();
