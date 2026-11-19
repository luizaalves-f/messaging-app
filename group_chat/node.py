"""A peer owns its conversation. HTTP is control/UI; peer traffic is UDP only."""

import copy
import queue
import threading
import uuid

from .config import Config
from .model import Conversation, validate_command
from .transport import ReliableFifoTransport


class ChatNode:
    def __init__(self, config: Config, peer_id: str, bind_host=None):
        self.config = config
        self.peer_id = peer_id
        self.member = config.member(peer_id)
        self.lock = threading.RLock()
        self.conversation = Conversation(m.id for m in config.members)
        self.submitted = {}
        self.sequenced = {}
        self.next_global = 1
        self.expected_global = 1
        self.closed = threading.Event()
        self.error = None
        self.transport = ReliableFifoTransport(config, peer_id, bind_host)
        self.thread = threading.Thread(target=self._consume, name=f"application-{peer_id}",
                                       daemon=True)
        self.thread.start()

    def submit(self, request):
        if not isinstance(request, dict):
            raise ValueError("Pedido deve ser um objeto.")
        kind = request.get("kind", "send")
        allowed = {"kind", "id", "text", "reply_to", "message_id"}
        if set(request) - allowed:
            raise ValueError("Campos desconhecidos no pedido.")
        command = {"kind": kind, "id": request.get("id", f"{self.peer_id}:{uuid.uuid4().hex}"),
                   "author": self.peer_id}
        if kind == "send":
            command.update(text=request.get("text"), reply_to=request.get("reply_to"))
        elif kind == "edit":
            command.update(text=request.get("text"), message_id=request.get("message_id"))
        elif kind == "delete":
            command["message_id"] = request.get("message_id")
        validate_command(command, self.conversation.members)
        if not command["id"].startswith(self.peer_id + ":"):
            raise ValueError("O ID do pedido deve começar com o ID do autor seguido de ':'.")
        with self.lock:
            if command["id"] in self.submitted:
                if self.submitted[command["id"]] != command:
                    raise ValueError("ID já usado por outro comando.")
                return {"id": command["id"], "status": "submitted"}
            if kind == "send" and command["reply_to"]:
                if command["reply_to"] not in self.conversation.messages:
                    raise ValueError("Só é possível responder a mensagens já confirmadas.")
            elif kind in {"edit", "delete"}:
                message = self.conversation.messages.get(command["message_id"])
                if message is None or message["author"] != self.peer_id:
                    raise ValueError("Selecione uma mensagem confirmada de sua autoria.")
                if message["deleted"]:
                    raise ValueError("Mensagem já foi apagada.")
            if self.config.mode == "total":
                self.transport.send(self.config.sequencer, {"type": "PROPOSE", "command": command})
            else:
                self.transport.multicast({"type": "FIFO", "command": command})
            self.submitted[command["id"]] = command
            return {"id": command["id"], "status": "submitted"}

    def snapshot(self):
        with self.lock:
            conversation = self.conversation.snapshot()
            pending = [copy.deepcopy(c) for i, c in self.submitted.items()
                       if i not in self.conversation.results]
            network = self.transport.snapshot()
            return {"id": self.peer_id, "name": self.member.name, "epoch": self.config.epoch,
                    "mode": self.config.mode, "sequencer": self.config.sequencer,
                    **conversation, "pending": pending, "network": network,
                    "error": self.error or network["error"],
                    "idle": not pending and not network["pending_acks"]
                    and not network["buffered"] and not network["queued_deliveries"]}

    def close(self):
        self.closed.set()
        self.thread.join(timeout=2)
        self.transport.close()

    def _consume(self):
        while not self.closed.is_set():
            try:
                sender, _, envelope = self.transport.deliveries.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                with self.lock:
                    self._deliver(sender, envelope)
            except (ValueError, KeyError, TypeError) as exc:
                # Invalid commands cannot take down the receiver thread.
                with self.lock:
                    self.error = f"Comando de rede rejeitado: {exc}"
            finally:
                self.transport.deliveries.task_done()

    def _deliver(self, sender, envelope):
        kind = envelope.get("type")
        command = envelope.get("command")
        validate_command(command, self.conversation.members)
        if self.config.mode == "fifo":
            if kind != "FIFO" or command["author"] != sender:
                raise ValueError("Origem do comando FIFO inválida.")
            self.conversation.apply(command)
        elif kind == "PROPOSE" and self.peer_id == self.config.sequencer:
            if command["author"] != sender or not command["id"].startswith(sender + ":"):
                raise ValueError("Autor da proposta inválido.")
            previous = self.sequenced.get(command["id"])
            if previous is not None:
                if previous != command:
                    raise ValueError("ID de proposta já usado com outro conteúdo.")
                return
            self.transport.multicast({"type": "ORDER", "global_seq": self.next_global,
                                      "command": command})
            self.sequenced[command["id"]] = command
            self.next_global += 1
        elif kind == "ORDER" and sender == self.config.sequencer:
            seq = envelope.get("global_seq")
            if type(seq) is not int or seq != self.expected_global:
                raise ValueError("Sequência global descontínua.")
            self.conversation.apply(command, seq)
            self.expected_global += 1
        else:
            raise ValueError("Envelope incompatível com o modo ou sequenciador.")
