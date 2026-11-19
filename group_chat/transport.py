"""Reliable FIFO overlay multicast over UDP.

Evolution of Part 1's ACK/retransmission/holdback protocol, with JSON, a delivery
queue, configurable addresses and a continuous lifecycle. A stream is identified
by (epoch, sender, destination): unicast requests cannot leave gaps in broadcasts.
ACK means receipt into volatile memory, not application commit or durable storage.
"""

import json
import queue
import random
import socket
import threading
import time
from collections import Counter
from dataclasses import dataclass

from .config import Config

MAX_PACKET = 16384


@dataclass
class Pending:
    packet: bytes
    release_at: float
    last_sent: float | None = None


class ReliableFifoTransport:
    def __init__(self, config: Config, peer_id: str, bind_host=None):
        self.config = config
        self.peer_id = peer_id
        self.addresses = {m.id: (socket.gethostbyname(m.host), m.udp_port)
                          for m in config.members}
        member = config.member(peer_id)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.socket.bind((bind_host or member.host, member.udp_port))
        except BaseException:
            self.socket.close()
            raise
        self.socket.settimeout(min(config.rto / 4, 0.04))
        self.deliveries = queue.Queue()
        self.lock = threading.RLock()
        self.closed = threading.Event()
        self.next_seq = Counter()
        self.expected = Counter()
        self.buffers = {m.id: {} for m in config.members}
        self.pending = {}
        self.loss = config.loss
        self.delays = {}
        self.random = random.Random()
        self.stats = Counter()
        self.error = None
        self.thread = threading.Thread(target=self._run, name=f"udp-{peer_id}", daemon=True)
        self.thread.start()

    def send(self, destination, payload):
        with self.lock:
            if self.closed.is_set():
                raise RuntimeError("Transporte encerrado.")
            if destination not in self.addresses:
                raise ValueError("Destino desconhecido.")
            seq = self.next_seq[destination]
            packet = json.dumps({"kind": "DATA", "epoch": self.config.epoch,
                                 "sender": self.peer_id, "destination": destination,
                                 "seq": seq, "payload": payload},
                                ensure_ascii=False, separators=(",", ":")).encode()
            if len(packet) > MAX_PACKET:
                raise ValueError("Comando excede o tamanho máximo do datagrama.")
            self.next_seq[destination] += 1
            self.pending[destination, seq] = Pending(
                packet, time.monotonic() + self.delays.get(destination, 0) / 1000)
            return seq

    def multicast(self, payload):
        # Includes self: local commands follow exactly the same delivery path.
        with self.lock:
            for destination in self.addresses:
                self.send(destination, payload)

    def configure_network(self, loss=None, delays=None):
        with self.lock:
            if loss is not None:
                if type(loss) not in (int, float) or not 0 <= loss <= 0.8:
                    raise ValueError("Perda deve estar entre 0 e 0.8.")
            if delays is not None:
                if not isinstance(delays, dict) or any(
                    dest not in self.addresses or type(ms) not in (int, float)
                    or not 0 <= ms <= 10000 for dest, ms in delays.items()
                ):
                    raise ValueError("Atrasos devem mapear participantes para 0..10000 ms.")
            if loss is not None:
                self.loss = float(loss)
            if delays is not None:
                self.delays = dict(delays)

    def snapshot(self):
        with self.lock:
            return {**dict(self.stats), "pending_acks": len(self.pending),
                    "buffered": sum(len(b) for b in self.buffers.values()),
                    "queued_deliveries": self.deliveries.unfinished_tasks,
                    "expected": dict(self.expected), "loss": self.loss,
                    "delays": dict(self.delays), "error": self.error}

    def close(self):
        self.closed.set()
        self.thread.join(timeout=2)
        self.socket.close()

    def _run(self):
        try:
            while not self.closed.is_set():
                try:
                    raw, address = self.socket.recvfrom(MAX_PACKET + 1)
                except socket.timeout:
                    pass
                else:
                    with self.lock:
                        if self.random.random() < self.loss:
                            self.stats["dropped"] += 1
                        else:
                            self._receive(raw, address)
                self._flush()
        except OSError as exc:
            if not self.closed.is_set():
                self.error = str(exc)

    def _receive(self, raw, address):
        try:
            msg = json.loads(raw)
            if not isinstance(msg, dict) or len(raw) > MAX_PACKET:
                raise ValueError
            sender, seq = msg["sender"], msg["seq"]
            if (msg["epoch"] != self.config.epoch or msg["destination"] != self.peer_id
                    or sender not in self.addresses or address != self.addresses[sender]
                    or type(seq) is not int or seq < 0):
                raise ValueError
            if msg["kind"] == "ACK":
                self.pending.pop((sender, seq), None)
                self.stats["acks_received"] += 1
                return
            if msg["kind"] != "DATA" or not isinstance(msg["payload"], dict):
                raise ValueError
        except (ValueError, KeyError, TypeError, UnicodeDecodeError):
            self.stats["invalid"] += 1
            return
        ack = {"kind": "ACK", "epoch": self.config.epoch, "sender": self.peer_id,
               "destination": sender, "seq": seq}
        self.socket.sendto(json.dumps(ack).encode(), self.addresses[sender])
        self.stats["received"] += 1
        if seq < self.expected[sender] or seq in self.buffers[sender]:
            self.stats["duplicates"] += 1
            return
        self.buffers[sender][seq] = msg["payload"]
        while self.expected[sender] in self.buffers[sender]:
            payload = self.buffers[sender].pop(self.expected[sender])
            self.deliveries.put((sender, self.expected[sender], payload))
            self.expected[sender] += 1
            self.stats["delivered"] += 1

    def _flush(self):
        now = time.monotonic()
        with self.lock:
            for (dest, _), pending in self.pending.items():
                if now < pending.release_at:
                    continue
                if pending.last_sent is not None:
                    if now - pending.last_sent < self.config.rto:
                        continue
                    self.stats["retransmitted"] += 1
                self.socket.sendto(pending.packet, self.addresses[dest])
                pending.last_sent = now
                self.stats["sent"] += 1
