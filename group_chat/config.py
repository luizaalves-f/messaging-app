"""Explicit membership shared by the dashboard and the independent peer processes."""

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Member:
    id: str
    name: str
    host: str
    udp_port: int
    http_port: int


@dataclass(frozen=True)
class Config:
    epoch: str
    mode: str
    members: tuple[Member, ...]
    loss: float = 0.0
    rto: float = 0.15

    def __post_init__(self):
        if self.mode not in {"fifo", "total"}:
            raise ValueError("Modo deve ser fifo ou total.")
        if not isinstance(self.epoch, str) or not self.epoch:
            raise ValueError("A sessão precisa de um epoch não vazio.")
        if not 2 <= len(self.members) <= 10:
            raise ValueError("Configure de 2 a 10 participantes.")
        if not 0 <= self.loss <= 0.8 or not 0.02 <= self.rto <= 5:
            raise ValueError("Perda deve estar entre 0 e 0.8; RTO entre 0.02 e 5 s.")
        ids = [m.id for m in self.members]
        if any(
            not isinstance(i, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,39}", i) for i in ids
        ) or len(set(ids)) != len(ids):
            raise ValueError("IDs devem ser únicos: letra minúscula seguida de letras, números, '_' ou '-' (até 40).")
        addresses = [(m.host, m.udp_port) for m in self.members]
        http = [(m.host, m.http_port) for m in self.members]
        if len(set(addresses)) != len(addresses) or len(set(http)) != len(http):
            raise ValueError("Cada participante precisa de endereços UDP e HTTP distintos.")
        for m in self.members:
            if not m.host or not m.name or any(type(p) is not int or not 1 <= p <= 65535
                                             for p in (m.udp_port, m.http_port)):
                raise ValueError("Endereço, nome ou porta inválidos.")

    @property
    def sequencer(self):
        return self.members[0].id

    def member(self, peer_id):
        return next(m for m in self.members if m.id == peer_id)

    def to_dict(self):
        return asdict(self)

    @classmethod
    def load(cls, path):
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        data["members"] = tuple(Member(**m) for m in data["members"])
        return cls(**data)
