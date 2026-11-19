"""Deterministic replicated conversation; no sockets, clocks or local randomness."""

import copy
import hashlib
import json


def validate_command(command, members):
    if not isinstance(command, dict):
        raise ValueError("Comando deve ser um objeto.")
    kind = command.get("kind")
    fields = {"send": {"id", "author", "kind", "text", "reply_to"},
              "edit": {"id", "author", "kind", "text", "message_id"},
              "delete": {"id", "author", "kind", "message_id"}}
    if not isinstance(kind, str) or kind not in fields or set(command) != fields[kind]:
        raise ValueError("Tipo ou campos do comando inválidos.")
    if not isinstance(command["author"], str) or command["author"] not in members:
        raise ValueError("Autor desconhecido.")
    for field in ("id", "message_id"):
        if field in command and (not isinstance(command[field], str)
                                 or not 1 <= len(command[field]) <= 80):
            raise ValueError("Identificador inválido.")
    if not command["id"].startswith(command["author"] + ":"):
        raise ValueError("O ID do comando deve identificar seu autor.")
    if kind in {"send", "edit"} and (not isinstance(command["text"], str)
                                    or not command["text"].strip()
                                    or len(command["text"]) > 1000):
        raise ValueError("Texto deve ter entre 1 e 1000 caracteres.")
    if kind == "send" and command["reply_to"] is not None and (
        not isinstance(command["reply_to"], str) or not 1 <= len(command["reply_to"]) <= 80
        or command["reply_to"] == command["id"]
    ):
        raise ValueError("Referência de resposta inválida.")


class Conversation:
    def __init__(self, members):
        self.members = set(members)
        self.messages = {}
        self.order = []
        self.events = []
        self.results = {}
        self.causal_violations = []

    def apply(self, command, global_seq=None):
        validate_command(command, self.members)
        command_id = command["id"]
        if command_id in self.results:
            return self.results[command_id]
        accepted, reason = True, None
        kind = command["kind"]
        if kind == "send":
            reply_to = command["reply_to"]
            if reply_to and reply_to not in self.messages:
                self.causal_violations.append({"message_id": command_id,
                                               "missing": reply_to,
                                               "position": len(self.events) + 1})
            self.messages[command_id] = {
                "id": command_id, "author": command["author"], "text": command["text"],
                "reply_to": reply_to, "edited": False, "deleted": False,
                "global_seq": global_seq,
            }
            self.order.append(command_id)
        else:
            message = self.messages.get(command["message_id"])
            if message is None:
                accepted, reason = False, "Mensagem ainda não existe nesta réplica."
            elif message["author"] != command["author"]:
                accepted, reason = False, "Somente o autor pode alterar a mensagem."
            elif message["deleted"]:
                accepted, reason = False, "Mensagem já foi apagada."
            elif kind == "edit":
                message.update(text=command["text"], edited=True)
            else:
                message.update(text="", deleted=True)
        result = {"id": command_id, "accepted": accepted, "reason": reason,
                  "global_seq": global_seq}
        self.results[command_id] = result
        self.events.append({"position": len(self.events) + 1, "command": copy.deepcopy(command),
                            **result})
        return result

    def snapshot(self):
        messages = [copy.deepcopy(self.messages[i]) for i in self.order]
        # Includes order AND decisions; matching text alone is insufficient.
        canonical = {"messages": messages, "events": self.events,
                     "causal_violations": self.causal_violations}
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True,
                                          ensure_ascii=False).encode()).hexdigest()
        return {**copy.deepcopy(canonical), "digest": digest,
                "event_ids": [e["id"] for e in self.events]}
