"""Process launcher and read-only replica comparison; never orders chat commands."""

import dataclasses
import json
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from pathlib import Path

from .config import Config
from .http_api import JsonHandler, request_json

ROOT = Path(__file__).resolve().parent.parent
STATIC = Path(__file__).with_name("static")


def compare_replicas(states):
    available = [s for s in states if "unavailable" not in s]
    if len(available) != len(states) or any(s.get("error") for s in available):
        return {"status": "unavailable", "label": "Réplica indisponível ou com erro"}
    if not available or any(not s["idle"] for s in available):
        return {"status": "syncing", "label": "Mensagens em trânsito"}
    if len({s["epoch"] for s in available}) != 1:
        return {"status": "unavailable", "label": "Réplicas em sessões diferentes"}
    event_sets = {frozenset(s["event_ids"]) for s in available}
    if len(event_sets) != 1:
        return {"status": "syncing", "label": "Aguardando entrega em todas as réplicas"}
    if not available[0]["events"]:
        return {"status": "empty", "label": "Pronto para conversar"}
    if len({s["digest"] for s in available}) == 1:
        return {"status": "synchronized", "label": "Mesma conversa em todas as réplicas"}
    return {"status": "diverged", "label": "Mesmas operações, conversas diferentes"}


class Runtime:
    def __init__(self, config: Config, managed=True):
        self.config = config
        self.managed = managed
        self.processes = []
        self.lock = threading.RLock()
        self.temp = tempfile.TemporaryDirectory(prefix="mesma-conversa-")
        self.demo_running = False
        self.demo_note = ""
        self.closing = threading.Event()
        self.demo_thread = None

    def start(self):
        with self.lock:
            if not self.managed:
                return
            path = Path(self.temp.name) / "session.json"
            path.write_text(json.dumps(self.config.to_dict()), encoding="utf-8")
            try:
                for member in self.config.members:
                    log = (Path(self.temp.name) / f"peer-{member.id}.log").open("w+")
                    process = subprocess.Popen(
                        [sys.executable, "-m", "group_chat", "peer", "--config", str(path),
                         "--id", member.id], cwd=ROOT, stdout=log, stderr=log)
                    self.processes.append((process, log, member.id))
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    for process, log, member_id in self.processes:
                        if process.poll() is not None:
                            log.seek(0)
                            raise RuntimeError(f"Peer {member_id} não iniciou: {log.read()}")
                    try:
                        states = [request_json(self.url(m.id, "/health"), timeout=0.2)
                                  for m in self.config.members]
                        if all(s["epoch"] == self.config.epoch for s in states):
                            return
                    except (ConnectionError, ValueError):
                        pass
                    time.sleep(0.05)
                raise RuntimeError("Tempo esgotado ao iniciar peers; verifique as portas.")
            except BaseException:
                self._stop_processes()
                raise

    def url(self, peer_id, path):
        try:
            member = self.config.member(peer_id)
        except StopIteration as exc:
            raise ValueError("Participante desconhecido.") from exc
        return f"http://{member.host}:{member.http_port}{path}"

    def command(self, peer_id, data):
        with self.lock:
            return request_json(self.url(peer_id, "/command"), data)

    def network(self, loss):
        if type(loss) not in (int, float) or not 0 <= loss <= 0.8:
            raise ValueError("Perda deve estar entre 0 e 0.8.")
        with self.lock:
            for m in self.config.members:
                request_json(self.url(m.id, "/network"), {"loss": loss})
            self.config = dataclasses.replace(self.config, loss=float(loss))

    def snapshot(self):
        with self.lock:
            def read(member):
                try:
                    return request_json(self.url(member.id, "/state"), timeout=0.8)
                except (ConnectionError, ValueError) as exc:
                    return {"id": member.id, "name": member.name, "unavailable": str(exc)}
            with ThreadPoolExecutor(max_workers=len(self.config.members)) as executor:
                states = list(executor.map(read, self.config.members))
            return {"config": self.config.to_dict(), "managed": self.managed,
                    "states": states, "comparison": compare_replicas(states),
                    "demo": {"running": self.demo_running, "note": self.demo_note}}

    def reset(self, mode, loss=None):
        with self.lock:
            if not self.managed:
                raise ValueError("Reinicie os peers externos com a mesma nova configuração.")
            if self.demo_running:
                raise ValueError("Aguarde a demonstração terminar.")
            self._restart(mode, self.config.loss if loss is None else loss)
            self.demo_note = ""

    def _restart(self, mode, loss):
        # Validate before stopping a working session.
        config = dataclasses.replace(self.config, epoch=uuid.uuid4().hex, mode=mode, loss=loss)
        self._stop_processes()
        self.config = config
        self.start()

    def demo(self, scenario):
        with self.lock:
            if not self.managed or len(self.config.members) < 3:
                raise ValueError("Demonstrações automáticas exigem 3 peers locais gerenciados.")
            if self.demo_running:
                raise ValueError("Uma demonstração já está em execução.")
            if scenario not in {"concurrent", "causal"}:
                raise ValueError("Cenário desconhecido.")
            self.demo_running = True
            self.demo_note = "Iniciando uma nova sessão para a demonstração…"
            self.demo_thread = threading.Thread(target=self._demo, args=(scenario,), daemon=True)
            self.demo_thread.start()

    def _demo(self, scenario):
        try:
            with self.lock:
                self._restart(self.config.mode, self.config.loss)
                a, b, c = self.config.members[:3]
                delays = {b.id: 1200} if scenario == "concurrent" else {c.id: 1800}
                request_json(self.url(a.id, "/network"), {"delays": delays})
                if scenario == "concurrent":
                    request_json(self.url(b.id, "/network"), {"delays": {a.id: 1200}})
                    self.demo_note = "Duas mensagens concorrentes; rotas A → B e B → A atrasadas."
                    self.command(a.id, {"text": "Vamos usar Python no trabalho?"})
                    self.command(b.id, {"text": "Minha sugestão é usar Java."})
                else:
                    self.demo_note = "A pergunta está atrasada em 1,8 s no caminho até a terceira réplica."
                    original = self.command(a.id, {"text": "Podemos apagar o banco de testes?"})
            if scenario == "causal":
                self._wait(lambda states: any(m["id"] == original["id"]
                           for m in states[1].get("messages", [])))
                self.command(b.id, {"text": "Sim, pode apagar.", "reply_to": original["id"]})
            self._wait(lambda states: all(s.get("idle") and len(s.get("events", [])) == 2
                                         for s in states))
            with self.lock:
                self.demo_note = ("Demonstração concluída. Compare a ordem, os hashes e as violações de contexto. "
                                  "Os atrasos foram removidos; a perda configurada continua ativa.")
        except (ValueError, ConnectionError, RuntimeError) as exc:
            with self.lock:
                self.demo_note = f"Demonstração interrompida: {exc}"
        finally:
            with self.lock:
                failures = []
                for member in self.config.members:
                    try:
                        request_json(self.url(member.id, "/network"), {"delays": {}}, timeout=0.5)
                    except (ConnectionError, ValueError):
                        failures.append(member.name)
                if failures:
                    self.demo_note += " Não foi possível remover atrasos em: " + ", ".join(failures) + "."
                self.demo_running = False

    def _wait(self, predicate, timeout=20):
        deadline = time.monotonic() + timeout
        while not self.closing.is_set() and time.monotonic() < deadline:
            if predicate(self.snapshot()["states"]):
                return
            self.closing.wait(0.04)
        raise RuntimeError("Tempo de espera esgotado; verifique os peers e a perda configurada.")

    def _stop_processes(self):
        for process, _, _ in self.processes:
            if process.poll() is None:
                process.terminate()
        for process, log, _ in self.processes:
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
            log.close()
        self.processes.clear()

    def close(self):
        self.closing.set()
        if self.demo_thread is not None:
            self.demo_thread.join(timeout=3)
        with self.lock:
            self._stop_processes()
            self.temp.cleanup()


def dashboard_server(runtime, host, port):
    class Handler(JsonHandler):
        def do_GET(self):
            if self.path == "/api/state":
                self.json(200, runtime.snapshot())
            elif self.path == "/api/export":
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Disposition", 'attachment; filename="mesma-conversa.json"')
                self.end_headers()
                try:
                    self.wfile.write(json.dumps(runtime.snapshot(), ensure_ascii=False, indent=2).encode())
                except (BrokenPipeError, ConnectionResetError):
                    pass
            elif self.path in {"/", "/app.js", "/style.css"}:
                name = "index.html" if self.path == "/" else self.path[1:]
                content_type = {"index.html": "text/html", "app.js": "text/javascript",
                                "style.css": "text/css"}[name]
                self.respond(200, (STATIC / name).read_bytes(), content_type + "; charset=utf-8")
            else:
                self.json(404, {"error": "Rota inexistente."})

        def do_POST(self):
            try:
                data = self.body()
                with runtime.lock:
                    if self.path == "/api/reset":
                        runtime.reset(data.get("mode", runtime.config.mode),
                                      data.get("loss", runtime.config.loss))
                        self.json(200, {"status": "ready"})
                    elif self.path == "/api/demo":
                        runtime.demo(data.get("scenario"))
                        self.json(202, {"status": "started"})
                    elif runtime.demo_running:
                        raise ValueError("Aguarde a demonstração terminar.")
                    elif self.path == "/api/network":
                        runtime.network(data.get("loss"))
                        self.json(200, {"status": "configured"})
                    elif self.path.startswith("/api/peers/") and self.path.endswith("/command"):
                        parts = self.path.split("/")
                        if len(parts) != 5:
                            raise ValueError("Rota inválida.")
                        self.json(202, runtime.command(parts[3], data))
                    else:
                        self.json(404, {"error": "Rota inexistente."})
            except (ValueError, TypeError) as exc:
                self.json(400, {"error": str(exc)})
            except (ConnectionError, RuntimeError) as exc:
                self.json(503, {"error": str(exc)})

    return ThreadingHTTPServer((host, port), Handler)
