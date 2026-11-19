"""Small JSON HTTP boundary shared by peer and dashboard servers (stdlib only)."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def request_json(url, data=None, timeout=2):
    request = Request(url, data=None if data is None else json.dumps(data).encode(),
                      headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except HTTPError as exc:
        try:
            try:
                message = json.load(exc).get("error", str(exc))
            except (ValueError, AttributeError):
                message = str(exc)
        finally:
            exc.close()
        raise ValueError(message) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ConnectionError(f"Peer indisponível: {url}") from exc


class JsonHandler(BaseHTTPRequestHandler):
    server_version = "MesmaConversa/1.0"

    def log_message(self, *_):
        pass

    def body(self):
        if self.headers.get_content_type() != "application/json":
            raise ValueError("Envie Content-Type: application/json.")
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 < length <= 32768:
            raise ValueError("Corpo JSON deve ter entre 1 e 32768 bytes.")
        value = json.loads(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValueError("Corpo deve ser um objeto JSON.")
        return value

    def json(self, status, value):
        self.respond(status, json.dumps(value, ensure_ascii=False).encode(),
                     "application/json; charset=utf-8")

    def respond(self, status, data, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass


def peer_server(node, bind_host=None):
    class Handler(JsonHandler):
        def do_GET(self):
            if self.path in {"/state", "/health"}:
                self.json(200, node.snapshot())
            else:
                self.json(404, {"error": "Rota inexistente."})

        def do_POST(self):
            try:
                data = self.body()
                if self.path == "/command":
                    self.json(202, node.submit(data))
                elif self.path == "/network":
                    if set(data) - {"loss", "delays"}:
                        raise ValueError("Configuração de rede desconhecida.")
                    node.transport.configure_network(**data)
                    self.json(200, node.transport.snapshot())
                else:
                    self.json(404, {"error": "Rota inexistente."})
            except (ValueError, TypeError) as exc:
                self.json(400, {"error": str(exc)})

    return ThreadingHTTPServer((bind_host or node.member.host, node.member.http_port), Handler)
