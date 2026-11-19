"""python3 -m group_chat [run | peer | dashboard]."""

import argparse
import signal
import sys
import threading
import uuid

from .config import Config, Member
from .dashboard import Runtime, dashboard_server
from .http_api import peer_server
from .node import ChatNode


def serve(server):
    def stop(*_):
        # shutdown must run outside the serve_forever thread.
        threading.Thread(target=server.shutdown, daemon=True).start()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser(description="Mesma Conversa — chat distribuído sobre UDP")
    sub = parser.add_subparsers(dest="action")
    run = sub.add_parser("run", help="Inicia 3 processos de peers e o painel local")
    run.add_argument("--mode", choices=("fifo", "total"), default="total")
    run.add_argument("--loss", type=float, default=0.0)
    run.add_argument("--port", type=int, default=8080, help="Porta HTTP do painel")
    run.add_argument("--udp-base", type=int, default=9100)
    run.add_argument("--peer-base", type=int, default=9200, help="Primeira porta HTTP de peer")
    run.add_argument("--host", default="127.0.0.1", help="Endereço de escuta do painel")
    peer = sub.add_parser("peer", help="Inicia apenas um peer de um grupo configurado")
    peer.add_argument("--config", required=True)
    peer.add_argument("--id", required=True)
    peer.add_argument("--bind", help="Endereço local de escuta UDP e HTTP, ex.: 0.0.0.0")
    dashboard = sub.add_parser("dashboard", help="Painel para peers iniciados separadamente")
    dashboard.add_argument("--config", required=True)
    dashboard.add_argument("--host", default="127.0.0.1")
    dashboard.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(sys.argv[1:] or ["run"])
    node = runtime = None
    try:
        if args.action == "peer":
            config = Config.load(args.config)
            node = ChatNode(config, args.id, args.bind)
            server = peer_server(node, args.bind)
        else:
            if args.action == "run":
                config = Config(uuid.uuid4().hex, args.mode, tuple(
                    Member(peer_id, name, "127.0.0.1", args.udp_base + i, args.peer_base + i)
                    for i, (peer_id, name) in enumerate(
                        (("ana", "Ana"), ("bruno", "Bruno"), ("carla", "Carla")))), args.loss)
            else:
                config = Config.load(args.config)
            runtime = Runtime(config, managed=args.action == "run")
            # Reserve the dashboard port before launching any child process.
            server = dashboard_server(runtime, args.host, args.port)
            try:
                runtime.start()
            except BaseException:
                server.server_close()
                raise
            print(f"Mesma Conversa: http://{args.host}:{args.port} | modo {config.mode}", flush=True)
            print("Ctrl+C encerra o painel e os peers locais.", flush=True)
        serve(server)
    except (ValueError, OSError, RuntimeError, StopIteration) as exc:
        parser.exit(1, f"Não foi possível iniciar: {exc}\n")
    finally:
        if runtime is not None:
            runtime.close()
        if node is not None:
            node.close()


if __name__ == "__main__":
    main()
