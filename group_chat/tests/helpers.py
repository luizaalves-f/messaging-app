import socket
import time
import uuid

from group_chat.config import Config, Member


def config(mode="total", loss=0.0, count=3):
    # Reserve all ports together so a port cannot be picked twice in one group.
    sockets = []
    ports = []
    try:
        for _ in range(count):
            pair = []
            for kind in (socket.SOCK_DGRAM, socket.SOCK_STREAM):
                sock = socket.socket(socket.AF_INET, kind)
                sock.bind(("127.0.0.1", 0))
                sockets.append(sock)
                pair.append(sock.getsockname()[1])
            ports.append(pair)
        return Config(uuid.uuid4().hex, mode, tuple(
            Member(name, name.title(), "127.0.0.1", *ports[i])
            for i, name in enumerate(("ana", "bruno", "carla")[:count])), loss, 0.05)
    finally:
        for sock in sockets:
            sock.close()


def wait_for(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.015)
    raise AssertionError("Condição não atingida antes do timeout")
