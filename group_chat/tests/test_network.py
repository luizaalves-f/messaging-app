import dataclasses
import queue
import unittest
from concurrent.futures import ThreadPoolExecutor

from group_chat.node import ChatNode
from group_chat.transport import ReliableFifoTransport
from group_chat.tests.helpers import config, wait_for


class TransportTests(unittest.TestCase):
    def test_reordered_datagrams_wait_for_gap_and_deliver_once(self):
        cfg = config(count=2)
        a = ReliableFifoTransport(cfg, "ana")
        self.addCleanup(a.close)
        b = ReliableFifoTransport(cfg, "bruno")
        self.addCleanup(b.close)
        a.configure_network(delays={"bruno": 250})
        a.send("bruno", {"text": "primeira"})
        a.configure_network(delays={})
        a.send("bruno", {"text": "segunda"})
        wait_for(lambda: b.snapshot()["buffered"] == 1)
        with self.assertRaises(queue.Empty):
            b.deliveries.get_nowait()
        self.assertEqual(b.deliveries.get(timeout=2)[2], {"text": "primeira"})
        b.deliveries.task_done()
        self.assertEqual(b.deliveries.get(timeout=2)[2], {"text": "segunda"})
        b.deliveries.task_done()
        wait_for(lambda: a.snapshot()["pending_acks"] == 0)

    def test_old_epoch_is_not_delivered_or_acknowledged(self):
        cfg = config(count=2)
        a = ReliableFifoTransport(cfg, "ana")
        self.addCleanup(a.close)
        b = ReliableFifoTransport(dataclasses.replace(cfg, epoch="another-session"), "bruno")
        self.addCleanup(b.close)
        a.send("bruno", {"text": "antiga"})
        wait_for(lambda: b.snapshot().get("invalid", 0) > 0)
        self.assertEqual(b.snapshot().get("delivered", 0), 0)
        self.assertEqual(a.snapshot()["pending_acks"], 1)


class NodeTests(unittest.TestCase):
    def nodes(self, mode="total", loss=0):
        cfg = config(mode, loss)
        nodes = [ChatNode(cfg, m.id) for m in cfg.members]
        for node in nodes:
            self.addCleanup(node.close)
        return nodes

    def test_concurrent_total_order_converges_under_packet_and_ack_loss(self):
        nodes = self.nodes(loss=0.35)
        for node in nodes:
            node.transport.configure_network(delays={"ana": 30, "bruno": 60, "carla": 10})
        with ThreadPoolExecutor(max_workers=3) as executor:
            def send_many(node):
                for i in range(8):
                    node.submit({"text": f"{node.peer_id} mensagem {i}"})
            list(executor.map(send_many, nodes))
        wait_for(lambda: all(len(n.snapshot()["events"]) == 24 and n.snapshot()["idle"]
                             for n in nodes), timeout=15)
        states = [n.snapshot() for n in nodes]
        self.assertEqual(len({s["digest"] for s in states}), 1)
        self.assertEqual([e["global_seq"] for e in states[0]["events"]], list(range(1, 25)))
        self.assertTrue(all(not s["causal_violations"] and not s["error"] for s in states))
        self.assertGreater(sum(s["network"].get("retransmitted", 0) for s in states), 0)

    def test_fifo_can_deliver_reply_before_question(self):
        a, b, c = self.nodes("fifo")
        a.transport.configure_network(delays={"carla": 450})
        original = a.submit({"text": "Podemos apagar?"})["id"]
        wait_for(lambda: original in b.snapshot()["event_ids"])
        reply = b.submit({"text": "Sim", "reply_to": original})["id"]
        wait_for(lambda: reply in c.snapshot()["event_ids"])
        self.assertEqual(c.snapshot()["event_ids"], [reply])
        wait_for(lambda: all(len(n.snapshot()["events"]) == 2 and n.snapshot()["idle"]
                             for n in (a, b, c)))
        self.assertEqual(c.snapshot()["event_ids"], [reply, original])
        self.assertEqual(len(c.snapshot()["causal_violations"]), 1)
        self.assertNotEqual(a.snapshot()["digest"], c.snapshot()["digest"])

    def test_total_order_preserves_reply_context_and_message_lifecycle(self):
        a, b, c = self.nodes()
        a.transport.configure_network(delays={"carla": 350})
        original = a.submit({"id": "ana:example", "text": "Primeiro texto"})
        self.assertEqual(a.submit({"id": "ana:example", "text": "Primeiro texto"}), original)
        with self.assertRaises(ValueError):
            a.submit({"id": "ana:example", "text": "Outro comando"})
        with self.assertRaises(ValueError):
            b.submit({"text": "Resposta prematura", "reply_to": original["id"]})
        wait_for(lambda: original["id"] in b.snapshot()["event_ids"])
        b.submit({"text": "Resposta", "reply_to": original["id"]})
        wait_for(lambda: original["id"] in a.snapshot()["event_ids"])
        with self.assertRaises(ValueError):
            b.submit({"kind": "delete", "message_id": original["id"]})
        a.submit({"kind": "edit", "message_id": original["id"], "text": "Texto editado"})
        a.submit({"kind": "delete", "message_id": original["id"]})
        wait_for(lambda: all(len(n.snapshot()["events"]) == 4 and n.snapshot()["idle"]
                             for n in (a, b, c)))
        states = [n.snapshot() for n in (a, b, c)]
        self.assertEqual(len({s["digest"] for s in states}), 1)
        self.assertTrue(all(not s["causal_violations"] for s in states))
        self.assertTrue(states[0]["messages"][0]["deleted"])


if __name__ == "__main__":
    unittest.main()
