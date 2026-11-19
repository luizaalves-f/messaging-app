import unittest

from group_chat.dashboard import Runtime
from group_chat.http_api import request_json
from group_chat.tests.helpers import config, wait_for


class DashboardIntegrationTests(unittest.TestCase):
    def test_real_processes_demonstrate_divergence_and_total_order_with_loss(self):
        runtime = Runtime(config("fifo"))
        self.addCleanup(runtime.close)
        runtime.start()
        for mode, scenario, expected in [
            ("fifo", "concurrent", "diverged"),
            ("fifo", "causal", "diverged"),
            ("total", "concurrent", "synchronized"),
            ("total", "causal", "synchronized"),
        ]:
            with self.subTest(mode=mode, scenario=scenario):
                runtime.reset(mode, loss=0.3 if mode == "total" else 0)
                runtime.demo(scenario)
                wait_for(lambda: not runtime.snapshot()["demo"]["running"], timeout=25)
                state = runtime.snapshot()
                self.assertNotIn("interrompida", state["demo"]["note"])
                self.assertEqual(state["comparison"]["status"], expected)
                self.assertTrue(all(len(s["events"]) == 2 for s in state["states"]))
                self.assertTrue(all(s["network"]["delays"] == {} for s in state["states"]))
                if mode == "total":
                    self.assertTrue(all(not s["causal_violations"] for s in state["states"]))
                elif scenario == "causal":
                    self.assertEqual(len(state["states"][2]["causal_violations"]), 1)

    def test_http_rejects_bad_commands_and_accepts_idempotent_requests(self):
        runtime = Runtime(config())
        self.addCleanup(runtime.close)
        runtime.start()
        url = runtime.url("ana", "/command")
        for data in [{"text": ""}, {"kind": []}, {"id": "bruno:1", "text": "inválido"},
                     {"text": "ok", "author": "bruno"}, {"kind": "edit", "message_id": "missing", "text": "oi"}]:
            with self.assertRaises(ValueError):
                request_json(url, data)
        command = {"id": "ana:repeat", "text": "Uma vez só"}
        self.assertEqual(request_json(url, command), request_json(url, command))
        wait_for(lambda: runtime.snapshot()["comparison"]["status"] == "synchronized")
        self.assertTrue(all(len(s["events"]) == 1 for s in runtime.snapshot()["states"]))
