import unittest

from group_chat.model import Conversation


def send(id="ana:1", author="ana", text="Olá", reply_to=None):
    return {"kind": "send", "id": id, "author": author, "text": text, "reply_to": reply_to}


class ConversationTests(unittest.TestCase):
    def test_same_order_replays_to_same_state_and_duplicates_do_not_apply_twice(self):
        commands = [send(), send("bruno:1", "bruno", "Oi!", "ana:1"),
                    {"kind": "edit", "id": "ana:2", "author": "ana",
                     "message_id": "ana:1", "text": "Olá, grupo!"},
                    {"kind": "delete", "id": "ana:3", "author": "ana", "message_id": "ana:1"}]
        a, b = Conversation(["ana", "bruno"]), Conversation(["ana", "bruno"])
        for seq, command in enumerate(commands, 1):
            a.apply(command, seq)
            b.apply(command, seq)
            b.apply(command, seq)
        self.assertEqual(a.snapshot(), b.snapshot())
        self.assertEqual(len(a.snapshot()["events"]), 4)
        self.assertEqual(a.snapshot()["messages"][0]["text"], "")
        self.assertTrue(a.snapshot()["messages"][0]["deleted"])
        self.assertEqual(a.snapshot()["messages"][1]["reply_to"], "ana:1")

    def test_reply_before_question_is_recorded_even_after_question_arrives(self):
        a = Conversation(["ana", "bruno"])
        b = Conversation(["ana", "bruno"])
        question = send()
        reply = send("bruno:1", "bruno", "Sim", "ana:1")
        a.apply(question)
        a.apply(reply)
        b.apply(reply)
        b.apply(question)
        self.assertNotEqual(a.snapshot()["digest"], b.snapshot()["digest"])
        self.assertEqual(len(b.snapshot()["causal_violations"]), 1)
        self.assertEqual(a.snapshot()["causal_violations"], [])

    def test_only_author_can_edit_and_deleted_message_cannot_be_resurrected(self):
        conversation = Conversation(["ana", "bruno"])
        conversation.apply(send())
        result = conversation.apply({"kind": "edit", "id": "bruno:1", "author": "bruno",
                                     "message_id": "ana:1", "text": "Outra coisa"})
        self.assertFalse(result["accepted"])
        conversation.apply({"kind": "delete", "id": "ana:2", "author": "ana", "message_id": "ana:1"})
        result = conversation.apply({"kind": "edit", "id": "ana:3", "author": "ana",
                                     "message_id": "ana:1", "text": "Voltei"})
        self.assertFalse(result["accepted"])
        self.assertTrue(conversation.snapshot()["messages"][0]["deleted"])

    def test_validation_and_snapshot_cannot_mutate_replica(self):
        conversation = Conversation(["ana", "bruno"])
        for command in [send(text="  "), send(text="x" * 1001), send(id="bruno:1"),
                        send(reply_to="ana:1")]:
            with self.assertRaises(ValueError):
                conversation.apply(command)
        conversation.apply(send())
        state = conversation.snapshot()
        state["messages"][0]["text"] = "Alterado externamente"
        self.assertEqual(conversation.snapshot()["messages"][0]["text"], "Olá")
