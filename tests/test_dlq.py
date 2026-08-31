import asyncio
import json
import unittest

from services.fraud.app.main import process_with_retry, route_to_dlq


class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})


class RetryAndDlqTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_then_success(self):
        calls = {"count": 0}

        async def flaky_operation():
            calls["count"] += 1
            if calls["count"] < 3:
                raise ValueError("transient failure")
            return "ok"

        result = await process_with_retry(
            flaky_operation,
            producer=None,
            original_topic="payments.raw",
            payload={"payment_id": "pay-123"},
            max_retries=3,
            base_delay=0,
        )

        self.assertEqual(result, "ok")
        self.assertEqual(calls["count"], 3)

    async def test_routes_failed_message_to_dlq(self):
        producer = FakeProducer()

        async def always_fails():
            raise RuntimeError("database unavailable")

        with self.assertRaises(RuntimeError):
            await process_with_retry(
                always_fails,
                producer=producer,
                original_topic="payments.raw",
                payload={"payment_id": "pay-456", "amount": 100},
                max_retries=2,
                base_delay=0,
            )

        self.assertEqual(len(producer.sent), 1)
        message = json.loads(producer.sent[0]["value"].decode("utf-8"))
        self.assertEqual(producer.sent[0]["topic"], "payments.dlq")
        self.assertEqual(message["payment_id"], "pay-456")
        self.assertEqual(message["source_topic"], "payments.raw")
        self.assertIn("database unavailable", message["error"]["message"])

    def test_route_to_dlq_builds_payload(self):
        message = route_to_dlq(
            original_topic="payments.validated",
            payload={"payment_id": "pay-789", "status": "validated"},
            error=ValueError("bad input"),
            attempt_count=4,
        )

        self.assertEqual(message["source_topic"], "payments.validated")
        self.assertEqual(message["payment_id"], "pay-789")
        self.assertEqual(message["attempt_count"], 4)
        self.assertEqual(message["error"]["type"], "ValueError")
        self.assertEqual(message["error"]["message"], "bad input")


if __name__ == "__main__":
    unittest.main()
