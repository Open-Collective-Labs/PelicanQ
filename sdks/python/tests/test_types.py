"""Unit tests for PelicanQ Python SDK types."""

import pytest
from pelicanq import ClientMessage, PublishResult, QueueOptions, QueueInfo, Delivery


class TestClientMessage:
    def test_new(self):
        msg = ClientMessage(b"hello")
        assert msg.payload == b"hello"
        assert msg.headers == {}
        assert msg.priority == 0
        assert msg.deliver_at is None
        assert msg.dedup_key is None

    def test_with_priority(self):
        msg = ClientMessage().with_priority(5)
        assert msg.priority == 5

    def test_priority_clamps(self):
        msg = ClientMessage().with_priority(15)
        assert msg.priority == 9

    def test_with_deliver_at(self):
        msg = ClientMessage().with_deliver_at(1000)
        assert msg.deliver_at == 1000

    def test_with_dedup_key(self):
        msg = ClientMessage().with_dedup_key("k1")
        assert msg.dedup_key == "k1"

    def test_with_header(self):
        msg = ClientMessage().with_header("ct", "text/plain")
        assert msg.headers["ct"] == "text/plain"


class TestQueueOptions:
    def test_default_all_none(self):
        opts = QueueOptions()
        assert opts.max_age_secs is None
        assert opts.max_messages is None
        assert opts.max_delivery_attempts is None
        assert opts.dead_letter_queue is None
        assert opts.dedup_window_secs is None


class TestPublishResult:
    def test_defaults(self):
        r = PublishResult()
        assert r.id == ""
        assert r.deduplicated is False


class TestQueueInfo:
    def test_defaults(self):
        q = QueueInfo()
        assert q.name == ""
        assert q.depth == 0


class TestDelivery:
    def test_defaults(self):
        d = Delivery()
        assert d.delivery_tag == 0
        assert isinstance(d.message, ClientMessage)
