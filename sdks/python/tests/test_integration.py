"""Integration test for PelicanQ Python SDK.

Requires a running PelicanQ daemon. Set PELICANQ_GRPC_ADDR or defaults to 127.0.0.1:7072.
"""

import os
import pytest
from pelicanq import PelicanClient, ClientMessage, QueueOptions


@pytest.fixture
def addr():
    return os.environ.get("PELICANQ_GRPC_ADDR", "127.0.0.1:7072")


@pytest.fixture
def client(addr):
    c = PelicanClient.connect(addr)
    yield c
    c.close()


@pytest.mark.skipif(
    not os.environ.get("PELICANQ_INTEGRATION"),
    reason="set PELICANQ_INTEGRATION=1 to run integration tests",
)
def test_publish_consume_ack(client):
    created = client.declare_queue("test-py-integration", QueueOptions())
    assert isinstance(created, bool)

    msg = ClientMessage(b"hello from python").with_priority(3)
    result = client.publish("test-py-integration", msg)
    assert result.id

    delivery = client.consume("test-py-integration")
    assert delivery is not None
    assert delivery.message.payload == b"hello from python"

    client.ack("test-py-integration", delivery.delivery_tag)

    status = client.health()
    assert status == "ok"
