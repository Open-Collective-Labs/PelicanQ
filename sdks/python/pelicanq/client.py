"""PelicanQ Python SDK client."""

from __future__ import annotations

import grpc

from pelicanq.v1 import queue_pb2_grpc, queue_pb2, admin_pb2_grpc, admin_pb2, message_pb2
from pelicanq.types import (
    PelicanError,
    ClientMessage,
    PublishResult,
    Delivery,
    QueueOptions,
    QueueInfo,
)


def _message_to_proto(msg: ClientMessage) -> message_pb2.Message:
    return message_pb2.Message(
        id="",
        payload=msg.payload,
        headers=msg.headers,
        timestamp=0,
        priority=msg.priority,
        deliver_at=msg.deliver_at,
        dedup_key=msg.dedup_key,
        delivery_attempts=0,
    )


def _message_from_proto(pb: message_pb2.Message) -> ClientMessage:
    m = ClientMessage(payload=pb.payload)
    m.headers = dict(pb.headers)
    m.priority = pb.priority
    m.deliver_at = pb.deliver_at if pb.HasField("deliver_at") else None
    m.dedup_key = pb.dedup_key if pb.HasField("dedup_key") else None
    return m


def _delivery_from_proto(cm: queue_pb2.ConsumedMessage) -> Delivery:
    msg = None
    mid = ""
    ts = 0
    attempts = 0
    if cm.HasField("message"):
        msg = _message_from_proto(cm.message)
        mid = cm.message.id
        ts = cm.message.timestamp
        attempts = cm.message.delivery_attempts
    return Delivery(
        delivery_tag=cm.delivery_tag,
        message=msg,
        id=mid,
        timestamp=ts,
        delivery_attempts=attempts,
    )


class PelicanClient:
    """A client for interacting with a PelicanQ daemon over gRPC."""

    def __init__(self, channel: grpc.Channel):
        self._channel = channel
        self._queue = queue_pb2_grpc.QueueServiceStub(channel)
        self._admin = admin_pb2_grpc.AdminServiceStub(channel)

    @classmethod
    def connect(cls, target: str = "127.0.0.1:7072") -> PelicanClient:
        channel = grpc.insecure_channel(target)
        return cls(channel)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> PelicanClient:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    @staticmethod
    def _raise(method: str, err: Exception) -> None:
        raise PelicanError(f"{method}: {err}") from err

    def declare_queue(self, name: str, opts: QueueOptions) -> bool:
        try:
            resp = self._queue.DeclareQueue(
                queue_pb2.DeclareQueueRequest(
                    name=name,
                    max_age_secs=opts.max_age_secs,
                    max_messages=opts.max_messages,
                    max_delivery_attempts=opts.max_delivery_attempts,
                    dead_letter_queue=opts.dead_letter_queue or "",
                    dedup_window_secs=opts.dedup_window_secs,
                )
            )
            return resp.created
        except Exception as e:
            self._raise("declare_queue", e)
            return False

    def publish(self, queue: str, msg: ClientMessage) -> PublishResult:
        try:
            resp = self._queue.Publish(
                queue_pb2.PublishRequest(
                    queue=queue,
                    message=_message_to_proto(msg),
                )
            )
            return PublishResult(id=resp.id, deduplicated=resp.deduplicated)
        except Exception as e:
            self._raise("publish", e)
            raise

    def publish_batch(self, queue: str, msgs: list[ClientMessage]) -> list[PublishResult]:
        try:
            resp = self._queue.PublishBatch(
                queue_pb2.PublishBatchRequest(
                    queue=queue,
                    messages=[_message_to_proto(m) for m in msgs],
                )
            )
            return [PublishResult(id=r.id, deduplicated=r.deduplicated) for r in resp.results]
        except Exception as e:
            self._raise("publish_batch", e)
            raise

    def consume(self, queue: str) -> Delivery | None:
        try:
            resp = self._queue.Consume(queue_pb2.ConsumeRequest(queue=queue))
            if resp.HasField("message") and resp.message is not None:
                return _delivery_from_proto(resp.message)
            return None
        except Exception as e:
            self._raise("consume", e)
            raise

    def consume_batch(self, queue: str, max: int = 10) -> list[Delivery]:
        try:
            resp = self._queue.ConsumeBatch(
                queue_pb2.ConsumeBatchRequest(queue=queue, max=max)
            )
            return [_delivery_from_proto(m) for m in resp.messages]
        except Exception as e:
            self._raise("consume_batch", e)
            raise

    def ack(self, queue: str, delivery_tag: int) -> None:
        try:
            self._queue.Ack(
                queue_pb2.AckRequest(queue=queue, delivery_tag=delivery_tag)
            )
        except Exception as e:
            self._raise("ack", e)

    def nack(self, queue: str, delivery_tag: int) -> None:
        try:
            self._queue.Nack(
                queue_pb2.NackRequest(queue=queue, delivery_tag=delivery_tag)
            )
        except Exception as e:
            self._raise("nack", e)

    def list_queues(self) -> list[QueueInfo]:
        try:
            resp = self._queue.ListQueues(queue_pb2.ListQueuesRequest())
            return [
                QueueInfo(name=q.name, depth=q.depth, scheduled_depth=q.scheduled_depth)
                for q in resp.queues
            ]
        except Exception as e:
            self._raise("list_queues", e)
            raise

    def health(self) -> str:
        try:
            resp = self._admin.Health(admin_pb2.HealthRequest())
            if resp.status != "ok":
                raise PelicanError(f"unhealthy: {resp.status}")
            return resp.status
        except Exception as e:
            self._raise("health", e)
            raise

    def cluster_status(self) -> dict:
        try:
            resp = self._admin.ClusterStatus(admin_pb2.ClusterStatusRequest())
            return {
                "self_id": resp.self_id,
                "is_leader": resp.is_leader,
                "current_leader_id": resp.current_leader_id if resp.HasField("current_leader_id") else None,
                "members": [{"id": m.id, "raft_addr": m.raft_addr, "client_addr": m.client_addr} for m in resp.members],
            }
        except Exception as e:
            self._raise("cluster_status", e)
            raise

    def consume_stream(self, queue: str):
        """Returns a bidirectional stream for consuming messages.
        
        Usage:
            stream = client.consume_stream("myqueue")
            for delivery in stream:
                print(delivery.message.payload)
                client.ack("myqueue", delivery.delivery_tag)
        """
        try:
            def request_iter():
                yield queue_pb2.ConsumeStreamAck(queue=queue)
            return self._queue.ConsumeStream(request_iter())
        except Exception as e:
            self._raise("consume_stream", e)
            raise
