"""Type classes for the PelicanQ Python SDK."""

from __future__ import annotations


class PelicanError(Exception):
    """Raised on PelicanQ API errors."""
    pass


class ClientMessage:
    """A message to publish to a queue."""

    def __init__(self, payload: bytes = b""):
        self.payload = payload
        self.headers: dict[str, str] = {}
        self.priority: int = 0
        self.deliver_at: int | None = None
        self.dedup_key: str | None = None

    def with_priority(self, p: int) -> ClientMessage:
        self.priority = max(0, min(p, 9))
        return self

    def with_deliver_at(self, ms: int) -> ClientMessage:
        self.deliver_at = ms
        return self

    def with_dedup_key(self, key: str) -> ClientMessage:
        self.dedup_key = key
        return self

    def with_header(self, k: str, v: str) -> ClientMessage:
        self.headers[k] = v
        return self


class PublishResult:
    """Result of a publish call."""

    def __init__(self, id: str = "", deduplicated: bool = False):
        self.id = id
        self.deduplicated = deduplicated

    def __repr__(self) -> str:
        return f"PublishResult(id={self.id!r}, deduplicated={self.deduplicated})"


class Delivery:
    """A consumed message with its delivery tag."""

    def __init__(
        self,
        delivery_tag: int = 0,
        message: ClientMessage | None = None,
        id: str = "",
        timestamp: int = 0,
        delivery_attempts: int = 0,
    ):
        self.delivery_tag = delivery_tag
        self.message = message or ClientMessage()
        self.id = id
        self.timestamp = timestamp
        self.delivery_attempts = delivery_attempts

    def __repr__(self) -> str:
        return f"Delivery(tag={self.delivery_tag})"


class QueueOptions:
    """Optional queue declaration parameters."""

    def __init__(
        self,
        max_age_secs: int | None = None,
        max_messages: int | None = None,
        max_delivery_attempts: int | None = None,
        dead_letter_queue: str | None = None,
        dedup_window_secs: int | None = None,
    ):
        self.max_age_secs = max_age_secs
        self.max_messages = max_messages
        self.max_delivery_attempts = max_delivery_attempts
        self.dead_letter_queue = dead_letter_queue
        self.dedup_window_secs = dedup_window_secs

    def __repr__(self) -> str:
        return f"QueueOptions(max_age_secs={self.max_age_secs}, ...)"


class QueueInfo:
    """Information about a queue."""

    def __init__(self, name: str = "", depth: int = 0, scheduled_depth: int = 0):
        self.name = name
        self.depth = depth
        self.scheduled_depth = scheduled_depth

    def __repr__(self) -> str:
        return f"QueueInfo(name={self.name!r}, depth={self.depth})"
