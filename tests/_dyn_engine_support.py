"""Fake Telegram client + harness for dynamic-engine tests (temporary v11 DBs).

Waits are condition-based (wait_until) or use explicit asyncio.Events as
barriers; no fixed sleeps decide an outcome.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest_asyncio
from telethon.tl.types import PeerChannel, PeerUser

from tests._dyn_support import storage_v11  # noqa: F401  (fixture)
from tgparser.collector import Collector
from tgparser.dynamic import DynamicEngine
from tgparser.media_queue import MediaQueue


def ch(n: int) -> int:
    """Canonical channel/megagroup id (telethon get_peer_id)."""
    return -(10**12 + n)


class FakeMessage:
    def __init__(self, mid, chat_id, text="hi", from_user=None):
        self.id = mid
        if chat_id <= -10**12:
            self.peer_id = PeerChannel(channel_id=-chat_id - 10**12)
        else:
            self.peer_id = PeerUser(user_id=chat_id)
        self.from_id = PeerUser(user_id=from_user) if from_user else None
        self.sender_id = from_user
        self.media = None
        self.fwd_from = None
        self.reply_to = None
        self.date = datetime.now(timezone.utc)
        self.edit_date = None
        self.post_author = None
        self.message = text
        self.entities = []
        self.grouped_id = None
        self.views = None
        self.forwards = None
        self.replies = None
        self.reactions = None

    async def get_sender(self):
        return None

    def to_dict(self):
        return {"id": self.id, "text": self.message}


class Dialog:
    def __init__(self, entity, name=None):
        self.entity = entity
        self.name = name


class FakeClient:
    """Implements only what Collector / DynamicEngine call."""

    def __init__(self):
        self.handlers: list = []
        self.messages: dict[int, list[FakeMessage]] = {}
        self.dialogs: dict[bool, list] | None = None   # None -> iter_dialogs blocks
        self.dialog_hook = None                         # async fn(archived, index)
        self.message_hook = None                        # async fn(chat_id, msg)
        self.resolve: dict = {}                         # get_entity(str) results/exceptions
        self.get_entity_calls: list = []
        self.iter_messages_calls: list = []

    def on(self, builder):
        def deco(f):
            self.handlers.append((builder, f))
            return f
        return deco

    async def get_entity(self, x):
        self.get_entity_calls.append(x)
        if isinstance(x, str):
            r = self.resolve.get(x)
            if isinstance(r, BaseException):
                raise r
            if r is None:
                raise ValueError("Cannot find any entity")
            return r
        return x

    async def get_input_entity(self, x):
        return x

    def iter_messages(self, entity, min_id=0, reverse=True):
        self.iter_messages_calls.append(entity)
        client = self

        async def gen():
            for m in sorted(client.messages.get(entity, []), key=lambda m: m.id):
                if m.id <= min_id:
                    continue
                if client.message_hook:
                    await client.message_hook(entity, m)
                yield m
        return gen()

    async def get_messages(self, entity, ids=None):
        for m in self.messages.get(entity, []):
            if m.id == ids:
                return m
        return None

    def iter_dialogs(self, archived=None):
        client = self

        async def gen():
            if client.dialogs is None:
                await asyncio.Event().wait()           # never configured: block
            for i, d in enumerate(client.dialogs.get(bool(archived), [])):
                if client.dialog_hook:
                    await client.dialog_hook(bool(archived), i)
                yield d
        return gen()

    async def __call__(self, request):
        raise RuntimeError(f"fake client: {type(request).__name__} not supported")

    async def catch_up(self):
        return None

    def is_connected(self):
        return True

    async def connect(self):
        return None

    async def disconnect(self):
        return None


async def wait_until(pred, timeout: float = 5.0, what: str = "condition"):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while True:
        r = pred()
        if asyncio.iscoroutine(r):
            r = await r
        if r:
            return r
        if loop.time() > end:
            raise AssertionError(f"timeout waiting for {what}")
        await asyncio.sleep(0.01)


ENGINE_OPTS = {
    "dynamic_poll_seconds": 0.05,
    "dynamic_discovery_poll_seconds": 0.05,
    "dynamic_yaml_poll_seconds": 0.05,
    "dynamic_listener_backoff_max": 0.2,
    "dynamic_ready_timeout_seconds": 5,
}
COLLECTOR_OPTS = {
    "download_media": False, "participants_sync": False,
    "backfill_batch_size": 100, "backfill_batch_pause_seconds": 0,
    "follow_linked_discussion": False, "per_message_retries": 1,
    "bg_concurrency": 4,
}


class Harness:
    def __init__(self, storage, tmp_path):
        self.storage = storage
        self.tmp_path = tmp_path
        self.client = FakeClient()
        self.engine: DynamicEngine | None = None
        self.collector: Collector | None = None
        self.media_q: MediaQueue | None = None

    def build(self, engine_opts=None, collector_opts=None, yaml_targets=None):
        self.media_q = MediaQueue(self.client, self.storage, base_dir=str(self.tmp_path),
                                  max_media_mb=10, concurrency=1)
        self.collector = Collector(self.client, self.storage, self.media_q,
                                   {**COLLECTOR_OPTS, **(collector_opts or {})})
        self.engine = DynamicEngine(self.client, self.storage, self.collector, self.media_q,
                                    yaml_targets=yaml_targets,
                                    options={**ENGINE_OPTS, **(engine_opts or {})})
        return self.engine

    async def start(self, **kw):
        if self.engine is None:
            self.build(**kw)
        await self.engine.start()
        return self.engine

    async def close(self):
        if self.engine:
            await self.engine.stop()
        if self.collector:
            await self.collector.stop_coordinator()
            await self.collector.wait_background(timeout=2)
        if self.media_q:
            await self.media_q.stop(timeout=1)

    async def wait_applied(self, timeout=5.0):
        async def ok():
            ps = await self.storage.get_policy_state()
            return (ps["applied_version"] == ps["requested_version"]
                    and self.engine.last_reconcile.get("version") == ps["requested_version"]
                    and self.engine.last_reconcile.get("state") == "ok")
        await wait_until(ok, timeout, "policy applied")

    async def wait_reconciled_state(self, state, timeout=5.0):
        async def ok():
            ps = await self.storage.get_policy_state()
            lr = self.engine.last_reconcile
            return lr.get("state") == state and lr.get("version") == ps["requested_version"]
        await wait_until(ok, timeout, f"reconcile state {state}")

    def handler(self, kind: str):
        for builder, fn in self.client.handlers:
            if type(builder).__name__ == kind:
                return fn
        raise KeyError(kind)


@pytest_asyncio.fixture
async def harness(storage_v11, tmp_path):
    h = Harness(storage_v11, tmp_path)
    try:
        yield h
    finally:
        await h.close()
