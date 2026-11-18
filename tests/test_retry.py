import asyncio
import pytest
from telethon.errors import FloodWaitError
from tgparser.retry import with_flood_wait


class _Fw(FloodWaitError):
    def __init__(self, seconds):
        self.seconds = seconds
        self.message = f"FLOOD_WAIT_{seconds}"


@pytest.mark.asyncio
async def test_flood_wait_retries_then_succeeds(monkeypatch):
    sleeps = []

    async def _sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("tgparser.retry.asyncio.sleep", _sleep)
    calls = {"n": 0}

    @with_flood_wait(max_tries=5, base=0.01, cap=0.1)
    async def op():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _Fw(3)
        return "ok"

    assert await op() == "ok"
    assert calls["n"] == 2
    assert sleeps and sleeps[0] >= 3


@pytest.mark.asyncio
async def test_transient_backoff(monkeypatch):
    sleeps = []

    async def _sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("tgparser.retry.asyncio.sleep", _sleep)
    calls = {"n": 0}

    @with_flood_wait(max_tries=3, base=0.01, cap=0.1)
    async def op():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("x")
        return 42

    assert await op() == 42
    assert calls["n"] == 3
    assert len(sleeps) == 2
