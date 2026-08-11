# AgentDuetTransport Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the inbound-only `AgentDuetTransport` (Pipecat transport over an AgentDuet SDK `Call`), a keyless example bot, and the tests that pin its teardown matrix — per [specs/2026-08-03-agentduet-transport-spike-design.md](2026-08-03-agentduet-transport-spike-design.md).

**Architecture:** A private `_AgentDuetSession` owns call lifecycle (answer, teardown latch, event fan-out) and is shared by the input/output transport halves; `AgentDuetTransport(BaseTransport)` owns the event registry and enforces the generic-alias rule structurally. Audio is pure byte pass-through (both sides mono s16 PCM at the call's fixed rate).

**Tech Stack:** Python ≥3.12, uv, `agentduet>=1.0.0b10`, `pipecat-ai>=1.7,<2`, pytest + pytest-asyncio + pytest-timeout, ruff.

**Verified API facts (do not re-derive; checked against installed `pipecat-ai` 1.7.0 and the `agentduet` 1.0.0b10 checkout at `~/Documents/wss-sdk-python`):**

- Event handlers: `BaseObject._register_event_handler(name)` / `await self._call_event_handler(name, *args)`; handlers receive `(transport, *args)`.
- `TransportParams.audio_in_sample_rate` / `audio_out_sample_rate` default `None`; when set, they win over `StartFrame` rates (`self._params.audio_in_sample_rate or frame.audio_in_sample_rate` in `BaseInputTransport.start`).
- Transport halves: override `start/stop/cancel` (call `await super()` first), `await self.set_transport_ready(frame)` when media may flow, `push_audio_frame(InputAudioRawFrame(...))` on input, `async def write_audio_frame(frame) -> bool` on output. `FrameProcessor` provides `self.create_task(coro)` / `await self.cancel_task(task)`.
- `InterruptionFrame` (NOT `StartInterruptionFrame`) is the system frame; output intercepts it in `process_frame` after `super()`.
- `CancelWorkerFrame(reason=...)` pushed from either direction reaches the worker and becomes a `CancelFrame`.
- `MediaSender` resamples output frames to the transport rate automatically — differing `PipelineParams` rates are normal, not an error.
- `UserTurnProcessor()` (bare; interruptions default on via the start strategy) broadcasts `UserStoppedSpeakingFrame` on turn stop.
- `_register_event_handler(name)` defaults to `sync=False`, which runs handlers **fire-and-forget in a task**. Register with `sync=True` so handlers are awaited inline — required for the "on_before_disconnect completes before disconnect events" invariant and the alias pair's "one moment" ordering.
- `MediaSender` buffers output audio into `audio_out_10ms_chunks` × 10 ms chunks (default 4 → 1280 bytes at 16 kHz mono) before calling `write_audio_frame`. Tests that assert on written audio must set `audio_out_10ms_chunks=1` and queue exact 10 ms multiples (320 bytes at 16 kHz).
- `BaseInputTransport._audio_in_queue` exists only after `set_transport_ready()` — `push_audio_frame` before that raises `AttributeError`. The input half must call `set_transport_ready` **before** starting the pump.
- `WorkerRunner()` constructs with defaults; `await runner.run(worker)` returns when the worker finishes (`auto_end=True` default).
- SDK: `CommandResult` is truthy on success; `answer()` raises `CallClosedError` when the call is over; `send_audio` raises `BufferFullError` / `CallClosedError`; `clear_send_audio_buffer()` flushes the local ring buffer instantly then awaits the `agent.interrupt` ack (10 s timeout), returning `payload=bytes_cleared`; `call.close()` fires `on_hangup` only when a voice WS was open; `on_hangup` handlers take one argument.

**One deliberate deviation from the design doc:** no warning on a differing `StartFrame` rate. `StartFrame.audio_out_sample_rate` defaults to 24000, so every default-config run would warn spuriously; and differing pipeline rates are legitimate (MediaSender resamples). The call's rate still wins unconditionally because the constructor writes it into `TransportParams`. The constructor guard (raise on user-set conflicting rate) stays.

---

## File structure

```
pyproject.toml                          package metadata, deps, tooling config
src/pipecat_agentduet/__init__.py       public exports: AgentDuetTransport, CallEventPayload
src/pipecat_agentduet/_session.py       _AgentDuetSession + CallEventPayload (lifecycle only, no frames)
src/pipecat_agentduet/transport.py      AgentDuetTransport + AgentDuetInputTransport + AgentDuetOutputTransport
tests/fakes.py                          FakeCall / FakeParty / RecordingNotifier test doubles
tests/test_session.py                   teardown matrix + event invariants (no Pipecat imports)
tests/test_transport.py                 frame plumbing, rate guard, alias firing (real mini-pipelines)
examples/inbound_tone_bot.py            keyless spike bot (SessionManager arrival + tone pipeline)
examples/outbound_track_probe.py        one-shot dial probe: which track carries the callee?
```

---

### Task 1: Package scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `src/pipecat_agentduet/__init__.py`
- Create: `tests/__init__.py` (empty)

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "pipecat-agentduet"
version = "0.1.0.dev0"
description = "AgentDuet transport for Pipecat: phone/WhatsApp calls in any Pipecat pipeline"
requires-python = ">=3.12"
# The agentduet floor names a pre-release on purpose: a specifier that itself
# carries one permits pre-releases of that package.
dependencies = [
    "agentduet>=1.0.0b10,<2",
    "pipecat-ai>=1.7,<2",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/pipecat_agentduet"]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=1.0",
    "pytest-timeout>=2.3",
    "ruff>=0.8",
]
# Example-only deps (Silero pulls onnxruntime; keep it out of the test env).
example = [
    "pipecat-ai[silero]>=1.7,<2",
    "python-dotenv>=1.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
# Backstop for async teardown tests: a deadlock fails with a stack dump
# instead of hanging (mirrors the SDK repo's convention).
timeout = 30

[tool.ruff]
line-length = 100
target-version = "py312"
```

- [ ] **Step 2: Write `src/pipecat_agentduet/__init__.py`** (placeholder; exports land in Tasks 3/6)

```python
"""AgentDuet transport for Pipecat."""
```

- [ ] **Step 3: Sync and verify the environment**

Run: `uv sync --group dev && uv run python -c "import pipecat, agentduet; print(pipecat.__name__, agentduet.__version__)"`
Expected: prints `pipecat 1.0.0b10` (pipecat's import banner appears above it)

Run: `uv run pytest`
Expected: `no tests ran` (exit code 5 is fine at this stage)

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml src/ tests/ uv.lock
git commit -m "chore: scaffold pipecat-agentduet package"
```

---

### Task 2: FakeCall test double

**Files:**
- Create: `tests/fakes.py`

The fake models only the public `Call` surface the transport touches. Key modelling decisions (from the design doc):

- `close()` does **not** fire hangup handlers (models the worst case: voice WS never opened, where the real SDK fires nothing). A separate `trigger_hangup()` lets tests fire the hangup path explicitly, including *after* `close()`, to pin the double-teardown guard.
- `answer()` outcome is scriptable: a `CommandResult`, an exception instance to raise, and an optional `asyncio.Event` to block on (lets a test inject a hangup mid-answer deterministically).

- [ ] **Step 1: Write `tests/fakes.py`**

```python
"""Test doubles for the public agentduet surface the transport touches."""

import asyncio

from agentduet import Address, CallState, CommandResult, Network
from agentduet.audio_config import CallAudioConfig
from agentduet.exceptions import CallClosedError


class FakeParty:
    """CallParty stand-in: a value plus a queue-fed audio stream."""

    def __init__(self, value: str):
        self.value = value
        self.audio_queue: asyncio.Queue[bytes | None] = asyncio.Queue()

    async def _stream(self):
        while True:
            chunk = await self.audio_queue.get()
            if chunk is None:  # sentinel: stream ends (call terminated)
                return
            yield chunk

    def audio_stream(self):
        return self._stream()


class FakeCall:
    """Public-surface Call double. Scriptable answer, manual hangup."""

    def __init__(
        self,
        *,
        subscriber: str = "+6511111111",
        caller_value: str = "+6522222222",
        callee_value: str = "+6511111111",
        sample_rate: int = 16000,
    ):
        self.id = "call-1"
        self.subscriber = subscriber
        self.participant = Address(Network.TELCO, caller_value)
        self.caller = FakeParty(caller_value)
        self.callee = FakeParty(callee_value)
        self.audio_config = CallAudioConfig(sample_rate=sample_rate)
        self.state = CallState.RINGING

        # Scripting knobs
        self.answer_result: CommandResult | Exception = CommandResult(success=True)
        self.answer_gate: asyncio.Event | None = None  # answer() awaits this if set
        self.send_audio_error: Exception | None = None
        self.clear_delay: float = 0.0
        self.clear_result = CommandResult(success=True, payload=0)

        # Recorders
        self.sent_audio: list[bytes] = []
        self.clear_calls: int = 0
        self.close_calls: int = 0
        self._hangup_handlers: list = []

    # -- registration ------------------------------------------------------
    def on_hangup(self, func):
        self._hangup_handlers.append(func)
        return func

    def on_call_event(self, event_name):
        def decorator(func):
            return func

        return decorator

    # -- commands ----------------------------------------------------------
    async def answer(self) -> CommandResult:
        if self.answer_gate is not None:
            await self.answer_gate.wait()
        if self.state == CallState.TERMINATED and not isinstance(
            self.answer_result, Exception
        ):
            raise CallClosedError()
        if isinstance(self.answer_result, Exception):
            raise self.answer_result
        if self.answer_result.success:
            self.state = CallState.LIVE
        return self.answer_result

    async def send_audio(self, audio: bytes) -> None:
        if self.state == CallState.TERMINATED:
            raise CallClosedError()
        if self.send_audio_error is not None:
            raise self.send_audio_error
        self.sent_audio.append(audio)

    async def clear_send_audio_buffer(self) -> CommandResult:
        if self.state == CallState.TERMINATED:
            raise CallClosedError()
        self.clear_calls += 1
        if self.clear_delay:
            await asyncio.sleep(self.clear_delay)
        return self.clear_result

    async def close(self) -> CommandResult:
        # Models the voice-WS-never-opened worst case: marks terminated,
        # ends the audio streams, fires NO hangup handlers.
        self.close_calls += 1
        await self._terminate()
        return CommandResult(success=True)

    # -- test controls -------------------------------------------------------
    async def trigger_hangup(self) -> None:
        """Remote hangup: terminate, then fire handlers exactly once each."""
        await self._terminate()
        handlers, self._hangup_handlers = self._hangup_handlers, []
        for handler in handlers:
            await handler(None)

    async def _terminate(self) -> None:
        self.state = CallState.TERMINATED
        # End party streams (StopAsyncIteration in consumers).
        self.caller.audio_queue.put_nowait(None)
        self.callee.audio_queue.put_nowait(None)


class RecordingNotifier:
    """Duck-types the notifier side of AgentDuetTransport for session tests."""

    def __init__(self):
        self.events: list[tuple[str, object]] = []
        self.cancel_reasons: list[str] = []

    async def _fire(self, name, payload):
        self.events.append((name, payload))

    async def _request_worker_cancel(self, reason):
        self.cancel_reasons.append(reason)

    def names(self) -> list[str]:
        return [name for name, _ in self.events]
```

- [ ] **Step 2: Smoke-check the fakes import and behave**

Run: `uv run python -c "
import asyncio
from tests.fakes import FakeCall

async def main():
    c = FakeCall()
    assert (await c.answer()).success
    await c.trigger_hangup()
    from agentduet import CallState
    assert c.state == CallState.TERMINATED

asyncio.run(main())
print('fakes ok')
"`
Expected: `fakes ok`

- [ ] **Step 3: Commit**

```bash
git add tests/fakes.py
git commit -m "test: add FakeCall/FakeParty/RecordingNotifier doubles"
```

---

### Task 3: Session identity — remote party, sample rate, payload

**Files:**
- Create: `src/pipecat_agentduet/_session.py`
- Create: `tests/test_session.py`

- [ ] **Step 1: Write the failing tests**

```python
"""_AgentDuetSession: identity, answer paths, teardown matrix."""

import asyncio

import pytest
from agentduet import CallState, CommandResult
from agentduet.exceptions import CallClosedError

from pipecat_agentduet._session import _AgentDuetSession, CallEventPayload
from tests.fakes import FakeCall, RecordingNotifier


def make_session(call: FakeCall | None = None):
    call = call or FakeCall()
    notifier = RecordingNotifier()
    return _AgentDuetSession(call, notifier), call, notifier


class TestIdentity:
    def test_remote_party_inbound_is_caller(self):
        # Inbound: subscriber is the callee, remote party is the caller.
        session, call, _ = make_session(
            FakeCall(subscriber="+65B", caller_value="+65A", callee_value="+65B")
        )
        assert session.remote_party is call.caller

    def test_remote_party_outbound_is_callee(self):
        # Outbound: subscriber placed the call, remote party is the callee.
        session, call, _ = make_session(
            FakeCall(subscriber="+65A", caller_value="+65A", callee_value="+65B")
        )
        assert session.remote_party is call.callee

    def test_sample_rate_reads_call_audio_config(self):
        session, _, _ = make_session(FakeCall(sample_rate=8000))
        assert session.sample_rate == 8000
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_session.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipecat_agentduet._session'`

- [ ] **Step 3: Write `src/pipecat_agentduet/_session.py`** (identity part; lifecycle methods land in Tasks 4–5, but write the full class skeleton now so each later step is additive)

```python
"""Call lifecycle for AgentDuetTransport: answer, teardown, event fan-out.

One _AgentDuetSession per call, shared by the input and output transport
halves. Every disconnect path funnels through _teardown() — the single
idempotent choke point (the SDK's own _mark_terminated pattern).

This module imports nothing from pipecat: it must be testable with no
frames in play.
"""

import asyncio
import logging
from dataclasses import dataclass

from agentduet import Address, CallState, CommandResult
from agentduet.exceptions import CallClosedError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CallEventPayload:
    """Payload for connected/disconnected events. Reuses SDK types only."""

    participant: Address
    call_id: str
    state: CallState


def _terminated_result() -> CommandResult:
    return CommandResult(
        success=False,
        error_code="CALL_TERMINATED",
        error_message="Call terminated before or during answer",
    )


class _AgentDuetSession:
    """Owns answer, the teardown latch, and event fan-out for one Call.

    ``notifier`` is the AgentDuetTransport (duck-typed in tests): it provides
    ``_fire(event_name, payload)`` and ``_request_worker_cancel(reason)``.
    """

    def __init__(self, call, notifier):
        self._call = call
        self._notifier = notifier
        self._start_begun = False
        self._start_complete = asyncio.Event()
        self._connected_fired = False
        self._torn_down = False
        self._self_initiated = False
        self._cancel_requested = False
        self._terminal_state_fired = False

    @property
    def call(self):
        return self._call

    @property
    def sample_rate(self) -> int:
        return self._call.audio_config.sample_rate

    @property
    def remote_party(self):
        # The remote party is whichever side isn't the subscriber. Track ids
        # are role-fixed locally (caller 0, callee 1), so this holds for both
        # call directions without touching the private _origin.
        if self._call.caller.value == self._call.subscriber:
            return self._call.callee
        return self._call.caller

    def _payload(self) -> CallEventPayload:
        return CallEventPayload(
            participant=self._call.participant,
            call_id=self._call.id,
            state=self._call.state,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_session.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add src/pipecat_agentduet/_session.py tests/test_session.py
git commit -m "feat: session identity - remote party, sample rate, payload"
```

---

### Task 4: Session start — the four answer paths

**Files:**
- Modify: `src/pipecat_agentduet/_session.py`
- Modify: `tests/test_session.py`

- [ ] **Step 1: Write the failing tests** (append to `tests/test_session.py`)

```python
class TestStart:
    async def test_truthy_answer_fires_connected_alias_pair_once(self):
        session, call, notifier = make_session()
        await session.start()
        assert notifier.names() == ["on_dialin_connected", "on_call_state_updated"]
        payload = notifier.events[0][1]
        assert isinstance(payload, CallEventPayload)
        assert payload.call_id == call.id
        assert payload.state == CallState.LIVE

    async def test_start_is_latched_second_caller_waits_no_double_answer(self):
        session, call, notifier = make_session()
        call.answer_gate = asyncio.Event()
        first = asyncio.create_task(session.start())
        second = asyncio.create_task(session.start())
        await asyncio.sleep(0)  # let both tasks reach the gate/latch
        call.answer_gate.set()
        await asyncio.gather(first, second)
        # One answer, one connected event — the second start() just waited.
        assert notifier.names().count("on_dialin_connected") == 1

    async def test_dead_before_start_fires_dialin_error_and_cancels(self):
        session, call, notifier = make_session()
        await call.trigger_hangup()  # already TERMINATED before start
        notifier.events.clear()
        await session.start()
        names = notifier.names()
        assert "on_dialin_error" in names
        assert "on_dialin_connected" not in names
        assert "on_dialin_stopped" not in names
        error = dict(notifier.events)["on_dialin_error"]
        assert error.error_code == "CALL_TERMINATED"
        assert notifier.cancel_reasons  # worker cancel requested

    async def test_falsy_answer_fires_dialin_error_with_real_result(self):
        session, call, notifier = make_session()
        call.answer_result = CommandResult(success=False, error_code="TIMEOUT")
        await session.start()
        error = dict(notifier.events)["on_dialin_error"]
        assert error.error_code == "TIMEOUT"
        assert "on_dialin_connected" not in notifier.names()
        assert notifier.cancel_reasons

    async def test_answer_raising_callclosed_fires_dialin_error(self):
        session, call, notifier = make_session()
        call.answer_result = CallClosedError()
        await session.start()
        error = dict(notifier.events)["on_dialin_error"]
        assert error.error_code == "CALL_TERMINATED"
        assert notifier.cancel_reasons

    async def test_hangup_racing_truthy_answer_suppresses_connected(self):
        # answer() returns truthy but a hangup was processed while it was in
        # flight: connected events must never fire (matrix row 4).
        session, call, notifier = make_session()
        call.answer_gate = asyncio.Event()
        start_task = asyncio.create_task(session.start())
        await asyncio.sleep(0)
        await call.trigger_hangup()  # teardown runs; call now TERMINATED
        call.answer_result = CommandResult(success=True)  # server said yes anyway
        call.answer_gate.set()
        await start_task
        names = notifier.names()
        assert "on_dialin_connected" not in names
        assert "on_client_connected" not in names
```

Note: the last test's `answer()` will actually raise `CallClosedError` from the fake (state is TERMINATED) — that is fine and realistic; the assertion is only that connected never fires. Keep the test as written: it pins the invariant, not the specific internal path.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_session.py -v -k TestStart`
Expected: FAIL — `AttributeError: '_AgentDuetSession' object has no attribute 'start'`

- [ ] **Step 3: Implement `start()` and helpers** (append inside `_AgentDuetSession`)

```python
    async def start(self) -> None:
        """Answer the call. Latched: the first caller answers, later callers
        wait for completion. Fires connected events only on a truthy answer
        with no teardown racing it."""
        if self._start_begun:
            await self._start_complete.wait()
            return
        self._start_begun = True
        try:
            # Hangup can arrive in any state, including mid-answer: register
            # the handler before answering so the window is closed.
            self._call.on_hangup(self._on_hangup)
            if self._call.state == CallState.TERMINATED:
                await self._fail_start(_terminated_result())
                return
            try:
                result = await self._call.answer()
            except CallClosedError:
                await self._fail_start(_terminated_result())
                return
            if not result:
                await self._fail_start(result)
                return
            if self._torn_down or self._call.state == CallState.TERMINATED:
                # A hangup raced the truthy answer; teardown ran (or will run)
                # from on_hangup. Connected events must never fire.
                return
            self._connected_fired = True
            await self._notifier._fire("on_dialin_connected", self._payload())
            await self._fire_state()
        finally:
            self._start_complete.set()

    async def _fail_start(self, result: CommandResult) -> None:
        already_torn_down = self._torn_down
        self._torn_down = True
        await self._notifier._fire("on_dialin_error", result)
        if not already_torn_down:
            await self._fire_state()
        await self._request_cancel("answer failed")

    async def _fire_state(self) -> None:
        state = self._call.state
        if state == CallState.TERMINATED:
            if self._terminal_state_fired:
                return
            self._terminal_state_fired = True
        await self._notifier._fire("on_call_state_updated", state)

    async def _request_cancel(self, reason: str) -> None:
        if self._cancel_requested:
            return
        self._cancel_requested = True
        await self._notifier._request_worker_cancel(reason)

    async def _on_hangup(self, _payload) -> None:
        await self._teardown()

    async def _teardown(self) -> None:
        raise NotImplementedError  # Task 5
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_session.py -v`
Expected: all `TestStart` tests PASS except `test_hangup_racing_truthy_answer_suppresses_connected`, which FAILS with `NotImplementedError` — its `trigger_hangup` fires the registered `on_hangup` → the `_teardown` stub. (`test_dead_before_start` hangs up *before* `start()` registers the handler, so it never reaches the stub and passes.) That one failure is the Task 5 seam; if anything else fails, fix before moving on.

- [ ] **Step 5: Commit**

```bash
git add -u
git commit -m "feat: session start - answer paths, latch, connected gating"
```

---

### Task 5: Session teardown — the matrix

**Files:**
- Modify: `src/pipecat_agentduet/_session.py`
- Modify: `tests/test_session.py`

- [ ] **Step 1: Write the failing tests** (append)

```python
class TestTeardown:
    async def test_remote_hangup_fires_ordered_disconnect_and_cancel(self):
        session, call, notifier = make_session()
        await session.start()
        notifier.events.clear()
        await call.trigger_hangup()
        names = notifier.names()
        # on_before_disconnect strictly precedes the disconnect events.
        assert names.index("on_before_disconnect") < names.index("on_dialin_stopped")
        assert notifier.cancel_reasons == ["remote hangup"]

    async def test_disconnect_fires_exactly_once_under_double_hangup(self):
        session, call, notifier = make_session()
        await session.start()
        await call.trigger_hangup()
        await session._teardown()  # simulate any second path racing in
        assert notifier.names().count("on_dialin_stopped") == 1
        assert notifier.names().count("on_before_disconnect") == 1

    async def test_self_initiated_close_fires_events_but_never_cancel(self):
        session, call, notifier = make_session()
        await session.start()
        notifier.events.clear()
        await session.close()
        assert "on_dialin_stopped" in notifier.names()
        assert notifier.cancel_reasons == []  # our own close: no worker cancel
        assert call.close_calls == 1

    async def test_close_without_hangup_event_still_tears_down(self):
        # FakeCall.close() fires no hangup (voice WS never opened): teardown
        # must still run, driven by close() itself.
        session, call, notifier = make_session()
        await session.start()
        await session.close()
        assert "on_dialin_stopped" in notifier.names()

    async def test_hangup_after_close_does_not_double_fire(self):
        session, call, notifier = make_session()
        await session.start()
        await session.close()
        await call.trigger_hangup()  # late transport-drop event
        assert notifier.names().count("on_dialin_stopped") == 1

    async def test_never_connected_call_gets_no_disconnect_events(self):
        # Falsy answer, then the hangup event lands: on_dialin_error already
        # reported it; disconnect events must not fire (alias-pair rule).
        session, call, notifier = make_session()
        call.answer_result = CommandResult(success=False, error_code="TIMEOUT")
        await session.start()
        await call.trigger_hangup()
        names = notifier.names()
        assert "on_dialin_stopped" not in names
        assert "on_before_disconnect" not in names
        assert len(notifier.cancel_reasons) == 1  # cancel exactly once

    async def test_terminal_state_update_fires_once(self):
        session, call, notifier = make_session()
        await session.start()
        await call.trigger_hangup()
        await session._teardown()
        terminated = [
            p for n, p in notifier.events
            if n == "on_call_state_updated" and p == CallState.TERMINATED
        ]
        assert len(terminated) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_session.py -v -k TestTeardown`
Expected: FAIL with `NotImplementedError`

- [ ] **Step 3: Implement teardown** (replace the `_teardown` stub; add `close`)

```python
    async def close(self) -> None:
        """Self-initiated close (pipeline ended/cancelled). Fires disconnect
        events but never a worker cancel — the pipeline is already going down."""
        self._self_initiated = True
        try:
            await self._call.close()
        finally:
            # call.close() only fires on_hangup when a voice WS was open; run
            # teardown directly so events fire on every path (idempotent).
            await self._teardown()

    async def _teardown(self) -> None:
        if self._torn_down:
            return
        self._torn_down = True
        if self._connected_fired:
            payload = self._payload()
            # Awaited: the app's last chance to flush/log before disconnect.
            await self._notifier._fire("on_before_disconnect", payload)
            await self._notifier._fire("on_dialin_stopped", payload)
        await self._fire_state()
        if not self._self_initiated:
            await self._request_cancel("remote hangup")
```

- [ ] **Step 4: Run the full session suite**

Run: `uv run pytest tests/test_session.py -v`
Expected: ALL PASS (identity + start + teardown)

- [ ] **Step 5: Commit**

```bash
git add -u
git commit -m "feat: session teardown - idempotent choke point, matrix pinned"
```

---

### Task 6: AgentDuetTransport — event registry, alias layer, rate guard

**Files:**
- Create: `src/pipecat_agentduet/transport.py`
- Modify: `src/pipecat_agentduet/__init__.py`
- Create: `tests/test_transport.py`

- [ ] **Step 1: Write the failing tests**

```python
"""AgentDuetTransport: construction, events, and frame plumbing."""

import asyncio

import pytest
from agentduet import CallState
from pipecat.transports.base_transport import TransportParams

from pipecat_agentduet import AgentDuetTransport
from pipecat_agentduet._session import CallEventPayload
from tests.fakes import FakeCall


class TestConstruction:
    def test_default_params_enable_audio_and_pin_rates(self):
        call = FakeCall(sample_rate=8000)
        transport = AgentDuetTransport(call)
        assert transport._params.audio_in_enabled is True
        assert transport._params.audio_out_enabled is True
        assert transport._params.audio_in_sample_rate == 8000
        assert transport._params.audio_out_sample_rate == 8000

    def test_conflicting_user_rate_raises(self):
        call = FakeCall(sample_rate=16000)
        with pytest.raises(ValueError, match="sample rate"):
            AgentDuetTransport(
                call,
                params=TransportParams(
                    audio_in_enabled=True,
                    audio_out_enabled=True,
                    audio_out_sample_rate=24000,
                ),
            )

    def test_matching_user_rate_accepted(self):
        call = FakeCall(sample_rate=16000)
        AgentDuetTransport(
            call,
            params=TransportParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_in_sample_rate=16000,
            ),
        )  # no raise

    def test_call_property_exposes_sdk_call(self):
        call = FakeCall()
        assert AgentDuetTransport(call).call is call


class TestEventAliases:
    async def test_native_and_generic_fire_same_payload_same_moment(self):
        call = FakeCall()
        transport = AgentDuetTransport(call)
        seen: list[tuple[str, object]] = []

        @transport.event_handler("on_dialin_connected")
        async def on_native(t, payload):
            seen.append(("native", payload))

        @transport.event_handler("on_client_connected")
        async def on_generic(t, payload):
            seen.append(("generic", payload))

        payload = CallEventPayload(
            participant=call.participant, call_id=call.id, state=CallState.LIVE
        )
        await transport._fire("on_dialin_connected", payload)
        assert [name for name, _ in seen] == ["native", "generic"]
        assert seen[0][1] is seen[1][1]  # identical payload object

    async def test_all_v1_event_names_registered(self):
        # add_event_handler on an unknown name only warns, so pin the
        # registry directly.
        transport = AgentDuetTransport(FakeCall())
        for name in [
            "on_client_connected", "on_client_disconnected",
            "on_dialin_connected", "on_dialin_stopped", "on_dialin_error",
            "on_dialout_answered", "on_dialout_stopped", "on_dialout_error",
            "on_error", "on_call_state_updated", "on_before_disconnect",
        ]:
            assert name in transport._event_handlers
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_transport.py -v`
Expected: FAIL — `ImportError: cannot import name 'AgentDuetTransport'`

- [ ] **Step 3: Write `src/pipecat_agentduet/transport.py`** (transport shell; the input/output halves land in Tasks 7–8 — write their class stubs now so `input()`/`output()` compile)

```python
"""AgentDuetTransport — a Pipecat transport over an AgentDuet Call.

Pure frame plumbing and lifecycle translation: audio bytes pass through
untouched (both sides are mono s16 PCM at the call's fixed rate), and the
call lifecycle is owned by the shared _AgentDuetSession.
"""

import logging
import time

from agentduet import CallEvent
from agentduet.exceptions import BufferFullError, CallClosedError
from pipecat.frames.frames import (
    CancelFrame,
    CancelWorkerFrame,
    EndFrame,
    InputAudioRawFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    StartFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.transports.base_transport import BaseTransport, TransportParams

from pipecat_agentduet._session import _AgentDuetSession

logger = logging.getLogger(__name__)

# The generic pair is an alias layer: fired in the same code path as the
# native event — one signal, two names, one moment, one payload object.
_GENERIC_ALIASES = {
    "on_dialin_connected": "on_client_connected",
    "on_dialout_answered": "on_client_connected",
    "on_dialin_stopped": "on_client_disconnected",
    "on_dialout_stopped": "on_client_disconnected",
}

_EVENT_NAMES = (
    "on_client_connected",
    "on_client_disconnected",
    "on_dialin_connected",
    "on_dialin_stopped",
    "on_dialin_error",
    "on_dialout_answered",
    "on_dialout_stopped",
    "on_dialout_error",
    "on_error",
    "on_call_state_updated",
    "on_before_disconnect",
)


class AgentDuetTransport(BaseTransport):
    """Bridges a live AgentDuet Call and a Pipecat pipeline.

    Takes an attached-but-not-yet-answered Call; answers on pipeline start,
    closes the call on pipeline end, cancels the pipeline on remote hangup.
    """

    def __init__(self, call, params: TransportParams | None = None):
        super().__init__()
        rate = call.audio_config.sample_rate
        params = params or TransportParams(audio_in_enabled=True, audio_out_enabled=True)
        for attr in ("audio_in_sample_rate", "audio_out_sample_rate"):
            configured = getattr(params, attr)
            if configured is not None and configured != rate:
                raise ValueError(
                    f"{attr}={configured} conflicts with the call's fixed sample rate "
                    f"{rate}. The rate is set once, in CallAudioConfig; do not set it "
                    f"on the transport."
                )
        # Single source of truth: the call's rate wins everywhere. Copy so
        # the caller's params object isn't mutated behind their back.
        self._params = params.model_copy(
            update={"audio_in_sample_rate": rate, "audio_out_sample_rate": rate}
        )
        self._session = _AgentDuetSession(call, self)
        self._input: AgentDuetInputTransport | None = None
        self._output: AgentDuetOutputTransport | None = None

        call.on_call_event(CallEvent.ERROR)(self._on_call_error)
        for name in _EVENT_NAMES:
            # sync=True: handlers run awaited, not fire-and-forget in a task.
            # This is what makes "on_before_disconnect completes before the
            # disconnect events" and the alias pair's one-moment ordering real.
            self._register_event_handler(name, sync=True)

    @property
    def call(self):
        """The underlying SDK Call, for capabilities beyond the transport
        (whisper/barge/spy, session access)."""
        return self._session.call

    def input(self) -> "AgentDuetInputTransport":
        if not self._input:
            self._input = AgentDuetInputTransport(self._session, self._params)
        return self._input

    def output(self) -> "AgentDuetOutputTransport":
        if not self._output:
            self._output = AgentDuetOutputTransport(self._session, self._params)
        return self._output

    async def _on_call_error(self, data):
        await self._fire("on_error", data)

    async def _fire(self, event_name: str, payload) -> None:
        await self._call_event_handler(event_name, payload)
        generic = _GENERIC_ALIASES.get(event_name)
        if generic:
            await self._call_event_handler(generic, payload)

    async def _request_worker_cancel(self, reason: str) -> None:
        frame = CancelWorkerFrame(reason=reason)
        if self._input is not None:
            await self._input.push_frame(frame, FrameDirection.UPSTREAM)
        elif self._output is not None:
            await self._output.push_frame(frame, FrameDirection.DOWNSTREAM)
        # Neither half built: no pipeline exists, nothing to cancel.


class AgentDuetInputTransport(BaseInputTransport):
    """Pumps the remote party's audio_stream() into the pipeline."""

    def __init__(self, session: _AgentDuetSession, params: TransportParams, **kwargs):
        super().__init__(params, **kwargs)
        self._session = session

    # Task 7


class AgentDuetOutputTransport(BaseOutputTransport):
    """Writes pipeline audio into call.send_audio(); clears on interruption."""

    def __init__(self, session: _AgentDuetSession, params: TransportParams, **kwargs):
        super().__init__(params, **kwargs)
        self._session = session

    # Task 8
```

- [ ] **Step 4: Update `src/pipecat_agentduet/__init__.py`**

```python
"""AgentDuet transport for Pipecat."""

from pipecat_agentduet._session import CallEventPayload
from pipecat_agentduet.transport import AgentDuetTransport

__all__ = ["AgentDuetTransport", "CallEventPayload"]
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_transport.py tests/test_session.py -v`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add src/pipecat_agentduet/ tests/test_transport.py
git commit -m "feat: AgentDuetTransport shell - events, alias layer, rate guard"
```

---

### Task 7: Input transport — the audio pump

**Files:**
- Modify: `src/pipecat_agentduet/transport.py`
- Modify: `tests/test_transport.py`

These tests run a real minimal pipeline (`PipelineWorker` + `WorkerRunner`) rather than monkeypatching processor internals — more robust to Pipecat refactors. Shared helper first.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_transport.py`)

```python
from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.frame_processor import FrameProcessor
from pipecat.workers.runner import WorkerRunner


class FrameCapture(FrameProcessor):
    """Terminal processor that records every frame it sees."""

    def __init__(self):
        super().__init__()
        self.frames: list[Frame] = []

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        self.frames.append(frame)
        await self.push_frame(frame, direction)


def make_worker(pipeline: Pipeline) -> PipelineWorker:
    # Lean worker for tests: no idle cancel, no RTVI/turn-tracking extras.
    # check_dangling_tasks off: the spawned barge-in clear task may outlive
    # a short test worker by design (Task 8's 5 s slow-ack test).
    return PipelineWorker(
        pipeline,
        idle_timeout_secs=None,
        enable_rtvi=False,
        enable_turn_tracking=False,
        check_dangling_tasks=False,
    )


async def run_worker(worker: PipelineWorker) -> asyncio.Task:
    runner = WorkerRunner(handle_sigint=False)
    task = asyncio.create_task(runner.run(worker))
    return task


class TestInputTransport:
    async def test_remote_audio_becomes_tagged_input_frames(self):
        call = FakeCall(sample_rate=8000)
        transport = AgentDuetTransport(call)
        capture = FrameCapture()
        worker = make_worker(Pipeline([transport.input(), capture]))
        run_task = await run_worker(worker)

        call.caller.audio_queue.put_nowait(b"\x01\x02" * 80)
        await asyncio.sleep(0.2)

        audio = [f for f in capture.frames if isinstance(f, InputAudioRawFrame)]
        assert audio, "no InputAudioRawFrame reached the pipeline"
        assert audio[0].audio == b"\x01\x02" * 80  # bytes untouched
        assert audio[0].sample_rate == 8000  # tagged with the call's rate
        assert audio[0].num_channels == 1

        await call.trigger_hangup()
        await asyncio.wait_for(run_task, timeout=5)

    async def test_remote_hangup_cancels_the_worker(self):
        call = FakeCall()
        transport = AgentDuetTransport(call)
        worker = make_worker(Pipeline([transport.input(), FrameCapture()]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)  # let StartFrame/answer complete

        await call.trigger_hangup()
        # CancelWorkerFrame -> CancelFrame -> worker ends -> runner returns.
        await asyncio.wait_for(run_task, timeout=5)

    async def test_pipeline_end_closes_the_call(self):
        call = FakeCall()
        transport = AgentDuetTransport(call)
        worker = make_worker(Pipeline([transport.input(), FrameCapture()]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)

        await worker.stop_when_done()  # graceful EndFrame path
        await asyncio.wait_for(run_task, timeout=5)
        assert call.close_calls >= 1  # transport closed the call, no user code
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_transport.py -v -k TestInputTransport`
Expected: FAIL — input half has no `start()`; no audio frames captured / worker never ends.

- [ ] **Step 3: Implement the input half** (replace the `# Task 7` stub)

```python
    async def start(self, frame: StartFrame):
        await super().start(frame)
        # Ready first: _audio_in_queue exists only after set_transport_ready,
        # and audio can arrive the instant answer() succeeds server-side.
        await self.set_transport_ready(frame)
        # Pump before answer: audio_stream() is order-independent and lazily
        # bound, so no first words are dropped while the pipeline wires up.
        self._pump_task = self.create_task(self._pump())
        await self._session.start()

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        await self._shutdown()

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        await self._shutdown()

    async def _pump(self):
        rate = self._session.sample_rate
        # Ends cleanly on termination: the stream raises StopAsyncIteration.
        async for chunk in self._session.remote_party.audio_stream():
            await self.push_audio_frame(
                InputAudioRawFrame(audio=chunk, sample_rate=rate, num_channels=1)
            )

    async def _shutdown(self):
        if self._pump_task is not None:
            await self.cancel_task(self._pump_task)
            self._pump_task = None
        await self._session.close()
```

Also add `self._pump_task = None` to `AgentDuetInputTransport.__init__`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_transport.py -v`
Expected: ALL PASS. If a test hangs, the 30 s pytest-timeout dumps stacks — check that `CancelWorkerFrame` is being pushed (session → transport `_request_worker_cancel`) and that `set_transport_ready` was reached.

- [ ] **Step 5: Commit**

```bash
git add -u
git commit -m "feat: input transport - audio pump, hangup cancel, close on end"
```

---

### Task 8: Output transport — write path and interruption

**Files:**
- Modify: `src/pipecat_agentduet/transport.py`
- Modify: `tests/test_transport.py`

- [ ] **Step 1: Write the failing tests** (append)

```python
from agentduet.exceptions import BufferFullError


# One 10 ms chunk at 16 kHz mono s16: 160 samples * 2 bytes. MediaSender
# buffers output audio to audio_out_10ms_chunks x 10 ms before calling
# write_audio_frame, so tests use chunk_size multiples and set the chunking
# to 1 to make writes deterministic.
CHUNK = 320


def make_output_frame(pcm: bytes) -> OutputAudioRawFrame:
    return OutputAudioRawFrame(audio=pcm, sample_rate=16000, num_channels=1)


class TestOutputTransport:
    async def _run_output(self, call: FakeCall):
        from pipecat.transports.base_transport import TransportParams

        transport = AgentDuetTransport(
            call,
            params=TransportParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_out_10ms_chunks=1,  # flush every 10 ms chunk
            ),
        )
        output = transport.output()
        worker = make_worker(Pipeline([output]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)  # StartFrame processed, answer done
        return transport, output, worker, run_task

    async def _finish(self, worker, run_task):
        await worker.stop_when_done()
        await asyncio.wait_for(run_task, timeout=5)

    async def test_output_audio_reaches_send_audio_byte_identical(self):
        call = FakeCall()
        transport, output, worker, run_task = await self._run_output(call)
        pcm = b"\x03\x04" * (CHUNK // 2)  # exactly one chunk
        await worker.queue_frame(make_output_frame(pcm))
        await asyncio.sleep(0.3)
        assert b"".join(call.sent_audio) == pcm
        await self._finish(worker, run_task)

    async def test_buffer_full_drops_chunk_pipeline_survives(self):
        call = FakeCall()
        transport, output, worker, run_task = await self._run_output(call)
        call.send_audio_error = BufferFullError("full")
        await worker.queue_frame(make_output_frame(b"\x00" * CHUNK))
        await asyncio.sleep(0.2)
        call.send_audio_error = None
        second = b"\x07\x08" * (CHUNK // 2)
        await worker.queue_frame(make_output_frame(second))
        await asyncio.sleep(0.2)
        # First chunk dropped on BufferFullError, second delivered intact.
        assert b"".join(call.sent_audio) == second
        await self._finish(worker, run_task)

    async def test_interruption_clears_buffer_without_blocking_frames(self):
        call = FakeCall()
        call.clear_delay = 5.0  # slow server ack: must NOT stall the pipeline
        transport, output, worker, run_task = await self._run_output(call)

        await worker.queue_frame(InterruptionFrame())
        await asyncio.sleep(0.2)
        assert call.clear_calls == 1  # clear was invoked...

        # ...and the frame path is still live: audio written well before the
        # 5 s ack resolves.
        pcm = b"\x0a\x0b" * (CHUNK // 2)
        await worker.queue_frame(make_output_frame(pcm))
        await asyncio.sleep(0.3)
        assert b"".join(call.sent_audio) == pcm
        await self._finish(worker, run_task)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_transport.py -v -k TestOutputTransport`
Expected: FAIL — output half has no `start()`/`write_audio_frame`.

- [ ] **Step 3: Implement the output half** (replace the `# Task 8` stub)

```python
    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self._session.start()  # latched no-op if the input half won
        await self.set_transport_ready(frame)

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        await self._session.close()

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        await self._session.close()

    async def write_audio_frame(self, frame: OutputAudioRawFrame) -> bool:
        try:
            await self._session.call.send_audio(frame.audio)
            return True
        except BufferFullError:
            # Back-pressure, not teardown: the default ring buffer holds ~32 s
            # at 16 kHz, so a full buffer means the server pull has stalled.
            logger.warning("outgoing audio buffer full; dropping %d bytes", len(frame.audio))
            return False
        except CallClosedError:
            # Termination races the send loop by design (SDK contract);
            # teardown runs from the hangup event, not from here.
            return False

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InterruptionFrame):
            # Spawned, not awaited: the local ring-buffer flush inside is
            # instant, but the agent.interrupt ack can take up to 10 s and
            # must not stall the system-frame path. Residual race accepted:
            # an ack landing after the next bot turn begins could flush that
            # turn's audio, but the next turn needs a full STT->LLM->TTS
            # round trip while the ack needs one network hop.
            self.create_task(self._clear_remote_buffer())

    async def _clear_remote_buffer(self):
        started = time.monotonic()
        try:
            result = await self._session.call.clear_send_audio_buffer()
        except CallClosedError:
            return  # call over; nothing left to clear
        elapsed_ms = (time.monotonic() - started) * 1000
        if result:
            # The two numbers the docs want: how much was flushed, how fast.
            logger.info(
                "barge-in: cleared %s buffered bytes, ack in %.0f ms",
                result.payload,
                elapsed_ms,
            )
        else:
            logger.warning("agent.interrupt failed: %s", result.error_code)
```

- [ ] **Step 4: Run the whole suite + lint**

Run: `uv run pytest -v && uv run ruff check .`
Expected: ALL PASS, no lint errors

- [ ] **Step 5: Commit**

```bash
git add -u
git commit -m "feat: output transport - send_audio path, non-blocking barge-in clear"
```

---

### Task 9: Example — `inbound_tone_bot.py`

**Files:**
- Create: `examples/inbound_tone_bot.py`

No test file: this is the live-validation artifact. It must import cleanly, which is checked below.

- [ ] **Step 1: Sync the example group**

Run: `uv sync --group dev --group example`
Expected: installs `onnxruntime` (Silero) without dependency conflicts

- [ ] **Step 2: Write `examples/inbound_tone_bot.py`**

```python
"""Keyless spike bot: answers an inbound call, plays a tone after each user
turn, stops the tone on barge-in. No STT/LLM/TTS keys needed.

Env: AGENTDUET_API_KEY, AGENTDUET_CONNECTOR_UUID, optional AGENTDUET_BASE_URL.
Run:  uv run --group example python examples/inbound_tone_bot.py
Then call the connector's number. Speak; after you stop, a tone plays for up
to 5 s. Speak over it: it must stop (barge-in). Watch the log for the
"barge-in: cleared N buffered bytes, ack in M ms" measurement lines.
"""

import asyncio
import logging
import math
import os
import struct
import uuid

from dotenv import load_dotenv

from agentduet import (
    CallAudioConfig,
    IncomingCallNotification,
    SessionManager,
    SessionManagerConfig,
)
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import (
    Frame,
    InterruptionFrame,
    OutputAudioRawFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.audio.vad_processor import VADProcessor
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.turns.user_turn_processor import UserTurnProcessor
from pipecat.workers.runner import WorkerRunner

from pipecat_agentduet import AgentDuetTransport

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("inbound_tone_bot")

SAMPLE_RATE = 16000  # Silero VAD supports 8k/16k only; 16k is the default


class ToneBot(FrameProcessor):
    """Plays a sine tone after each user turn; stops it on interruption."""

    def __init__(self, *, freq: float = 440.0, seconds: float = 5.0, amplitude: float = 0.3):
        super().__init__()
        self._freq = freq
        self._seconds = seconds
        self._amplitude = amplitude
        self._gen_task = None

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InterruptionFrame):
            if self._gen_task is not None:
                await self.cancel_task(self._gen_task)
                self._gen_task = None
        elif isinstance(frame, UserStoppedSpeakingFrame):
            if self._gen_task is not None:
                await self.cancel_task(self._gen_task)
            self._gen_task = self.create_task(self._play_tone())
        await self.push_frame(frame, direction)

    async def _play_tone(self):
        chunk_samples = SAMPLE_RATE // 50  # 20 ms
        total_chunks = int(self._seconds * 50)
        peak = int(32767 * self._amplitude)
        sample_index = 0
        for _ in range(total_chunks):
            samples = [
                int(peak * math.sin(2 * math.pi * self._freq * (sample_index + i) / SAMPLE_RATE))
                for i in range(chunk_samples)
            ]
            sample_index += chunk_samples
            pcm = struct.pack(f"<{chunk_samples}h", *samples)
            await self.push_frame(
                OutputAudioRawFrame(audio=pcm, sample_rate=SAMPLE_RATE, num_channels=1)
            )


async def run_call(sm: SessionManager, noti: IncomingCallNotification):
    session = await sm.open_session(uuid.uuid4().hex, noti.subscriber)
    call = await session.process_call(noti)
    transport = AgentDuetTransport(call)

    @transport.event_handler("on_client_connected")
    async def on_connected(t, payload):
        logger.info("connected: %s (call %s)", payload.participant.value, payload.call_id)

    @transport.event_handler("on_client_disconnected")
    async def on_disconnected(t, payload):
        logger.info("disconnected: %s (call %s)", payload.participant.value, payload.call_id)

    pipeline = Pipeline(
        [
            transport.input(),
            VADProcessor(vad_analyzer=SileroVADAnalyzer()),
            UserTurnProcessor(),  # bare: interruptions default to enabled
            ToneBot(),
            transport.output(),
        ]
    )
    # idle_timeout off (a silent caller isn't an idle pipeline); RTVI and
    # turn-tracking stay at their defaults — the tests disable them only
    # for isolation.
    worker = PipelineWorker(pipeline, idle_timeout_secs=None)
    runner = WorkerRunner(handle_sigint=False)
    await runner.run(worker)
    logger.info("pipeline finished for call %s", call.id)


async def main():
    load_dotenv()
    config = SessionManagerConfig.create(
        api_key=os.environ["AGENTDUET_API_KEY"],
        connector_uuid=os.environ["AGENTDUET_CONNECTOR_UUID"],
        base_url=os.getenv("AGENTDUET_BASE_URL"),
        call_audio=CallAudioConfig(sample_rate=SAMPLE_RATE),
    )
    async with SessionManager(config) as sm:

        @sm.on_incoming_call
        async def on_call(noti: IncomingCallNotification):
            # Own task: never block the SDK event bus for the call's duration.
            asyncio.create_task(run_call(sm, noti))

        logger.info("listening for calls…")
        await sm.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 3: Verify it imports and the pipeline constructs**

Run: `uv run --group example python -c "
import examples.inbound_tone_bot as bot
print('imports ok')
"`
Expected: `imports ok` (Silero model may download on first VAD construction at runtime — that is fine and happens on the first call, not at import)

- [ ] **Step 4: Verify the SDK config call matches the installed SDK**

Run: `uv run python -c "
import inspect
from agentduet import SessionManagerConfig
print(inspect.signature(SessionManagerConfig.create))
"`
Expected: signature includes `api_key`, `connector_uuid`, `base_url`, `call_audio`. If it differs, fix the example to match — the installed SDK is the source of truth.

- [ ] **Step 5: Commit**

```bash
git add examples/inbound_tone_bot.py
git commit -m "feat: keyless inbound tone-bot example"
```

---

### Task 10: Outbound track probe + live validation

**Files:**
- Create: `examples/outbound_track_probe.py`

- [ ] **Step 1: Write `examples/outbound_track_probe.py`**

```python
"""One-shot probe: dial out, then report which track (caller/callee) carries
the remote party's audio. Settles the VoiceAgent caller/callee discrepancy
with data before the outbound transport round is designed.

Env: AGENTDUET_API_KEY, AGENTDUET_CONNECTOR_UUID, optional AGENTDUET_BASE_URL,
PROBE_SUBSCRIBER (the line to call from), PROBE_DEST (E.164 number to call).
Run:  uv run python examples/outbound_track_probe.py
Answer the phone and speak; the probe logs bytes per track for 10 s.
"""

import asyncio
import logging
import os
import uuid

from agentduet import Address, CallAudioConfig, SessionManager, SessionManagerConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("probe")


async def count_bytes(name: str, party, results: dict):
    total = 0
    try:
        async for chunk in party.audio_stream():
            total += len(chunk)
            results[name] = total
    except asyncio.CancelledError:
        results[name] = total
        raise


async def main():
    config = SessionManagerConfig.create(
        api_key=os.environ["AGENTDUET_API_KEY"],
        connector_uuid=os.environ["AGENTDUET_CONNECTOR_UUID"],
        base_url=os.getenv("AGENTDUET_BASE_URL"),
        call_audio=CallAudioConfig(sample_rate=16000),
    )
    async with SessionManager(config) as sm:
        session = await sm.open_session(uuid.uuid4().hex, os.environ["PROBE_SUBSCRIBER"])
        call = await session.make_call(Address.telco(os.environ["PROBE_DEST"]))
        logger.info("dialing %s (call %s)…", os.environ["PROBE_DEST"], call.id)
        result = await call.dial()
        if not result:
            logger.error("dial failed: %s", result.error_code)
            return
        logger.info("answered. speak into the phone; sampling both tracks for 10 s…")
        results: dict[str, int] = {}
        tasks = [
            asyncio.create_task(count_bytes("caller(track0)", call.caller, results)),
            asyncio.create_task(count_bytes("callee(track1)", call.callee, results)),
        ]
        await asyncio.sleep(10)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("subscriber=%s caller=%s callee=%s", call.subscriber, call.caller, call.callee)
        logger.info("bytes per track: %s", results)
        logger.info(
            "=> the remote party's audio is on the track with the (much) larger count"
        )
        await call.close()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Verify it imports**

Run: `uv run python -c "import examples.outbound_track_probe; print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add examples/outbound_track_probe.py
git commit -m "feat: outbound track probe for the caller/callee question"
```

- [ ] **Step 4: Live validation (manual — requires AgentDuet credentials; a human runs these)**

Run `uv run --group example python examples/inbound_tone_bot.py`, call the connector's number, and check off:

1. Speak, pause → tone plays. Speak over the tone → it stops. Note the log's `barge-in: cleared N buffered bytes, ack in M ms` lines — **these numbers go in the docs**.
2. Hang up mid-tone → log shows `disconnected:` once, `pipeline finished`, process still listening for the next call.
3. Call and hang up while it's still ringing → no `connected:` line ever, pipeline torn down, process healthy.
4. Call twice in a row → second call works (fresh transport per call).

Then run the probe (`PROBE_SUBSCRIBER=… PROBE_DEST=… uv run python examples/outbound_track_probe.py`), answer, speak, and record which track carried your voice. **Report the finding** (expected: callee/track1, which would confirm the `VoiceAgent._bridge` outbound bug) — file it against the SDK repo, do not fix it here.

- [ ] **Step 5: Record results**

Append a `## Spike results` section to `specs/2026-08-03-agentduet-transport-spike-design.md` with: barge-in cutoff numbers, the track-probe finding, and any teardown surprises. Commit:

```bash
git add specs/2026-08-03-agentduet-transport-spike-design.md
git commit -m "docs: record spike live-validation results"
```
