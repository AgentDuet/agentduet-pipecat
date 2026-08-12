"""Test doubles for the public agentduet surface the transport touches."""

import asyncio

from agentduet import Address, CallEvent, CallState, CommandResult, Network
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
        # Real Call.__init__ sets NEW unconditionally; process_call never
        # changes it. Direction is NOT observable from state — only from the
        # caller/subscriber relationship.
        self.state = CallState.NEW

        # Scripting knobs
        self.answer_result: CommandResult | Exception = CommandResult(success=True)
        self.answer_gate: asyncio.Event | None = None  # answer() awaits this if set
        self.send_audio_error: Exception | None = None
        self.clear_delay: float = 0.0
        self.clear_result = CommandResult(success=True, payload=0)
        self.dial_result: CommandResult | Exception = CommandResult(success=True)
        self.dial_gate: asyncio.Event | None = None  # dial() awaits this if set

        # Recorders
        self.sent_audio: list[bytes] = []
        self.answer_calls: int = 0
        self.dial_calls: int = 0
        self.dial_ring_time: int | None = None
        self.clear_calls: int = 0
        self.close_calls: int = 0
        self._hangup_handlers: list = []
        self._event_handlers: dict = {}

    @classmethod
    def outbound(
        cls,
        *,
        subscriber: str = "+6511111111",
        dest_value: str = "+6533333333",
        sample_rate: int = 16000,
    ) -> "FakeCall":
        """An outbound-dial shell as Session.make_call mints it:
        caller == subscriber, callee == participant == dest (state is NEW
        for every call; direction lives in the caller/subscriber relation)."""
        call = cls(
            subscriber=subscriber,
            caller_value=subscriber,
            callee_value=dest_value,
            sample_rate=sample_rate,
        )
        call.participant = Address(Network.TELCO, dest_value)
        return call

    # -- registration ------------------------------------------------------
    def on_hangup(self, func):
        self._hangup_handlers.append(func)
        return func

    def on_call_event(self, event_name):
        def decorator(func):
            self._event_handlers.setdefault(str(event_name), []).append(func)
            return func

        return decorator

    # -- commands ----------------------------------------------------------
    async def answer(self) -> CommandResult:
        self.answer_calls += 1
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

    async def dial(self, *, ring_time_seconds: int = 60) -> CommandResult:
        self.dial_calls += 1
        self.dial_ring_time = ring_time_seconds
        if self.dial_gate is not None:
            await self.dial_gate.wait()
        if self.state == CallState.TERMINATED and not isinstance(
            self.dial_result, Exception
        ):
            raise CallClosedError()
        if isinstance(self.dial_result, Exception):
            raise self.dial_result
        if self.dial_result.success:
            self.state = CallState.LIVE
        else:
            # Real dial() force-closes the voice WS on CALL_UNANSWERED/TIMEOUT,
            # and the voice layer synthesizes a HANGUP that fires handlers.
            # Modeled as hangup-before-return (the tighter ordering).
            await self.trigger_hangup()
        return self.dial_result

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

    async def trigger_error(self, data) -> None:
        """Fire whatever handlers were registered for CallEvent.ERROR."""
        for handler in self._event_handlers.get(str(CallEvent.ERROR), []):
            await handler(data)

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
