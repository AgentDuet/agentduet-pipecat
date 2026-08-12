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

    def __init__(
        self,
        call,
        params: TransportParams | None = None,
        *,
        ring_time_seconds: int = 60,
    ):
        super().__init__()
        # Validated here, not left to the SDK: dial() runs inside a spawned
        # task after pipeline start, where a ValueError would be invisible.
        if not 1 <= ring_time_seconds <= 120:
            raise ValueError("ring_time_seconds must be between 1 and 120")
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
        self._session = _AgentDuetSession(call, self, ring_time_seconds=ring_time_seconds)
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
            self._session.register_half()
        return self._input

    def output(self) -> "AgentDuetOutputTransport":
        if not self._output:
            self._output = AgentDuetOutputTransport(self._session, self._params)
            self._session.register_half()
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
        self._pump_task = None

    async def start(self, frame: StartFrame):
        await super().start(frame)
        # Ready first: _audio_in_queue exists only after set_transport_ready,
        # and audio can arrive the instant answer() succeeds server-side.
        await self.set_transport_ready(frame)
        # Pump before answer: audio_stream() is order-independent and lazily
        # bound, so no first words are dropped while the pipeline wires up.
        self._pump_task = self.create_task(self._pump())
        # Inbound only: a user-initiated CancelFrame arriving here queues
        # behind this in-flight answer() (up to its ~60 s ring timeout) since
        # start() must return before cancel() runs on this same processor.
        # Remote hangup is unaffected: it aborts answer() itself via
        # CallClosedError. On outbound, session.start() returns as soon as
        # the dial() task is spawned, so nothing queues behind it.
        await self._session.start()

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        await self._cancel_pump()
        # Not a close: BaseInputTransport pushes EndFrame downstream *before*
        # stop() runs, so farewell audio is still traversing the pipeline.
        # The session self-closes once every registered half has stopped.
        await self._session.half_stopped()

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        await self._cancel_pump()
        # Cancel drops everything in flight by design: close immediately.
        await self._session.close()

    async def _pump(self):
        rate = self._session.sample_rate
        # Ends cleanly on termination: the stream raises StopAsyncIteration.
        async for chunk in self._session.remote_party.audio_stream():
            await self.push_audio_frame(
                InputAudioRawFrame(audio=chunk, sample_rate=rate, num_channels=1)
            )

    async def _cancel_pump(self):
        if self._pump_task is not None:
            await self.cancel_task(self._pump_task)
            self._pump_task = None


class AgentDuetOutputTransport(BaseOutputTransport):
    """Writes pipeline audio into call.send_audio(); clears on interruption."""

    def __init__(self, session: _AgentDuetSession, params: TransportParams, **kwargs):
        super().__init__(params, **kwargs)
        self._session = session
        self._clear_task = None
        self._logged_closed_drop = False

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self._session.start()  # latched no-op if the input half won
        await self.set_transport_ready(frame)

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        await self._cancel_clear_task()
        # By the time BaseOutputTransport.stop() returns, MediaSender has
        # drained its queue and appended end-silence, so half_stopped() (not
        # an immediate close) is safe here too.
        await self._session.half_stopped()

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        await self._cancel_clear_task()
        await self._session.close()

    async def _cancel_clear_task(self):
        # A barge-in shortly before hangup must not leave this task dangling
        # past the worker — production workers warn on dangling tasks by
        # default.
        if self._clear_task is not None:
            await self.cancel_task(self._clear_task)
            self._clear_task = None

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
            if not self._logged_closed_drop:
                self._logged_closed_drop = True
                logger.debug("send_audio after call closed; dropping remaining output")
            return False

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InterruptionFrame):
            # Spawned, not awaited: the local ring-buffer flush inside is
            # instant, but the agent.interrupt ack can take up to 10 s and
            # must not stall the system-frame path. Residual race accepted:
            # an ack landing after the next bot turn begins could flush that
            # turn's audio, but the next turn needs a full STT->LLM->TTS
            # round trip while the ack needs one network hop. Overwriting a
            # still-running previous clear task is fine: clear is idempotent
            # and a newer interruption supersedes the older one.
            self._clear_task = self.create_task(self._clear_remote_buffer())

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
