"""AgentDuetTransport — a Pipecat transport over an AgentDuet Call.

Pure frame plumbing and lifecycle translation: audio bytes pass through
untouched (both sides are mono s16 PCM at the call's fixed rate), and the
call lifecycle is owned by the shared _AgentDuetSession.
"""

import logging

from agentduet import CallEvent
from pipecat.frames.frames import CancelWorkerFrame
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
        self._pump_task = None

    # Implemented in Task 7.


class AgentDuetOutputTransport(BaseOutputTransport):
    """Writes pipeline audio into call.send_audio(); clears on interruption."""

    def __init__(self, session: _AgentDuetSession, params: TransportParams, **kwargs):
        super().__init__(params, **kwargs)
        self._session = session

    # Implemented in Task 8.
