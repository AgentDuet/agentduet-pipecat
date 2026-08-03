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
