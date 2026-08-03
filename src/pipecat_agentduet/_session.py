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
        raise NotImplementedError  # next slice
