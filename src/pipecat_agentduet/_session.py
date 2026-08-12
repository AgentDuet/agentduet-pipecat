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
from agentduet.exceptions import CallClosedError, CallError

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

    def __init__(self, call, notifier, *, ring_time_seconds: int = 60):
        self._call = call
        self._notifier = notifier
        # Direction: on a make_call shell the SDK sets caller == subscriber; on
        # an IncomingCallNotification attach, caller is the external party. Same
        # public predicate remote_party uses — state can't discriminate (every
        # Call constructs in NEW) and _origin is private. Known limit: an
        # OutgoingCallNotification attach (copilot shape, v1 non-goal) also has
        # caller == subscriber; dial() on it raises CallStateError, which the
        # establish path maps to a clean on_dialout_error + cancel. Durable fix
        # is a public Call.origin on the SDK (feature request filed).
        self._outbound = call.caller.value == call.subscriber
        self._ring_time_seconds = ring_time_seconds
        if self._outbound:
            self._connected_event = "on_dialout_answered"
            self._stopped_event = "on_dialout_stopped"
            self._error_event = "on_dialout_error"
            self._fail_reason = "dial failed"
        else:
            self._connected_event = "on_dialin_connected"
            self._stopped_event = "on_dialin_stopped"
            self._error_event = "on_dialin_error"
            self._fail_reason = "answer failed"
        self._start_begun = False
        self._start_complete = asyncio.Event()
        self._establish_task: asyncio.Task | None = None
        self._connected_fired = False
        self._torn_down = False
        self._self_initiated = False
        self._cancel_requested = False
        self._terminal_state_fired = False
        # Half-latch (Daily's _leave_counter pattern): the session only
        # self-closes once every constructed transport half has reported its
        # EndFrame stop, so farewell audio in flight between input and output
        # isn't dropped by an early close.
        self._registered_halves = 0
        self._stopped_halves = 0

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
        """Establish the call: answer (inbound) or dial (outbound).

        Latched: the first caller establishes, later callers wait for start to
        complete. Inbound answer is awaited inline (fast, 10 s command timeout);
        outbound dial is spawned as a task because dial() blocks until answered
        or failed (up to ring_time_seconds) and must not stall pipeline startup
        (parent spec §5). "Answered" is the resolution of that task.
        """
        if self._start_begun:
            await self._start_complete.wait()
            return
        self._start_begun = True
        try:
            # Hangup can arrive in any state, including mid-answer/mid-dial:
            # register the handler before establishing so the window is closed.
            self._call.on_hangup(self._on_hangup)
            if self._call.state == CallState.TERMINATED:
                await self._fail_start(_terminated_result())
                return
            if self._outbound:
                self._establish_task = asyncio.create_task(self._establish())
                # _establish handles every SDK-contract exception itself; this
                # backstop only surfaces genuinely unexpected bugs, which would
                # otherwise hide until GC logs "exception was never retrieved".
                self._establish_task.add_done_callback(self._log_establish_crash)
            else:
                await self._establish()
        finally:
            self._start_complete.set()

    @staticmethod
    def _log_establish_crash(task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception() is not None:
            logger.error("dial task crashed", exc_info=task.exception())

    async def _establish(self) -> None:
        """Answer or dial, then fire connected events on a truthy result with
        no teardown racing it. Self-resolving on every path: a hangup or close
        mid-flight aborts the SDK command with CallClosedError/CallStateError,
        so this task never needs external cancellation."""
        try:
            if self._outbound:
                result = await self._call.dial(ring_time_seconds=self._ring_time_seconds)
            else:
                result = await self._call.answer()
        except CallClosedError:
            await self._fail_start(_terminated_result())
            return
        except CallError as e:
            # Covers CallStateError too (a hangup landing between task spawn
            # and dial()'s state check surfaces as CallStateError, not
            # CallClosedError) and voice-WS open failures on either path.
            if self._call.state == CallState.TERMINATED:
                await self._fail_start(_terminated_result())
            else:
                await self._fail_start(
                    CommandResult(success=False, error_code="CALL_ERROR", error_message=str(e))
                )
            return
        if not result:
            await self._fail_start(result)
            return
        if self._torn_down or self._call.state == CallState.TERMINATED:
            # A hangup raced the truthy result; teardown ran (or will run)
            # from on_hangup. Connected events must never fire.
            # Deliberate silence (spec matrix row 4): the hangup path owns
            # all signaling for this call.
            return
        self._connected_fired = True
        await self._notifier._fire(self._connected_event, self._payload())
        await self._fire_state()

    async def _fail_start(self, result: CommandResult) -> None:
        if self._self_initiated:
            # close() raced an in-flight answer(): close()'s own _teardown
            # (in its finally) owns all signaling here. Reporting an error or
            # requesting a cancel for our own deliberate close is spurious.
            return
        already_torn_down = self._torn_down
        self._torn_down = True
        logger.debug("call %s %s: %s", self._call.id, self._fail_reason, result.error_code)
        await self._notifier._fire(self._error_event, result)
        if not already_torn_down:
            await self._fire_state()
        await self._request_cancel(self._fail_reason)

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

    def register_half(self) -> None:
        """Called once per constructed transport half (input/output).

        Tracks how many halves must report ``half_stopped()`` before the
        session closes the call on the graceful EndFrame path.
        """
        self._registered_halves += 1

    async def half_stopped(self) -> None:
        """A transport half finished its EndFrame stop.

        When every registered half has reported, self-close — this is the
        graceful (non-cancel) path, so audio already handed off between
        halves gets a chance to actually be sent before the call closes.
        """
        self._stopped_halves += 1
        if self._stopped_halves >= self._registered_halves:
            await self.close()

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
        logger.debug(
            "call %s teardown (self_initiated=%s, connected=%s)",
            self._call.id,
            self._self_initiated,
            self._connected_fired,
        )
        if self._connected_fired:
            payload = self._payload()
            # Awaited: the app's last chance to flush/log before disconnect.
            await self._notifier._fire("on_before_disconnect", payload)
            await self._notifier._fire(self._stopped_event, payload)
        await self._fire_state()
        if not self._self_initiated:
            await self._request_cancel("remote hangup")
