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
