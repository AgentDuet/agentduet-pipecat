"""_AgentDuetSession: identity, answer paths, teardown matrix."""

import asyncio

from agentduet import CallState, CommandResult
from agentduet.exceptions import CallClosedError

from pipecat_agentduet._session import CallEventPayload, _AgentDuetSession
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
        assert call.answer_calls == 1

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
        session, _call, notifier = make_session()
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

    async def test_close_racing_inflight_start_no_error_no_cancel(self):
        # Pipeline cancel while answer() is in flight: close() wins, and the
        # aborted start must not report an error or push a worker cancel.
        session, call, notifier = make_session()
        call.answer_gate = asyncio.Event()
        start_task = asyncio.create_task(session.start())
        await asyncio.sleep(0)  # start() is now awaiting the gate
        await session.close()   # self-initiated; terminates the call
        call.answer_gate.set()  # answer() now raises CallClosedError
        await start_task
        names = notifier.names()
        assert "on_dialin_error" not in names
        assert "on_dialin_connected" not in names
        assert notifier.cancel_reasons == []


class TestDirectionDetection:
    async def test_outbound_shell_detected_by_caller_eq_subscriber(self):
        session = _AgentDuetSession(FakeCall.outbound(), RecordingNotifier())
        assert session._outbound is True

    async def test_inbound_call_detected_despite_new_state(self):
        # Real inbound calls are ALSO CallState.NEW at construction; only the
        # caller/subscriber relation discriminates.
        call = FakeCall()
        assert call.state == CallState.NEW
        session = _AgentDuetSession(call, RecordingNotifier())
        assert session._outbound is False
