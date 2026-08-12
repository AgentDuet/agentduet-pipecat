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


class TestOutboundStart:
    async def test_start_returns_before_dial_resolves(self):
        call = FakeCall.outbound()
        call.dial_gate = asyncio.Event()  # never released in this test
        session = _AgentDuetSession(call, RecordingNotifier())
        await asyncio.wait_for(session.start(), timeout=1.0)  # must not block on dial
        assert session._establish_task is not None
        # start() has no yield points, so the spawned task hasn't run yet —
        # yield once before checking dial was actually initiated. Do NOT "fix"
        # a failure here by making start() await the dial: the spawned task IS
        # the design (spec §5: dial must not stall pipeline startup).
        await asyncio.sleep(0)
        assert call.dial_calls == 1

    async def test_dial_success_fires_answered_then_state(self):
        call = FakeCall.outbound()
        notifier = RecordingNotifier()
        session = _AgentDuetSession(call, notifier)
        await session.start()
        await session._establish_task
        assert notifier.names() == ["on_dialout_answered", "on_call_state_updated"]
        payload = notifier.events[0][1]
        assert payload.participant.value == "+6533333333"
        assert payload.state == CallState.LIVE

    async def test_ring_time_passed_through(self):
        call = FakeCall.outbound()
        session = _AgentDuetSession(call, RecordingNotifier(), ring_time_seconds=30)
        await session.start()
        await session._establish_task
        assert call.dial_ring_time == 30

    async def test_dial_falsy_fires_error_and_exactly_one_cancel(self):
        # A falsy dial also fires on_hangup (the SDK force-closes the voice WS,
        # see verified facts), so the cancel REASON races between "dial failed"
        # and "remote hangup" — pin the invariants, not the reason string.
        call = FakeCall.outbound()
        call.dial_result = CommandResult(success=False, error_code="CALL_UNANSWERED")
        notifier = RecordingNotifier()
        session = _AgentDuetSession(call, notifier)
        await session.start()
        await session._establish_task
        idx = notifier.names().index("on_dialout_error")
        assert notifier.events[idx][1].error_code == "CALL_UNANSWERED"
        assert len(notifier.cancel_reasons) == 1
        assert "on_dialout_answered" not in notifier.names()
        assert "on_dialout_stopped" not in notifier.names()

    async def test_terminated_before_start_fires_error_without_dialing(self):
        call = FakeCall.outbound()
        call.state = CallState.TERMINATED
        notifier = RecordingNotifier()
        session = _AgentDuetSession(call, notifier)
        await session.start()
        assert call.dial_calls == 0
        assert notifier.events[0][0] == "on_dialout_error"
        assert notifier.events[0][1].error_code == "CALL_TERMINATED"
        assert notifier.cancel_reasons == ["dial failed"]


class TestOutboundTeardown:
    async def test_hangup_mid_dial_resolves_task_no_connected_events(self):
        call = FakeCall.outbound()
        call.dial_gate = asyncio.Event()
        notifier = RecordingNotifier()
        session = _AgentDuetSession(call, notifier)
        await session.start()
        await call.trigger_hangup()          # remote gives up while ringing
        call.dial_gate.set()                 # dial now observes TERMINATED
        await asyncio.wait_for(session._establish_task, timeout=1.0)
        names = notifier.names()
        assert "on_dialout_answered" not in names
        assert "on_dialout_stopped" not in names
        assert "on_dialout_error" in names   # CALL_TERMINATED, matrix row 2 twin
        assert "remote hangup" in notifier.cancel_reasons

    async def test_close_mid_dial_is_silent(self):
        call = FakeCall.outbound()
        call.dial_gate = asyncio.Event()
        notifier = RecordingNotifier()
        session = _AgentDuetSession(call, notifier)
        await session.start()
        await session.close()                # pipeline cancelled mid-dial
        call.dial_gate.set()
        await asyncio.wait_for(session._establish_task, timeout=1.0)
        assert "on_dialout_error" not in notifier.names()
        assert notifier.cancel_reasons == []  # self-initiated: never cancel

    async def test_answered_then_hangup_full_disconnect_path(self):
        call = FakeCall.outbound()
        notifier = RecordingNotifier()
        session = _AgentDuetSession(call, notifier)
        await session.start()
        await session._establish_task
        await call.trigger_hangup()
        names = notifier.names()
        assert names.index("on_before_disconnect") < names.index("on_dialout_stopped")
        assert names.count("on_dialout_stopped") == 1
        assert notifier.cancel_reasons == ["remote hangup"]

    async def test_dial_call_error_maps_to_error_event(self):
        from agentduet.exceptions import CallError

        call = FakeCall.outbound()
        call.dial_result = CallError("voice WS refused")
        notifier = RecordingNotifier()
        session = _AgentDuetSession(call, notifier)
        await session.start()
        await session._establish_task
        idx = notifier.names().index("on_dialout_error")
        assert notifier.events[idx][1].error_code == "CALL_ERROR"
        assert notifier.cancel_reasons == ["dial failed"]


class TestInboundCallErrorHardening:
    async def test_answer_call_error_now_reported_not_raised(self):
        from agentduet.exceptions import CallError

        call = FakeCall()
        call.answer_result = CallError("voice WS refused")
        notifier = RecordingNotifier()
        session = _AgentDuetSession(call, notifier)
        await session.start()  # must not raise
        idx = notifier.names().index("on_dialin_error")
        assert notifier.events[idx][1].error_code == "CALL_ERROR"
        assert notifier.cancel_reasons == ["answer failed"]
