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
