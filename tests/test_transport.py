"""AgentDuetTransport: construction, events, and frame plumbing."""

import pytest
from agentduet import CallState
from pipecat.transports.base_transport import TransportParams

from pipecat_agentduet import AgentDuetTransport
from pipecat_agentduet._session import CallEventPayload
from tests.fakes import FakeCall


class TestConstruction:
    def test_default_params_enable_audio_and_pin_rates(self):
        call = FakeCall(sample_rate=8000)
        transport = AgentDuetTransport(call)
        assert transport._params.audio_in_enabled is True
        assert transport._params.audio_out_enabled is True
        assert transport._params.audio_in_sample_rate == 8000
        assert transport._params.audio_out_sample_rate == 8000

    def test_conflicting_user_rate_raises(self):
        call = FakeCall(sample_rate=16000)
        with pytest.raises(ValueError, match="sample rate"):
            AgentDuetTransport(
                call,
                params=TransportParams(
                    audio_in_enabled=True,
                    audio_out_enabled=True,
                    audio_out_sample_rate=24000,
                ),
            )

    def test_matching_user_rate_accepted(self):
        call = FakeCall(sample_rate=16000)
        AgentDuetTransport(
            call,
            params=TransportParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_in_sample_rate=16000,
            ),
        )  # no raise

    def test_call_property_exposes_sdk_call(self):
        call = FakeCall()
        assert AgentDuetTransport(call).call is call


class TestEventAliases:
    async def test_native_and_generic_fire_same_payload_same_moment(self):
        call = FakeCall()
        transport = AgentDuetTransport(call)
        seen: list[tuple[str, object]] = []

        @transport.event_handler("on_dialin_connected")
        async def on_native(t, payload):
            seen.append(("native", payload))

        @transport.event_handler("on_client_connected")
        async def on_generic(t, payload):
            seen.append(("generic", payload))

        payload = CallEventPayload(
            participant=call.participant, call_id=call.id, state=CallState.LIVE
        )
        await transport._fire("on_dialin_connected", payload)
        assert [name for name, _ in seen] == ["native", "generic"]
        assert seen[0][1] is seen[1][1]  # identical payload object

    async def test_all_v1_event_names_registered(self):
        # add_event_handler on an unknown name only warns, so pin the
        # registry directly.
        transport = AgentDuetTransport(FakeCall())
        for name in [
            "on_client_connected", "on_client_disconnected",
            "on_dialin_connected", "on_dialin_stopped", "on_dialin_error",
            "on_dialout_answered", "on_dialout_stopped", "on_dialout_error",
            "on_error", "on_call_state_updated", "on_before_disconnect",
        ]:
            assert name in transport._event_handlers
