"""AgentDuetTransport: construction, events, and frame plumbing."""

import asyncio

import pytest
from agentduet import CallState
from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.frame_processor import FrameProcessor
from pipecat.transports.base_transport import TransportParams
from pipecat.workers.runner import WorkerRunner

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


class FrameCapture(FrameProcessor):
    """Terminal processor that records every frame it sees."""

    def __init__(self):
        super().__init__()
        self.frames: list[Frame] = []

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        self.frames.append(frame)
        await self.push_frame(frame, direction)


def make_worker(pipeline: Pipeline) -> PipelineWorker:
    # Lean worker for tests: no idle cancel, no RTVI/turn-tracking extras.
    # check_dangling_tasks off: the spawned barge-in clear task may outlive
    # a short test worker by design (Task 8's 5 s slow-ack test).
    return PipelineWorker(
        pipeline,
        idle_timeout_secs=None,
        enable_rtvi=False,
        enable_turn_tracking=False,
        check_dangling_tasks=False,
    )


async def run_worker(worker: PipelineWorker) -> asyncio.Task:
    runner = WorkerRunner(handle_sigint=False)
    task = asyncio.create_task(runner.run(worker))
    return task


class TestInputTransport:
    async def test_remote_audio_becomes_tagged_input_frames(self):
        call = FakeCall(sample_rate=8000)
        transport = AgentDuetTransport(call)
        capture = FrameCapture()
        worker = make_worker(Pipeline([transport.input(), capture]))
        run_task = await run_worker(worker)

        call.caller.audio_queue.put_nowait(b"\x01\x02" * 80)
        await asyncio.sleep(0.2)

        audio = [f for f in capture.frames if isinstance(f, InputAudioRawFrame)]
        assert audio, "no InputAudioRawFrame reached the pipeline"
        assert audio[0].audio == b"\x01\x02" * 80  # bytes untouched
        assert audio[0].sample_rate == 8000  # tagged with the call's rate
        assert audio[0].num_channels == 1

        await call.trigger_hangup()
        await asyncio.wait_for(run_task, timeout=5)

    async def test_remote_hangup_cancels_the_worker(self):
        call = FakeCall()
        transport = AgentDuetTransport(call)
        worker = make_worker(Pipeline([transport.input(), FrameCapture()]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)  # let StartFrame/answer complete

        await call.trigger_hangup()
        # CancelWorkerFrame -> CancelFrame -> worker ends -> runner returns.
        await asyncio.wait_for(run_task, timeout=5)

    async def test_pipeline_end_closes_the_call(self):
        call = FakeCall()
        transport = AgentDuetTransport(call)
        worker = make_worker(Pipeline([transport.input(), FrameCapture()]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)

        await worker.stop_when_done()  # graceful EndFrame path
        await asyncio.wait_for(run_task, timeout=5)
        assert call.close_calls >= 1  # transport closed the call, no user code
