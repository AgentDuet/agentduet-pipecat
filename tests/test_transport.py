"""AgentDuetTransport: construction, events, and frame plumbing."""

import asyncio

import pytest
from agentduet import CallState
from agentduet.exceptions import BufferFullError
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

    async def test_call_error_forwards_to_on_error(self):
        call = FakeCall()
        transport = AgentDuetTransport(call)
        seen = []

        @transport.event_handler("on_error")
        async def on_error(t, data):
            seen.append(data)

        await call.trigger_error({"code": "boom"})
        assert seen == [{"code": "boom"}]


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
    await runner.add_workers(worker)
    task = asyncio.create_task(runner.run())
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


# One 10 ms chunk at 16 kHz mono s16: 160 samples * 2 bytes. MediaSender
# buffers output audio to audio_out_10ms_chunks x 10 ms before calling
# write_audio_frame, so tests use chunk_size multiples and set the chunking
# to 1 to make writes deterministic.
CHUNK = 320


def make_output_frame(pcm: bytes) -> OutputAudioRawFrame:
    return OutputAudioRawFrame(audio=pcm, sample_rate=16000, num_channels=1)


class TestOutputTransport:
    async def _run_output(self, call: FakeCall):
        transport = AgentDuetTransport(
            call,
            params=TransportParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_out_10ms_chunks=1,  # flush every 10 ms chunk
            ),
        )
        output = transport.output()
        worker = make_worker(Pipeline([output]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)  # StartFrame processed, answer done
        return transport, output, worker, run_task

    async def _finish(self, worker, run_task):
        await worker.stop_when_done()
        await asyncio.wait_for(run_task, timeout=5)

    async def test_output_audio_reaches_send_audio_byte_identical(self):
        call = FakeCall()
        _transport, _output, worker, run_task = await self._run_output(call)
        pcm = b"\x03\x04" * (CHUNK // 2)  # exactly one chunk
        await worker.queue_frame(make_output_frame(pcm))
        await asyncio.sleep(0.3)
        assert b"".join(call.sent_audio) == pcm
        await self._finish(worker, run_task)

    async def test_buffer_full_drops_chunk_pipeline_survives(self):
        call = FakeCall()
        _transport, _output, worker, run_task = await self._run_output(call)
        call.send_audio_error = BufferFullError("full")
        await worker.queue_frame(make_output_frame(b"\x00" * CHUNK))
        await asyncio.sleep(0.2)
        call.send_audio_error = None
        second = b"\x07\x08" * (CHUNK // 2)
        await worker.queue_frame(make_output_frame(second))
        await asyncio.sleep(0.2)
        # First chunk dropped on BufferFullError, second delivered intact.
        assert b"".join(call.sent_audio) == second
        await self._finish(worker, run_task)

    async def test_interruption_clears_buffer_without_blocking_frames(self):
        call = FakeCall()
        call.clear_delay = 5.0  # slow server ack: must NOT stall the pipeline
        _transport, _output, worker, run_task = await self._run_output(call)

        await worker.queue_frame(InterruptionFrame())
        await asyncio.sleep(0.2)
        assert call.clear_calls == 1  # clear was invoked...

        # ...and the frame path is still live: audio written well before the
        # 5 s ack resolves.
        pcm = b"\x0a\x0b" * (CHUNK // 2)
        await worker.queue_frame(make_output_frame(pcm))
        await asyncio.sleep(0.3)
        assert b"".join(call.sent_audio) == pcm
        await self._finish(worker, run_task)


class TestDualHalfLifecycle:
    async def test_endframe_does_not_close_call_until_output_stops(self):
        # EndFrame reaching the input half must NOT close the call: farewell
        # audio queued ahead of EndFrame has to reach the output half first.
        call = FakeCall()
        transport = AgentDuetTransport(
            call,
            params=TransportParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_out_10ms_chunks=1,
                # Default 2 s of post-EndFrame silence (real Pipecat behaviour,
                # BaseOutputTransport.MediaSender._send_silence) would land in
                # call.sent_audio after the farewell and break the exact byte
                # match below; this test is about ordering, not silence, so
                # turn it off rather than weaken the assertion.
                audio_out_end_silence_secs=0,
            ),
        )
        worker = make_worker(Pipeline([transport.input(), transport.output()]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)

        farewell = b"\x05\x06" * (CHUNK // 2)
        await worker.queue_frame(make_output_frame(farewell))
        await worker.stop_when_done()
        await asyncio.wait_for(run_task, timeout=5)

        # The farewell survived EndFrame passing the input half...
        assert b"".join(call.sent_audio) == farewell
        # ...and the call still closed once both halves stopped.
        assert call.close_calls >= 1


class TestOutboundPipeline:
    async def test_dialout_alias_pair_fires_once_with_same_payload(self):
        call = FakeCall.outbound()
        call.dial_gate = asyncio.Event()
        transport = AgentDuetTransport(call)
        seen: list[tuple[str, object]] = []

        @transport.event_handler("on_dialout_answered")
        async def on_native(t, payload):
            seen.append(("native", payload))

        @transport.event_handler("on_client_connected")
        async def on_generic(t, payload):
            seen.append(("generic", payload))

        worker = make_worker(Pipeline([transport.input(), FrameCapture()]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)  # dial spawned, gated on dial_gate

        call.dial_gate.set()
        await asyncio.wait_for(transport._session._establish_task, timeout=1.0)

        assert [name for name, _ in seen] == ["native", "generic"]
        assert seen[0][1] is seen[1][1]  # identical payload object

        await call.trigger_hangup()
        await asyncio.wait_for(run_task, timeout=5)

    async def test_outbound_audio_flows_after_answer(self):
        call = FakeCall.outbound(sample_rate=16000)
        call.dial_gate = asyncio.Event()
        transport = AgentDuetTransport(
            call,
            params=TransportParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_out_10ms_chunks=1,  # flush every 10 ms chunk
            ),
        )
        capture = FrameCapture()
        worker = make_worker(Pipeline([transport.input(), capture, transport.output()]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)  # dial spawned, gated on dial_gate

        call.dial_gate.set()
        await asyncio.wait_for(transport._session._establish_task, timeout=1.0)

        call.callee.audio_queue.put_nowait(b"\x01\x02" * 80)
        await asyncio.sleep(0.2)

        audio = [f for f in capture.frames if isinstance(f, InputAudioRawFrame)]
        assert audio, "no InputAudioRawFrame reached the pipeline"
        assert audio[0].audio == b"\x01\x02" * 80  # bytes untouched
        assert audio[0].sample_rate == 16000  # tagged with the call's rate
        assert audio[0].num_channels == 1

        pcm = b"\x03\x04" * (CHUNK // 2)  # exactly one chunk
        await worker.queue_frame(make_output_frame(pcm))
        await asyncio.sleep(0.3)
        assert b"".join(call.sent_audio) == pcm

        await call.trigger_hangup()
        await asyncio.wait_for(run_task, timeout=5)

    async def test_cancel_mid_dial_tears_down_silently(self):
        call = FakeCall.outbound()
        call.dial_gate = asyncio.Event()
        transport = AgentDuetTransport(call)
        seen: list[str] = []

        @transport.event_handler("on_client_connected")
        async def on_connected(t, payload):
            seen.append("connected")

        @transport.event_handler("on_client_disconnected")
        async def on_disconnected(t, payload):
            seen.append("disconnected")

        @transport.event_handler("on_dialout_error")
        async def on_dialout_error(t, payload):
            seen.append("error")

        worker = make_worker(Pipeline([transport.input(), FrameCapture()]))
        run_task = await run_worker(worker)
        await asyncio.sleep(0.1)  # dial spawned, gated on dial_gate

        await worker.cancel(reason="test cancel mid-dial")
        # worker.cancel() only queues the CancelFrame; wait for it to actually
        # reach AgentDuetInputTransport.cancel() (call.close() runs) before
        # releasing the gate, or dial() could race ahead and observe a call
        # that isn't TERMINATED yet.
        for _ in range(50):
            if call.close_calls >= 1:
                break
            await asyncio.sleep(0.01)
        assert call.close_calls >= 1
        call.dial_gate.set()  # dial() now observes TERMINATED, resolves silently
        # Drain the dial task deterministically: also propagates any
        # unexpected exception from _establish instead of letting the
        # done-callback merely log it while this test passes regardless.
        await asyncio.wait_for(transport._session._establish_task, timeout=1.0)

        await asyncio.wait_for(run_task, timeout=5)
        assert seen == []


class TestRingTime:
    def test_invalid_ring_time_raises_at_construction(self):
        with pytest.raises(ValueError, match="ring_time_seconds"):
            AgentDuetTransport(FakeCall.outbound(), ring_time_seconds=0)
        with pytest.raises(ValueError, match="ring_time_seconds"):
            AgentDuetTransport(FakeCall.outbound(), ring_time_seconds=121)

    def test_ring_time_reaches_session(self):
        transport = AgentDuetTransport(FakeCall.outbound(), ring_time_seconds=45)
        assert transport._session._ring_time_seconds == 45
