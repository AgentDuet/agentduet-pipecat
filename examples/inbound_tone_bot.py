"""Keyless spike bot: answers an inbound call, plays a tone after each user
turn, stops the tone on barge-in. No STT/LLM/TTS keys needed.

Env: AGENTDUET_API_KEY, AGENTDUET_CONNECTOR_UUID, optional AGENTDUET_BASE_URL.
Run:  uv run --group example python examples/inbound_tone_bot.py
Then call the connector's number. Speak; after you stop, a tone plays for up
to 5 s. Speak over it: it must stop (barge-in). Watch the log for the
"barge-in: cleared N buffered bytes, ack in M ms" measurement lines.
"""

import os

# nltk's import-security hook false-positives on the project-local .venv
# layout (see conftest.py). Must be set before any pipecat import.
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")

import asyncio
import logging
import math
import struct
import uuid

from agentduet import (
    CallAudioConfig,
    IncomingCallNotification,
    SessionManager,
    SessionManagerConfig,
)
from dotenv import load_dotenv
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import (
    Frame,
    InterruptionFrame,
    OutputAudioRawFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.audio.vad_processor import VADProcessor
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.turns.user_turn_processor import UserTurnProcessor
from pipecat.workers.runner import WorkerRunner

from pipecat_agentduet import AgentDuetTransport

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("inbound_tone_bot")

SAMPLE_RATE = 16000  # Silero VAD supports 8k/16k only; 16k is the default

_call_tasks: set[asyncio.Task] = set()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"set {name} in the environment or .env")
    return value


class ToneBot(FrameProcessor):
    """Plays a sine tone after each user turn; stops it on interruption."""

    def __init__(self, *, freq: float = 440.0, seconds: float = 5.0, amplitude: float = 0.3):
        super().__init__()
        self._freq = freq
        self._seconds = seconds
        self._amplitude = amplitude
        self._gen_task = None

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InterruptionFrame):
            if self._gen_task is not None:
                await self.cancel_task(self._gen_task)
                self._gen_task = None
        elif isinstance(frame, UserStoppedSpeakingFrame):
            if self._gen_task is not None:
                await self.cancel_task(self._gen_task)
            self._gen_task = self.create_task(self._play_tone())
        await self.push_frame(frame, direction)

    async def _play_tone(self):
        chunk_samples = SAMPLE_RATE // 50  # 20 ms
        total_chunks = int(self._seconds * 50)
        peak = int(32767 * self._amplitude)
        sample_index = 0
        for _ in range(total_chunks):
            samples = [
                int(peak * math.sin(2 * math.pi * self._freq * (sample_index + i) / SAMPLE_RATE))
                for i in range(chunk_samples)
            ]
            sample_index += chunk_samples
            pcm = struct.pack(f"<{chunk_samples}h", *samples)
            await self.push_frame(
                OutputAudioRawFrame(audio=pcm, sample_rate=SAMPLE_RATE, num_channels=1)
            )


async def run_call(sm: SessionManager, noti: IncomingCallNotification):
    session = await sm.open_session(uuid.uuid4().hex, noti.subscriber)
    call = await session.process_call(noti)
    transport = AgentDuetTransport(call)

    @transport.event_handler("on_client_connected")
    async def on_connected(t, payload):
        logger.info("connected: %s (call %s)", payload.participant.value, payload.call_id)

    @transport.event_handler("on_client_disconnected")
    async def on_disconnected(t, payload):
        logger.info("disconnected: %s (call %s)", payload.participant.value, payload.call_id)

    pipeline = Pipeline(
        [
            transport.input(),
            VADProcessor(vad_analyzer=SileroVADAnalyzer()),
            UserTurnProcessor(),  # bare: interruptions default to enabled
            ToneBot(),
            transport.output(),
        ]
    )
    # idle_timeout off (a silent caller isn't an idle pipeline); RTVI and
    # turn-tracking stay at their defaults — the tests disable them only
    # for isolation.
    worker = PipelineWorker(pipeline, idle_timeout_secs=None)
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    await runner.run()
    logger.info("pipeline finished for call %s", call.id)


async def main():
    load_dotenv()
    api_key = _require_env("AGENTDUET_API_KEY")
    connector_uuid = _require_env("AGENTDUET_CONNECTOR_UUID")
    config = SessionManagerConfig.create(
        api_key=api_key,
        connector_uuid=connector_uuid,
        base_url=os.getenv("AGENTDUET_BASE_URL"),
        call_audio=CallAudioConfig(sample_rate=SAMPLE_RATE),
    )
    async with SessionManager(config) as sm:

        @sm.on_incoming_call
        async def on_call(noti: IncomingCallNotification):
            # Own task: never block the SDK event bus for the call's duration.
            # Tracked (not fire-and-forget): a bare create_task() holds no
            # reference (GC risk) and swallows exceptions until GC logs
            # "Task exception was never retrieved" — a misconfigured
            # connector would otherwise fail as silent dead air.
            task = asyncio.create_task(run_call(sm, noti))
            _call_tasks.add(task)

            def _done(t: asyncio.Task) -> None:
                _call_tasks.discard(t)
                if not t.cancelled() and t.exception() is not None:
                    logger.exception("call handler crashed", exc_info=t.exception())

            task.add_done_callback(_done)

        logger.info("listening for calls…")
        await sm.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
