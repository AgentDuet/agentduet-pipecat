"""Native showcase bot: answers an inbound call and runs a real
Deepgram STT -> Gemini LLM -> Deepgram TTS cascade (two vendor keys total). This is the pasted-quickstart
example — house style matches Pipecat's own transport examples (custom
main(), native event names, direct construction), no adapter-specific
plumbing beyond the transport.

Env (from the shell or examples/.env — see examples/.env.example):
AGENTDUET_API_KEY, AGENTDUET_CONNECTOR_UUID, optional AGENTDUET_BASE_URL,
DEEPGRAM_API_KEY (STT and TTS), GOOGLE_API_KEY, optional DEEPGRAM_TTS_VOICE.
Run:  uv run --group example python examples/voice_bot.py
Then call the connector's number. The bot greets you first, then converses.
"""

import os

# nltk's import-security hook false-positives on the project-local .venv
# layout (see conftest.py). Must be set before any pipecat import.
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")

import asyncio
import logging
import uuid

from agentduet import (
    CallAudioConfig,
    IncomingCallNotification,
    SessionManager,
    SessionManagerConfig,
)
from dotenv import load_dotenv
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.audio.vad_processor import VADProcessor
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.deepgram.tts import DeepgramTTSService
from pipecat.services.google.llm import GoogleLLMService
from pipecat.turns.user_turn_processor import UserTurnProcessor
from pipecat.workers.runner import WorkerRunner

from pipecat_agentduet import AgentDuetTransport

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("voice_bot")

SAMPLE_RATE = 16000  # Silero VAD supports 8k/16k only; 16k is the default

# Arbitrary default (British Reading Lady) — not vetted for quality, just a

_call_tasks: set[asyncio.Task] = set()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"set {name} in the environment or .env")
    return value


async def run_call(
    sm: SessionManager,
    noti: IncomingCallNotification,
    *,
    deepgram_api_key: str,
    google_api_key: str,
    tts_voice: str | None,
):
    session = await sm.open_session(uuid.uuid4().hex, noti.subscriber)
    call = await session.process_call(noti)
    transport = AgentDuetTransport(call)

    context = LLMContext(
        [
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant on a phone call. Keep answers to "
                    "one or two sentences."
                ),
            }
        ]
    )
    aggregators = LLMContextAggregatorPair(context)

    stt = DeepgramSTTService(api_key=deepgram_api_key)
    llm = GoogleLLMService(api_key=google_api_key)  # default model: gemini-2.5-flash
    tts = DeepgramTTSService(api_key=deepgram_api_key, voice=tts_voice)

    pipeline = Pipeline(
        [
            transport.input(),
            VADProcessor(vad_analyzer=SileroVADAnalyzer()),
            # Real STT is wired below, so (unlike the tone bots) the default
            # stop strategy — the Smart Turn v3 semantic model — is correct
            # here: it needs a transcript to judge, which this pipeline now
            # produces. Do NOT pin SpeechTimeoutUserTurnStopStrategy the way
            # the tone bots (examples/inbound_tone_bot.py, outbound_tone_bot.py)
            # do — that pin exists only because those pipelines are keyless
            # and have no transcript for Smart Turn to use.
            UserTurnProcessor(),
            stt,
            aggregators.user(),
            llm,
            tts,
            transport.output(),
            aggregators.assistant(),
        ]
    )
    # idle_timeout off (a silent caller isn't an idle pipeline); RTVI and
    # turn-tracking stay at their defaults — the tests disable them only
    # for isolation.
    worker = PipelineWorker(pipeline, idle_timeout_secs=None)

    # Native event for the greeting; the generic on_client_connected /
    # on_client_disconnected pair fires in the same code path with the same
    # payload (alias layer, spec-mandated) — either name works identically
    # for a pasted-quickstart bot.
    @transport.event_handler("on_dialin_connected")
    async def on_connected(t, payload):
        logger.info("connected: %s (call %s)", payload.participant.value, payload.call_id)
        # Kick off the conversation: the bot speaks first.
        context.add_message(
            {"role": "developer", "content": "Start by briefly greeting the caller."}
        )
        await worker.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_dialin_stopped")
    async def on_disconnected(t, payload):
        logger.info("disconnected: %s (call %s)", payload.participant.value, payload.call_id)

    @transport.event_handler("on_dialin_error")
    async def on_answer_error(t, result):
        logger.error("answer failed: %s (%s)", result.error_code, result.error_message)

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    await runner.run()
    logger.info("pipeline finished for call %s", call.id)


async def main():
    load_dotenv()
    api_key = _require_env("AGENTDUET_API_KEY")
    connector_uuid = _require_env("AGENTDUET_CONNECTOR_UUID")
    # Required up front (not lazily inside run_call) so a misconfigured
    # deployment fails at startup, not silently on the first inbound call.
    deepgram_api_key = _require_env("DEEPGRAM_API_KEY")
    google_api_key = _require_env("GOOGLE_API_KEY")
    # None -> the service default voice (aura-2-helena-en in pipecat 1.7.0).
    tts_voice = os.getenv("DEEPGRAM_TTS_VOICE")
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
            task = asyncio.create_task(
                run_call(
                    sm,
                    noti,
                    deepgram_api_key=deepgram_api_key,
                    google_api_key=google_api_key,
                    tts_voice=tts_voice,
                )
            )
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
