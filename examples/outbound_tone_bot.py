"""Keyless spike bot: dials out, plays a tone after each user turn, stops
the tone on barge-in. No STT/LLM/TTS keys needed. Live-validation vehicle
for the dial path (spec §5 outbound).

Env (from the shell or examples/.env — see examples/.env.example):
AGENTDUET_API_KEY, AGENTDUET_CONNECTOR_UUID, optional AGENTDUET_BASE_URL,
PROBE_SUBSCRIBER (the line to call from), PROBE_DEST (E.164 number to call).
Run:  uv run --group example python examples/outbound_tone_bot.py
Your phone rings; answer it and speak. After you stop, a tone plays for up
to 5 s. Speak over it: it must stop (barge-in). Watch the log for the
"barge-in: cleared N buffered bytes, ack in M ms" measurement lines.
"""

import os

# nltk's import-security hook false-positives on the project-local .venv
# layout (see conftest.py). Must be set before any pipecat import.
os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")

import asyncio
import logging
import uuid

from _tone import ToneBot
from agentduet import (
    Address,
    CallAudioConfig,
    Network,
    SessionManager,
    SessionManagerConfig,
)
from dotenv import load_dotenv
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.audio.vad_processor import VADProcessor
from pipecat.turns.user_stop import SpeechTimeoutUserTurnStopStrategy
from pipecat.turns.user_turn_processor import UserTurnProcessor
from pipecat.turns.user_turn_strategies import UserTurnStrategies
from pipecat.workers.runner import WorkerRunner

from pipecat_agentduet import AgentDuetTransport

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("outbound_tone_bot")

SAMPLE_RATE = 16000  # Silero VAD supports 8k/16k only; 16k is the default


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"set {name} in the environment or .env")
    return value


async def main():
    load_dotenv()
    api_key = _require_env("AGENTDUET_API_KEY")
    connector_uuid = _require_env("AGENTDUET_CONNECTOR_UUID")
    subscriber = _require_env("PROBE_SUBSCRIBER")
    dest = _require_env("PROBE_DEST")
    config = SessionManagerConfig.create(
        api_key=api_key,
        connector_uuid=connector_uuid,
        base_url=os.getenv("AGENTDUET_BASE_URL"),
        call_audio=CallAudioConfig(sample_rate=SAMPLE_RATE),
    )
    # No arrival layer here (unlike inbound_tone_bot.py): the outbound call
    # is the whole reason this process exists, so a plain main() dials it
    # directly (parent spec §5 outbound step 1).
    async with SessionManager(config) as sm:
        session = await sm.open_session(uuid.uuid4().hex, subscriber)
        call = await session.make_call(Address(Network.TELCO, dest))
        # Ring time is 45 s here (SDK default is 60) to show the kwarg.
        transport = AgentDuetTransport(call, ring_time_seconds=45)

        # Native event for dial-specific handling; generic on_client_disconnected
        # for the shared path below — the mix doubles as documentation of the
        # alias layer (one signal, two names, one moment, one payload).
        @transport.event_handler("on_dialout_answered")
        async def on_answered(t, payload):
            logger.info("answered: %s (call %s)", payload.participant.value, payload.call_id)

        @transport.event_handler("on_dialout_error")
        async def on_dial_error(t, result):
            logger.error("dial failed: %s", result.error_code)

        @transport.event_handler("on_client_disconnected")
        async def on_disconnected(t, payload):
            logger.info("disconnected: %s (call %s)", payload.participant.value, payload.call_id)

        pipeline = Pipeline(
            [
                transport.input(),
                VADProcessor(vad_analyzer=SileroVADAnalyzer()),
                # Plain VAD-timeout turn stop. The default stop strategy is the
                # Smart Turn v3 semantic model, which (correctly) judges test
                # phrases as incomplete turns and stalls the tone by 5-15 s
                # until the stop-timeout fires — observed on live validation
                # 2026-08-11 (inbound). wait_for_transcript=False because this
                # keyless pipeline has no STT to produce one. Interruptions
                # stay enabled (start-strategy default).
                UserTurnProcessor(
                    user_turn_strategies=UserTurnStrategies(
                        stop=[SpeechTimeoutUserTurnStopStrategy(wait_for_transcript=False)]
                    )
                ),
                ToneBot(sample_rate=SAMPLE_RATE),
                transport.output(),
            ]
        )
        # idle_timeout off (a silent callee isn't an idle pipeline); RTVI and
        # turn-tracking stay at their defaults — the tests disable them only
        # for isolation.
        worker = PipelineWorker(pipeline, idle_timeout_secs=None)
        # handle_sigint=True: single-call process, Ctrl-C should end it. The
        # inbound example uses False because its SessionManager loop (run_forever)
        # owns SIGINT for the whole listening process; here there is no such loop.
        runner = WorkerRunner(handle_sigint=True)
        await runner.add_workers(worker)
        await runner.run()
        logger.info("worker exited for call %s", call.id)


if __name__ == "__main__":
    asyncio.run(main())
