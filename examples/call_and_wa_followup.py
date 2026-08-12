"""Multi-modal bot: voice calls and WhatsApp messages handled side by side in
one process, with a post-call WhatsApp follow-up sent in the SAME session
that just handled the call. That's the differentiator this example exists to
show: one arrival layer (`SessionManager`), one `Session` object spanning
both a phone call and a chat thread with the same caller.

Voice pipeline reuses the keyless ToneBot from _tone.py (see
inbound_tone_bot.py for the play-a-tone-after-each-turn behaviour), so this
example needs no STT/LLM/TTS keys either — the multi-modal story is the
point, not the AI stack.

Env (from the shell or examples/.env — see examples/.env.example):
  AGENTDUET_API_KEY, AGENTDUET_CONNECTOR_UUID, optional AGENTDUET_BASE_URL.
  Optional: WA_API_VERSION (defaults to "v21.0"), WA_FOLLOWUP_TO.

Run:  uv run --group example python examples/call_and_wa_followup.py
Then call the connector's number. Speak; after you stop, a tone plays for up
to 5 s, same as inbound_tone_bot.py. Hang up: a WhatsApp follow-up is sent
in the same session that handled the call.

Demo without a WhatsApp-capable connector: a TELCO-only connector has no WA
thread with the caller, so the follow-up has nowhere to go by default. Set
WA_FOLLOWUP_TO to an E.164/whatsapp address in examples/.env and the
follow-up will be sent there instead, so you can see the call-then-message
flow even on a voice-only connector. To see the real thing — the follow-up
landing in the same thread the caller would message the bot back on — you
need a WA-capable connector (see Task 10).

This example can't be fully validated end-to-end without a WA connector:
SendWAMessage's `data` dict is passed through to the WhatsApp Cloud API by
the connector, and its exact shape/behaviour is connector-dependent.
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
    AgentDuetError,
    CallAudioConfig,
    IncomingCallNotification,
    IncomingMessage,
    Network,
    SendWAMessage,
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
logger = logging.getLogger("call_and_wa_followup")

SAMPLE_RATE = 16000  # Silero VAD supports 8k/16k only; 16k is the default

_call_tasks: set[asyncio.Task] = set()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"set {name} in the environment or .env")
    return value


async def run_call(sm: SessionManager, noti: IncomingCallNotification):
    session_id = uuid.uuid4().hex
    session = await sm.open_session(session_id, noti.subscriber)
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
            # Plain VAD-timeout turn stop, same rationale as inbound_tone_bot.py:
            # the default Smart Turn v3 stop strategy stalls this keyless
            # pipeline (no STT transcript) by 5-15 s.
            UserTurnProcessor(
                user_turn_strategies=UserTurnStrategies(
                    stop=[SpeechTimeoutUserTurnStopStrategy(wait_for_transcript=False)]
                )
            ),
            ToneBot(sample_rate=SAMPLE_RATE),
            transport.output(),
        ]
    )
    worker = PipelineWorker(pipeline, idle_timeout_secs=None)
    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    await runner.run()
    logger.info("pipeline finished for call %s", call.id)

    # --- The multi-modal payoff -------------------------------------------
    # runner.run() only returns once the pipeline has ended (call hung up or
    # closed), but `session` is still open here: a Session outlives any one
    # call, so we can send a WhatsApp message through the SAME session that
    # just carried the voice call — same caller, same conversation, two
    # channels. This is the whole point of this example.
    #
    # A caller who dialed in over WA has a WA thread we can just reply on
    # (noti.participant.value is their WA address). A TELCO caller has no
    # such thread, so there's nowhere to send a follow-up unless the
    # operator points it somewhere explicitly via WA_FOLLOWUP_TO — that lets
    # this demo show the flow even on a voice-only connector.
    wa_followup_to = os.getenv("WA_FOLLOWUP_TO")
    if noti.network == Network.WA:
        destination = noti.participant.value
    elif wa_followup_to:
        destination = wa_followup_to
    else:
        logger.info("no WA follow-up: caller is not on WA and WA_FOLLOWUP_TO is unset")
        return

    try:
        result = await session.send_message(
            SendWAMessage(
                api_version=os.getenv("WA_API_VERSION", "v21.0"),
                data={
                    "messaging_product": "whatsapp",
                    "to": destination,
                    "type": "text",
                    "text": {"body": "Thanks for calling! Reply here any time."},
                },
            )
        )
        if result.success:
            logger.info("WA follow-up sent to %s in session %s", destination, session_id)
        else:
            logger.warning("WA follow-up failed: %s %s", result.error_code, result.error_content)
    except AgentDuetError as exc:
        # send_message can raise (e.g. RequestTimeoutError, TransportError per
        # the SDK docs); without this, a transient send failure would surface
        # as "call handler crashed" even though the call itself succeeded.
        logger.warning("WA follow-up failed: %s", exc)


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

        @sm.on_incoming_message
        async def on_message(msg: IncomingMessage):
            # Incoming messages can be redelivered by the SDK; a production
            # app should dedup by msg.id before acting on one twice.
            logger.info("message from %s: %s", msg.participant.value, msg.payload)

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

        logger.info("listening for calls and messages…")
        await sm.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
