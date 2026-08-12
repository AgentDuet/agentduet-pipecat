# pipecat-agentduet

Phone and WhatsApp calls in any [Pipecat](https://github.com/pipecat-ai/pipecat) pipeline, via the [AgentDuet](https://pypi.org/project/agentduet/) SDK. The SDK connects *outbound* to the AgentDuet platform and calls are delivered down that connection — so there is no webhook to receive, no public endpoint, no tunnel, and no serializer to pick. One class, `AgentDuetTransport`, moves audio and call lifecycle between a live AgentDuet `Call` and your pipeline.

## Install

Not on PyPI yet (publishing is pending a packaging decision); install from source for now. The repo location may change before publishing.

```sh
pip install git+https://github.com/vonhutuan-b3/agentduet-pipecat.git
```

Requires Python >= 3.12. The quickstart below also needs Pipecat's service extras: `pip install "pipecat-ai[silero,deepgram,google]"`.

## Inbound quickstart

A *connector* is a provisioned phone/WhatsApp number (or number pool) on the AgentDuet platform; your bot process attaches to one and receives its calls. The API key and connector UUID come from your AgentDuet workspace/portal.

A complete bot: answers calls on your connector, greets the caller, and runs a Deepgram → Gemini → Deepgram cascade (STT and TTS on one Deepgram key — two vendor keys total). Save as `bot.py`, set `AGENTDUET_API_KEY`, `AGENTDUET_CONNECTOR_UUID`, `DEEPGRAM_API_KEY`, and `GOOGLE_API_KEY`, run `python bot.py`, then call your connector's number.

```python
import os

os.environ.setdefault("NLTK_DISABLE_IMPORT_SECURITY", "1")  # nltk hook false-positives on .venv-in-cwd layouts

import asyncio
import uuid

from agentduet import IncomingCallNotification, SessionManager, SessionManagerConfig
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


async def run_call(sm: SessionManager, noti: IncomingCallNotification):
    # any unique session id + the called number's subscriber
    session = await sm.open_session(uuid.uuid4().hex, noti.subscriber)
    transport = AgentDuetTransport(await session.process_call(noti))
    context = LLMContext([{"role": "system", "content": "You are a helpful assistant on a phone call."}])
    aggregators = LLMContextAggregatorPair(context)
    pipeline = Pipeline([
        transport.input(),
        VADProcessor(vad_analyzer=SileroVADAnalyzer()),
        UserTurnProcessor(),
        DeepgramSTTService(api_key=os.environ["DEEPGRAM_API_KEY"]),
        aggregators.user(),
        GoogleLLMService(api_key=os.environ["GOOGLE_API_KEY"]),
        DeepgramTTSService(api_key=os.environ["DEEPGRAM_API_KEY"]),  # default voice: aura-2-helena-en
        transport.output(),
        aggregators.assistant(),
    ])
    worker = PipelineWorker(pipeline, idle_timeout_secs=None)

    @transport.event_handler("on_dialin_connected")
    async def on_connected(t, payload):
        context.add_message({"role": "developer", "content": "Start by briefly greeting the caller."})
        await worker.queue_frames([LLMRunFrame()])  # the bot speaks first

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)
    await runner.run()


async def main():
    config = SessionManagerConfig.create(
        api_key=os.environ["AGENTDUET_API_KEY"],
        connector_uuid=os.environ["AGENTDUET_CONNECTOR_UUID"],
    )
    async with SessionManager(config) as sm:

        @sm.on_incoming_call
        async def on_call(noti: IncomingCallNotification):
            asyncio.create_task(run_call(sm, noti))  # see examples/voice_bot.py for tracked tasks

        await sm.run_forever()


asyncio.run(main())
```

The transport answers the call on pipeline start (so STT/LLM/TTS are wired before media flows), closes it when the pipeline ends, and cancels the pipeline on remote hangup — no lifecycle code to write.

Coming from the Twilio-family quickstarts, here is what you did **not** have to do:

- write a FastAPI/websocket server to receive the call
- run ngrok or provision a public URL and point the number's webhook at it
- return TwiML (or equivalent) to route the media stream
- choose and configure a frame serializer
- configure sample rates or transcode 8 kHz mulaw — both sides are already mono s16 PCM at the call's rate

## Outbound

Same transport, same pipeline — you make the call instead of waiting for one:

```python
import uuid

from agentduet import Address, Network, SessionManager, SessionManagerConfig

from pipecat_agentduet import AgentDuetTransport


async def main():
    config = SessionManagerConfig.create(api_key=..., connector_uuid=...)
    async with SessionManager(config) as sm:
        session = await sm.open_session(uuid.uuid4().hex, "+6512340000")  # the line to call from
        call = await session.make_call(Address(Network.TELCO, "+6598760000"))
        transport = AgentDuetTransport(call, ring_time_seconds=45)  # default 60, range 1-120

        @transport.event_handler("on_dialout_answered")
        async def on_answered(t, payload):
            print(f"answered: {payload.participant.value}")

        # Build the same Pipeline around transport.input()/transport.output()
        # as above, then run the worker — the dial happens on pipeline start.
```

"Answered" is `on_dialout_answered` / `on_client_connected`, fired when the SDK's `dial()` resolves — there is no separate SIP-leg event, so answered means the remote party is live.

## Events

Handlers are registered with `@transport.event_handler(name)` and called as `handler(transport, payload)`. The generic `on_client_connected` / `on_client_disconnected` pair is an alias layer fired in the same code path as the corresponding native dial-in/dial-out event: one signal, two names, one moment, identical payloads — so any standard Pipecat example bot works unmodified, and inbound/outbound handlers are interchangeable.

| Event | Fires when | Payload |
|---|---|---|
| `on_client_connected` | call is live (alias, both directions) | `CallEventPayload` |
| `on_client_disconnected` | hangup (alias, both directions) | `CallEventPayload` |
| `on_dialin_connected` | `answer()` succeeds | `CallEventPayload` |
| `on_dialin_stopped` | inbound hangup | `CallEventPayload` |
| `on_dialin_error` | `answer()` fails | `CommandResult` |
| `on_dialout_answered` | `dial()` resolves success | `CallEventPayload` |
| `on_dialout_stopped` | outbound hangup | `CallEventPayload` |
| `on_dialout_error` | `dial()` resolves failure | `CommandResult` |
| `on_error` | SDK `CallEvent.ERROR` | SDK error data |
| `on_call_state_updated` | each observed `CallState` transition | `CallState` |
| `on_before_disconnect` | just before the disconnect events, awaited — last chance to flush/log | `CallEventPayload` |

`CallEventPayload` is `participant` (SDK `Address`), `call_id`, and `state` (SDK `CallState`); error payloads are the SDK's `CommandResult`. No parallel type system.

## Audio

Audio is mono 16-bit PCM at the connector's fixed rate — 8000, 16000, or 24000 Hz, set once in `CallAudioConfig` (default 16000). The transport has zero audio configuration by design: it reads the call's rate and declares it on both halves, so there is no second place to type a sample rate (setting a conflicting rate on `TransportParams` raises). Pipecat resamples at the seams — TTS native rate down to the line rate, input up to what STT wants — and bytes cross the transport untouched.

## Gotchas

- **Keyless / no-STT pipelines must pin the turn-stop strategy.** Pipecat's default user-turn stop strategy is the Smart Turn v3 semantic model, which needs a transcript to judge; with no STT it judges phrases as incomplete turns and stalls the bot's reply 5–15 s until the stop timeout (observed on live validation). Pin `UserTurnProcessor(user_turn_strategies=UserTurnStrategies(stop=[SpeechTimeoutUserTurnStopStrategy(wait_for_transcript=False)]))` as the tone-bot examples do (`from pipecat.turns.user_stop import SpeechTimeoutUserTurnStopStrategy`, `from pipecat.turns.user_turn_strategies import UserTurnStrategies`). Real STT pipelines keep the default.
- **A caller who abandons during ring can occupy a pipeline for ~10 s (inbound).** If the caller hangs up before the server has SIP dialog state for the call, the hangup has nothing to land against and the server never learns the call ended; the SDK's 10 s command timeout is the designed fallback that reclaims the pipeline. Expected behavior (root-caused with server logs), not a defect — no connected/disconnected events fire for such calls.
- **Outbound dial failures: handle `CALL_UNANSWERED` and `TIMEOUT` as one outcome.** Which code you get is route/carrier-dependent — some routes report failures, others just let the client-side ring deadline expire. In particular, a callee who *rejects* the call may be indistinguishable from one who never answered (live-validated: on some carriers the decline never propagates, and the dial resolves as `TIMEOUT` after the full `ring_time_seconds`). Branch on `result.success`, log the code, and never write logic that assumes a reject resolves early. Distinguishing busy/no-answer/rejected needs a dial-progress protocol addition (parent spec §10) — not available in v1.

## Scaling

Delivery is connector-wide competing-consumer: run N copies of your bot process against one connector and calls distribute across them — no load balancer, no sticky sessions, no per-process configuration. Add a process to add capacity.

## Going deeper

The transport is the top of a progressive-disclosure ladder. `transport.call` exposes the underlying SDK `Call` for capabilities beyond frame plumbing — `whisper()`, `barge()`, `spy()` for copilot pipelines (follow-up feature territory, reachable today as a preview). The `Session` you opened the call on also carries messaging (`session.send_message`), enabling post-call follow-ups in the same session that handled the call — see `examples/call_and_wa_followup.py`.

Not yet supported: DTMF, recording, transcription events, and packaged copilot whisper/barge pipelines. Each is deliberately out of v1 scope, not an oversight.

## Examples

In-repo examples read credentials from `examples/.env` (see `examples/.env.example`) and run with `uv run --group example python examples/<name>.py`.

| Example | What it shows | Keys beyond AgentDuet |
|---|---|---|
| `examples/inbound_tone_bot.py` | keyless inbound smoke test (barge-in measurement) | none |
| `examples/outbound_tone_bot.py` | keyless outbound dial demo | none |
| `examples/voice_bot.py` | full STT/LLM/TTS quickstart bot | Deepgram (STT+TTS), Google (Gemini) |
| `examples/call_and_wa_followup.py` | voice call + WhatsApp follow-up in one session | none |
| `examples/outbound_track_probe.py` | diagnostic: which track carries the remote party's audio | none |
