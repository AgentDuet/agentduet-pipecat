# AgentDuet Pipecat Transport: Design Spec

| | |
|---|---|
| Status | Draft v3 for review (v3 drops the host module; arrival is the raw SDK, `bot()` conformance deferred upstream) |
| Audience | SDK engineers, integration engineers |
| Scope | A Pipecat transport built on the AgentDuet Python SDK |
| Depends on | `agentduet` (this SDK), `pipecat-ai` |

## 1. Overview

Pipecat is the leading open-source orchestration framework for voice AI agents. This spec
defines an AgentDuet integration for Pipecat with a single deliverable:
**`AgentDuetTransport`**, a Pipecat transport that moves audio and call lifecycle
between a live AgentDuet `Call` and a Pipecat pipeline. It takes a `Call` and does not
care who produced it.

The adapter ships **no arrival API of its own**: no runner class, no host function, no
adapter-level decorators. Arrival is the SDK's existing surface
(`@sm.on_incoming_call`, with `@sm.on_incoming_message` beside it), which is already
the correct API and, unlike Pipecat's call-scoped `bot(runner_args)` convention,
carries both channels (calls and messages) in one process. `bot(runner_args)`
conformance is deferred to the upstream dev-runner contribution (sections 4.3, 9),
where Pipecat's own runner hosts it.

One transport class makes AgentDuet compatible with every STT, LLM, and TTS service
Pipecat supports, and replaces the per-framework example burden (Gemini Live, ADK,
Nova Sonic) with a single maintained adapter.

### Why the transport level, not a serializer

Pipecat has two integration styles:

- **Serializer** (Twilio, Telnyx, Plivo, Exotel): the carrier initiates a raw websocket
  to the developer's public server; a small serializer translates the wire format. Fits
  vendors whose interface is an inbound media stream and who ship no realtime SDK.
- **Dedicated transport** (Daily, LiveKit): the vendor SDK owns the connection and dials
  out to the vendor cloud; the transport wraps the SDK.

AgentDuet is unambiguously the second kind: the SDK dials out over its dual-connection
setup, so there is no inbound websocket for a serializer to translate. The transport also
gives us the capability ceiling the serializer level cannot express: per-party tracks,
whisper/barge/spy, the session model, and transparent reconnect.

The transport is built **on top of the SDK**, not against the wire protocol. The SDK
already owns reconnect with backoff, the `session.claim` handshake, flow control, and the
delivery gate; a second wire-level client would duplicate all of it and create protocol
drift, which is exactly what `specs/wire-protocol-spec.md` and its CI gate exist to
prevent.

## 2. Goals and non-goals

### Goals (v1)

- Inbound: incoming call answered by a Pipecat pipeline (caller audio in, agent audio out).
- Outbound: `session.make_call(dest)` plus the transport (which dials on pipeline start).
- Full interruption support (barge-in) via `clear_send_audio_buffer()`.
- Event surface compatible with Pipecat convention (section 6), so standard example
  bots work unmodified.
- Zero audio-format configuration on the transport (section 7).
- Mutually linked lifecycles: pipeline end closes the call; hangup cancels the pipeline.

### Non-goals (v1)

- Whisper/barge/spy copilot pipelines (both party tracks in, output whispered to one
  side). This is the key differentiator but maps awkwardly onto Pipecat's single-user
  pipeline assumption; it lands as a follow-up once the plain transport is proven.
  The `Call` methods remain reachable via `transport.call`.
- Messaging (`msg.in`, `send_message`). Pipecat pipelines are per-call; WhatsApp
  messaging belongs at the application level (reachable via the session, see
  section 4.4) or in a separate integration.
- DTMF. Requires a wire-protocol addition (section 10).
- Recording, transcription events, data channels: no SDK backing; not exposed.
- A `bot(runner_args)` host in the adapter package. The convention is call-scoped and
  has no slot for connector-level events, so incoming messages could only be supported
  via non-portable kwargs of our own invention, recreating the parallel-API problem
  this design removes. Deferred to the upstream dev-runner contribution (section 4.3).
- Pipecat Cloud deployment claims. Their Cloud spawns bot processes on demand per
  session via their own arrival infrastructure; our arrival is a persistent outbound
  SessionManager connection. Until an always-on listener mode is verified with them,
  the honest portability claim is "same bot code, self-hosted host".

## 3. Background: how the pieces line up

The Pipecat and AgentDuet abstractions map almost one to one:

| Pipecat concept | AgentDuet SDK |
|---|---|
| Input frames (`InputAudioRawFrame`) | the remote party's `audio_stream()`: `call.caller` for inbound calls, `call.callee` for outbound (select by call origin) |
| Output frames (`OutputAudioRawFrame`) | `call.send_audio()` |
| Interruption (`StartInterruptionFrame`) | `call.clear_send_audio_buffer()` |
| Client disconnected event | `call.on_hangup` |
| Transport sample-rate params | `CallAudioConfig.sample_rate` |

### Pipecat API drift caveat

Pipecat's pipeline execution API has moved recently: current examples and docs use
`PipelineWorker` / `WorkerRunner` where older material (and some sketches in this spec)
used `PipelineTask` / `PipelineRunner`. The concepts we design against (frames,
transports, system-frame interruption, the `bot(runner_args)` entrypoint) are stable;
class names are not. **Spike task 1 is an inventory of the current API surface**
(pipeline execution classes, frame names, runner-args types) before any transport code
is written. Code sketches in this spec are conceptual.

## 4. Architecture

### 4.1 The arrival layer

Inbound calls arrive at unpredictable times on a connector-wide stream, so something
long-running must listen and spawn one pipeline per call. This layer necessarily sits
**before** the transport: `@transport.event_handler(...)` requires a transport
instance, a transport instance requires a `Call`, and a `Call` only exists after a
notification is attached. One transport = one call = one pipeline; when call #2
arrives while call #1 is live, only the arrival layer can construct the second
transport and pipeline.

Every inbound telephony integration in Pipecat has this layer; it is just not Pipecat
API. For the Twilio family it is the developer's FastAPI server. For us it is the
SDK's `SessionManager`, which also means nothing needs to be publicly reachable: the
SDK connects outbound, so there is no webhook, no public endpoint, and no tunnel in
development.

```
                        AgentDuet cloud
                              |
              SM-WS (persistent)   voice connection (per call)
                              |         |
   +--------------------------------------------------+
   |  Arrival layer (the raw SDK)                      |
   |    @sm.on_incoming_call     -> transport+pipeline |
   |    @sm.on_incoming_message  -> session messaging  |
   +---------------------|----------------------------+
                         |  one per call
   +---------------------v----------------------------+
   |  Pipecat pipeline                                 |
   |  [ AgentDuetTransport.input()  -> STT -> LLM      |
   |    -> TTS -> AgentDuetTransport.output() ]        |
   +--------------------------------------------------+
```

### 4.2 The arrival code

No adapter API at all; the SDK's existing surface plus the transport:

```python
async with SessionManager(config) as sm:

    @sm.on_incoming_call
    async def on_call(noti: IncomingCallNotification):
        session = await sm.open_session(new_session_id(), noti.subscriber)
        transport = AgentDuetTransport(await session.process_call(noti))
        await run_bot(transport)          # user's transport-agnostic bot logic

    await sm.run_forever()
```

Three lines of arrival boilerplate. `process_call` attaches without answering
(`answer()` is a separate step the transport performs), so the SDK already provides
the attach-vs-answer split the transport needs. An optional convenience
`AgentDuetTransport.from_notification(sm, noti)` may collapse the two middle lines;
it is sugar, not architecture.

Per-call decisions (screening, CRM lookup by caller, choosing a system prompt) happen
naturally in the handler, before the pipeline exists. Messaging sits beside calls in
the same process: register `@sm.on_incoming_message` next to `@sm.on_incoming_call`,
and the one arrival layer carries the whole multi-modal story.

### 4.3 Deferred: `bot(runner_args)` entrypoint conformance

Pipecat's portable convention (used by its development runner and by Pipecat Cloud)
is a standard entrypoint, `async def bot(runner_args)`, invoked by a host once per
connection. It is deliberately **not** shipped in the adapter package, for one
decisive reason: the convention is call-scoped, with no slot for connector-level
events, so incoming messages (`msg.in`) cannot reach a bot through it. A host of ours
could only add messaging via kwargs of our own invention (`run(bot, on_message=...)`)
that no other host would honor: a non-portable parallel API, and it would market the
integration as voice-only besides.

What the convention offers is handled upstream instead (section 9): a
`pipecat/runner/agentduet.py` contribution can host `bot(AgentDuetRunnerArguments)`
for voice-only bots living in the dev-runner workflow. That contribution owns the
runner-args type, `create_transport` dispatch, and a `session_id_factory` hook
(preserving the SDK's caller-supplied session id capability, e.g. one session id per
(subscriber, participant) pair for conversational continuity).

Bot *logic* portability does not depend on any of this: the transport-agnostic
`run_bot(transport)` function ports across vendors regardless of which arrival code
calls it. What is given up until the upstream PR lands is only bot-*file*
portability, worth three lines of `main()`.

### 4.4 Progressive disclosure

The quickstart shows `SessionManager` only as connection boilerplate (a context
manager and a decorator); everything underneath stays hidden until needed:

```
transport only  ->  transport.call  ->  call/session  ->  SDK internals
(pipeline code)     (whisper/barge)     (messaging,        (claims, reconnect,
                                         continuity)        delivery gate)
```

The differentiators live one attribute away: a "call ends, send a WhatsApp follow-up
in the same session" flow needs the session handle; a copilot pipeline needs
`transport.call.whisper()`. Docs should not explain sessions or the connection
machinery until a "going deeper" section.

### 4.5 Horizontal scaling (inherited, worth advertising)

Delivery is connector-wide competing-consumer: run N bot processes on one connector
and calls distribute across them, with no load balancer and no sticky sessions. This
is the same outbound-worker dispatch shape as LiveKit Agents' worker model, which the
market treats as best-in-class DX; ours comes free from the SDK's delivery model and
belongs in the integration docs as a production story.

### 4.6 AgentDuetTransport

Takes an attached (but not yet answered/dialed) `Call` plus transport params. Pure
frame plumbing and lifecycle translation; it never touches connector state. Exposes
`transport.call` for SDK capabilities (whisper/barge/spy, session access).

## 5. Call lifecycle mapping

### Inbound

1. Arrival layer attaches the call (`RINGING`) and builds transport + pipeline.
2. On pipeline start, the transport calls `call.answer()`. Answering inside
   `transport.start()` (the DailyTransport join-on-start pattern) means the pipeline
   (STT, LLM, TTS) is fully wired before media flows, so the caller's first words are
   never dropped.
3. Answer succeeds (`LIVE`): fire `on_dialin_connected` + `on_client_connected`.
4. Hangup (`call.on_hangup`): cancel the pipeline worker (the current-API pattern;
   Daily's canonical example cancels on `on_participant_left`) and fire
   `on_dialin_stopped` + `on_client_disconnected`.
5. Pipeline ends first (end/cancel reaches the output transport): the transport calls
   `call.close()`. "Pipeline done means call ends" holds without user code, mirroring
   the Twilio serializer's auto-hangup contract.

### Outbound

1. `session.make_call(dest)` yields a `Call` in `NEW`; transport and pipeline are
   built the same way (no arrival layer needed: the call is the process's reason to
   exist, so a plain `main()` works, as in Pipecat's own transport examples).
2. On pipeline start, the transport spawns `call.dial(ring_time_seconds=...)` **as a
   task**. `dial()` does not return until answered or failed (default ring time 60 s),
   so awaiting it inline would stall pipeline startup; spawned, the pipeline sits idle
   (correctly, there is no audio yet) until the dial resolves.
3. The SDK has no "answered" event; answered is the resolution of the awaited `dial()`.
   `CommandResult.success` true: fire `on_dialout_answered` + `on_client_connected`.
   False (unanswered/timeout/error): fire `on_dialout_error` with the `CommandResult`
   as payload, then cancel the worker (no call, no conversation).
4. Hangup and pipeline-end behave as in inbound (with `on_dialout_stopped`).

### Robustness requirement

Hangup can arrive at **any** state, including while `answer()` or `dial()` is in
flight (caller gives up while ringing). The transport must tear the pipeline down
cleanly from every state. This is the fiddliest part of the implementation; the audio
plumbing is easy by comparison. `TERMINATED` is sticky in the SDK and must be treated
as terminal everywhere.

## 6. Event surface

### Design rules

1. If Pipecat has a name for it, use their name. If Pipecat has a frame for it, use the
   frame (audio, interruption, DTMF are frames, never events). Only when neither exists
   do we invent an event, named after the capability, not the wire protocol.
2. Frames are in-band pipeline control; events are out-of-band app callbacks. A hangup
   uses both channels: pipeline teardown (worker cancel) and
   `on_client_disconnected` (lets app code log, update CRM).
3. The generic pair (`on_client_connected` / `on_client_disconnected`) is an **alias
   layer** fired in the same code path as the corresponding native dial-in/dial-out
   event: one signal, two names, one moment. This is the DailyTransport pattern
   (its generic pair aliases participant joined/left).
4. Payload parity: generic and native events carry the same payload, and inbound and
   outbound carry the same shape, so a handler written against the generic event is
   fully portable. Payloads reuse SDK types (`Address`, `CommandResult`, `CallState`);
   the transport introduces no parallel type system.

Rationale for strict convention compliance: Pipecat's docs, quickstarts, and example
bots are written against `on_client_connected`. The LiveKit transport diverged (room
native names only, no generic aliases) and every standard example silently breaks on
it. A developer evaluating this transport by pasting the quickstart bot must have it
work unmodified.

### v1 events

| Event | Fires when | Payload |
|---|---|---|
| `on_client_connected` | Call reaches `LIVE` (alias, both directions) | remote party `Address` + call metadata |
| `on_client_disconnected` | Hangup (alias, both directions) | same shape |
| `on_error` | `CallEvent.ERROR`, transport failure | error info |
| `on_dialin_connected` | `answer()` succeeds | as generic |
| `on_dialin_stopped` | Inbound hangup | as generic |
| `on_dialin_error` | `answer()` fails | `CommandResult` |
| `on_dialout_answered` | `dial()` resolves success | as generic |
| `on_dialout_stopped` | Outbound hangup | as generic |
| `on_dialout_error` | `dial()` resolves failure | `CommandResult` |
| `on_call_state_updated` | Each `CallState` transition the transport drives or observes | `CallState` |
| `on_before_disconnect` | Just before transport teardown | last chance to flush/log (CRM writes) |

### Deliberately omitted

| Not exposed | Reason |
|---|---|
| `on_dialout_connected` (vs answered) | `dial()` has one resolution point; cannot distinguish SIP leg up from human answered. A lying event is worse than a missing one. |
| Ringing events | `RINGING` is set locally when the dial command is sent, not on remote ringing; Daily exposes no ringing event either. `on_call_state_updated` covers debugging. |
| `on_dialin_ready`, warnings | Daily plumbing / no warning channel in our protocol. |
| Incoming call | Arrival layer, not a transport event: no transport exists yet (section 4.1). |
| Wire/session states (`session.claim`, reconnect, `OPENING`) | The SDK's value is hiding these; exposing them creates permanent API surface. |
| `msg.in` | Wrong layer (see non-goals; reachable via the session). |
| DTMF, recording, transcription, app messages | No SDK/protocol backing (see section 10). |

## 7. Audio

Pipecat's in-pipeline format is raw PCM, 16-bit signed, mono, in self-describing frames
(each `AudioRawFrame` carries `sample_rate` and `num_channels`). Rates are configured
once in `PipelineParams` and propagate via `StartFrame`; Pipecat inserts SOXR resampling
at the seams (TTS native rate down to transport rate, input up to what STT needs).
Silero VAD runs in the input transport and natively supports 8000 and 16000 Hz.

AgentDuet delivers mono 16-bit PCM at 8000, 16000, or 24000 Hz, fixed
**connection-level** in the SM-WS setup frame (`CallAudioConfig.sample_rate`,
default 16000). Consequences:

- **Zero format conversion.** Both sides are mono s16 PCM; bytes pass through untouched.
  (The Twilio-family serializers transcode 8 kHz mulaw on every frame.)
- **The default is Pipecat's happy path.** 16 kHz is Silero's native rate and what most
  STT wants: the default pipeline needs no input resampling at all. The 24 kHz option
  lets high-quality TTS reach the caller without downsampling, which 8 kHz carriers
  structurally cannot offer.
- **No audio configuration on the transport.** Because the rate is fixed per connector,
  the transport reads `call.audio_config.sample_rate`, tags input frames with it, and
  declares the same as its output rate. The user sets the rate once, in
  `CallAudioConfig`, and cannot create a mismatch because there is no second place to
  type it. (Sample-rate mismatch is the classic telephony-transport support issue.)
- **Output pacing composes.** Pipecat's output transport writes 10 ms-multiple chunks
  into `call.send_audio()`; the SDK ring buffer absorbs them; the server pulls at line
  rate via flow control.

## 8. Interruption (barge-in)

Pipecat detects user speech via VAD and pushes `StartInterruptionFrame` as a **system
frame** (processed immediately, jumping all data queues). Each stage cancels and
flushes: LLM aborts the in-flight completion, TTS discards pending synthesis, the
output transport flushes queued chunks. The transport-specific duty is clearing audio
that already left the pipeline for the network.

Three buffer layers at the moment of interruption:

| Layer | Owner | Cleared by |
|---|---|---|
| Pipecat internal queues | Pipecat | automatic on `StartInterruptionFrame` |
| SDK ring buffer | transport | `call.clear_send_audio_buffer()` (one line) |
| Audio the server already pulled | server | the same call: the flush is server-side too (confirmed) |

`clear_send_audio_buffer()` flushes the full path, client ring buffer and server-side
buffer alike, so the only residual is audio already in the carrier playout path:
effectively the floor any telephony integration can reach. **Spike task:** measure the
end-to-end barge-in cutoff anyway; the figure goes in the docs.

Recommendation for example code: enable Pipecat interruption strategies (min-words) so
backchannels ("mm-hmm") do not kill the bot mid-sentence; phone audio is
backchannel-heavy.

## 9. Packaging and distribution

- **Separate public package** (working name `pipecat-agentduet`), not a module in this
  repo: the adapter needs public source to be listable anywhere, and Pipecat's release
  cadence (the API drift in section 3 is live evidence) must not force SDK releases.
  The package ships the transport and examples only; it contains no arrival code. Pin
  `agentduet>=X,<Y`; the wire-spec gate keeps the protocol contract honest underneath.
- **Examples strategy** mirrors how first-class transports present themselves:
  - A **native showcase** (`transports-agentduet.py` style, matching the house style
    of Pipecat's `examples/transports/` folder: custom `main()`, native event names,
    direct construction), doubling as the quickstart. It must include an explicit STT
    stage (Daily's example omits STT because the transport supplies transcription;
    ours does not).
  - A **multi-modal example**: `on_incoming_call` and `on_incoming_message` side by
    side, with a post-call WhatsApp follow-up in the same session. This is the
    differentiator no other transport can demonstrate.
  There is one inbound pattern, the decorator, for demos and services alike: one
  thing to learn, nothing to migrate when a demo becomes a service.
- **Upstreaming**, in order of value:
  1. The transport into `pipecat-ai` as an in-tree service behind an extra
     (`pip install pipecat-ai[agentduet]`), the same shape as Daily and LiveKit.
  2. A `pipecat/runner/agentduet.py` with real arrival support (`-t agentduet`) and
     the `bot(runner_args)` conformance deferred from v1 (section 4.3). Precedent:
     per-vendor modules under `pipecat.runner` exist today; LiveKit's stops at token
     helpers because its dispatch lives in LiveKit Agents, outside Pipecat's reach.
     Our dispatch receiver is the SDK itself, so we can clear a bar LiveKit
     structurally could not.
  Both are discoverability and convention-blessing (their docs, their extras list,
  the `-t` workflow), not capability: everything works from our own package plus the
  SDK on day one.
- **Upstream framing:** lead with what the ecosystem lacks (subscriber identity,
  copilot on real calls, voice + WhatsApp in one session), not with being a simpler
  Daily PSTN alternative. AgentDuet is a capability provider, complementary to Pipecat
  and to Daily's hosting business (Pipecat Cloud); the only competitive overlap is the
  commodity "agent answers a number" slice Daily already shares with four carriers.

### Prerequisites

1. **`agentduet` on PyPI**: already satisfied. The SDK ships on pypi.org (TestPyPI is
   staging only); the adapter pins a released version.
2. **Decision: public adapter repo** (org, name, license). ~~The one true blocker for
   *publishing* the adapter.~~ **Resolved 2026-08-12:** public repo
   `AgentDuet/agentduet-pipecat`, package `pipecat-agentduet`, licensed
   **BSD-2-Clause** (copyright AgentDuet) — matching `pipecat-ai` exactly so the
   §11.3 upstream contribution needs no relicensing. The core SDK stays proprietary.

## 10. Protocol gaps surfaced by this work

These are server + wire-protocol feature requests (each requires a spec revision under
the drift gate), not transport work:

1. **DTMF (inbound digits).** Every competing telephony integration supports it
   (Daily `on_dtmf_event`, serializers via `InputDTMFFrame`); IVR-style interactions
   are bread and butter for phone agents. First protocol addition to queue.
2. **Dial progress** (remote ringing, busy vs no-answer vs rejected): needed eventually
   for outbound campaign use cases; `dial()`'s single resolution point is fine for v1.

(A server-side send-buffer flush was on this list; confirmed already present:
`clear_send_audio_buffer()` flushes server-side, see section 8.)

## 11. Milestones

1. **Spike:**
   - Inventory the current Pipecat API surface (section 3 caveat) before writing code.
   - Transport + raw-SDK arrival against an existing example scenario.
   - Measure barge-in cutoff; validate the "few hundred lines" estimate.
2. **v1:** event surface per section 6, both examples, docs with a quickstart
   mirroring Pipecat's Twilio page (the side-by-side sells the DX: no public server,
   no tunnel, no serializer; credentials plus a decorator).
3. **Upstream PRs** (transport, then runner arrival support with `bot()` conformance,
   `create_transport` dispatch, and the `session_id_factory` hook) once the API stops
   moving.
4. **Follow-ups:** copilot mode (whisper/barge pipelines), DTMF once the protocol
   lands, messaging patterns at the session level, Pipecat Cloud always-on
   feasibility.

## 12. Open questions

1. ~~Public-repo mechanics for the adapter (org, name, license).~~ Resolved
   2026-08-12 — see section 9 prerequisite 2.
2. Upstream runner contribution details (deferred with it): `create_transport`
   dispatch for our runner-args type, and whether Pipecat Cloud has an always-on
   listener mode that fits a persistent SessionManager connection.
