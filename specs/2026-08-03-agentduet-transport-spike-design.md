# AgentDuetTransport Spike — Design

| | |
|---|---|
| Status | Approved design, pending implementation plan |
| Date | 2026-08-03 |
| Scope | Milestone 1 (spike) of [specs/pipecat-transport-spec.md](../../../specs/pipecat-transport-spec.md): inbound-only transport, keyless example bot, live validation, barge-in measurement. Outbound dialing is a later round. |
| Verified against | `pipecat-ai` 1.7.0 (installed and inventoried), `agentduet` 1.0.0b10 (local checkout at `~/Documents/wss-sdk-python`) |

## 1. Context and resolved unknowns

The parent spec (Draft v3) defers to a spike the inventory of Pipecat's current
API. That inventory is done; these findings supersede the parent spec's sketches:

- **`PipelineWorker` / `WorkerRunner` are current.** `PipelineParams` lives in
  `pipecat.pipeline.worker`. `PipelineRunner` survives only as a `WorkerRunner`
  subclass; `pipeline/task.py` defines no classes.
- **`StartInterruptionFrame` no longer exists.** The system frame is
  `InterruptionFrame`. The clear-buffer hook is intercepting it in the output
  transport's `process_frame` — the Twilio serializer's pattern
  (`serializers/twilio.py`), not Daily's (Daily has no server-side buffer to
  clear and implements nothing here).
- **VAD is no longer inside the input transport.** In 1.7 it is a pipeline
  stage: `VADProcessor(vad_analyzer=SileroVADAnalyzer())`. `TransportParams`
  has no VAD fields.
- **VAD alone never triggers interruption.** `VADProcessor` emits
  `VADUserStartedSpeakingFrame`; only a `UserTurnProcessor` converts user
  speech into `broadcast_interruption()` → `InterruptionFrame` both
  directions. Any pipeline that wants barge-in must include it. (Interruption
  enablement is a kwarg of the user-turn *start strategy*,
  `enable_interruptions=True` by default — not a `UserTurnProcessor`
  constructor parameter — so a bare `UserTurnProcessor()` already gives
  barge-in.)
- **Worker shutdown from a transport** is `CancelWorkerFrame` (hard) or
  `EndWorkerFrame` (graceful) pushed from either transport half; the worker
  source/sink convert it to `CancelFrame`/`EndFrame` through the pipeline.
- **Transport contract:** subclass `BaseInputTransport` / `BaseOutputTransport`;
  override `start`/`stop`/`cancel`; call `await set_transport_ready(frame)` when
  media may flow; input pushes via `push_audio_frame`; output overrides
  `write_audio_frame(frame) -> bool`; events via `_register_event_handler` /
  `_call_event_handler` on the `BaseTransport`.

Two SDK facts the parent spec did not cover:

- **`Call` has no public inbound/outbound discriminator** (`_origin` is
  private). The transport derives the remote party from public surface instead:
  the party whose `value != call.subscriber`. Track ids are role-fixed locally
  (caller=0, callee=1), so this yields the parent spec's §3 rule without an SDK
  change.
- **`VoiceAgent._bridge` reads `call.caller.audio_stream()` on the outbound
  path too**, where caller is the subscriber (the agent's own leg). Suspected
  latent SDK bug; it is *not* fixed here. The spike's live outbound probe
  (§8) settles empirically which track carries the callee; the finding gets
  reported to the SDK repo separately.

## 2. Deliverables and layout

Package `pipecat-agentduet`, import `pipecat_agentduet`, in this repo.
Dependencies: `agentduet>=1.0.0b10,<2`, `pipecat-ai>=1.7,<2`. Tooling mirrors
the SDK repo: uv, ruff, pytest + pytest-asyncio + pytest-timeout.

```
pyproject.toml
src/pipecat_agentduet/__init__.py     exports AgentDuetTransport
src/pipecat_agentduet/transport.py    AgentDuetTransport + input/output halves
src/pipecat_agentduet/_session.py     _AgentDuetSession — lifecycle, teardown, events
tests/fakes.py                        FakeCall
tests/test_session.py                 teardown matrix, event invariants
tests/test_transport.py               frame plumbing, sample-rate guard
examples/inbound_tone_bot.py          keyless spike bot
```

One `transport.py` per Pipecat house style; split only past ~300 lines.
`_session.py` is separate because it carries all the lifecycle risk and must be
testable with no frames in play.

## 3. Components

### `_AgentDuetSession` (private)

Wraps the attached-not-answered `Call`. Owns answer, hangup wiring, the
teardown latch, and event fan-out. Input and output transports share one
instance (the `WebsocketClientSession` pattern).

- `remote_party` → `call.callee if call.caller.value == call.subscriber else call.caller`.
- `sample_rate` → `call.audio_config.sample_rate`.
- `start()` — latched (idempotent; both halves call it). Registers `on_hangup`
  **before** `answer()` so a hangup mid-answer cannot slip between. Fires
  connected events only on truthy `answer()` and only if the teardown latch is
  unset.
- `close()` — sets `_self_initiated`, then `call.close()` (itself idempotent).
- `_teardown()` — single idempotent choke point for every disconnect path,
  mirroring the SDK's `_mark_terminated`. Order within it: await
  `on_before_disconnect` → fire disconnect events → push `CancelWorkerFrame`
  (only if not `_self_initiated`).

### `AgentDuetTransport(BaseTransport)`

```python
AgentDuetTransport(call, params: TransportParams | None = None)
```

- `params` defaults to audio in+out enabled.
- Constructor **raises** if user-set `audio_in_sample_rate` /
  `audio_out_sample_rate` conflict with `call.audio_config.sample_rate` — the
  parent spec's "no second place to type a rate" rule, enforced. A
  `StartFrame` rate is ignored without a warning — the call's rate always
  wins because the constructor writes it into `TransportParams`. (No warning
  by design: `StartFrame.audio_out_sample_rate` defaults to 24000, so warning
  on mismatch would fire spuriously on every default-config run, and
  differing pipeline rates are legitimate — Pipecat resamples at the
  transport seam.)
- Exposes `transport.call` for SDK capabilities.
- Owns the event registry (`_register_event_handler`); a single
  `_fire(native_name, payload)` helper also invokes the generic twin, making
  the alias rule ("one signal, two names, one moment, identical payload")
  structural rather than conventional.

### Input transport (`BaseInputTransport`)

- `start()`: `set_transport_ready` first (the input audio queue exists only
  after it, and audio can arrive the instant `answer()` succeeds server-side),
  then start the `audio_stream()` pump task, **then** `await session.start()`
  (answer). Starting the pump before answer is safe — `audio_stream()` is
  documented order-independent and lazily bound — and closes the first-words
  gap more tightly than answering first.
- Pump: `async for chunk in session.remote_party.audio_stream():` →
  `push_audio_frame(InputAudioRawFrame(chunk, rate, 1))`. The stream ends
  cleanly on termination (StopAsyncIteration), so the pump task just exits.
- `stop` (EndFrame): cancel pump, then `session.half_stopped()` — **not** an
  immediate close. `BaseInputTransport` pushes EndFrame downstream *before*
  calling `stop()`, so at that moment the farewell audio is still traversing
  STT→LLM→TTS→output; closing the call here would drop it deterministically.
  The session closes when the **last registered half** reports stopped
  (Daily's `_leave_counter` pattern), which honors the parent spec's rule
  "pipeline end reaching the *output* transport ⇒ `call.close()`" while
  keeping input-only pipelines working (one registered half).
- `cancel` (CancelFrame): cancel pump, `session.close()` immediately — cancel
  means drop everything in flight.

### Output transport (`BaseOutputTransport`)

- `start()`: `await session.start()` (latched no-op if input won), then
  `set_transport_ready`.
- `write_audio_frame(frame)`: `await call.send_audio(frame.audio)` — bytes
  untouched (both sides mono s16 PCM at the same rate).
  - `BufferFullError` → drop chunk, warn, return False. Back-pressure, not
    teardown (default ring buffer ≈ 32 s at 16 kHz; drops indicate a stall).
  - `CallClosedError` → return False silently; termination racing the send
    loop is documented SDK behaviour, and `_teardown()` runs from the hangup
    event, not from here.
- `process_frame`: on `InterruptionFrame`, **spawn**
  `call.clear_send_audio_buffer()` as a task (plus `super()`'s handling). Not
  awaited inline: the local ring-buffer flush inside it is instant, but it then
  awaits the `agent.interrupt` server ack with a 10 s timeout, which would
  stall the system-frame path exactly when latency matters. Documented residual
  race: an ack landing after the *next* bot turn starts could flush that
  turn's audio; the next turn needs a full STT→LLM→TTS round trip while the
  ack needs one network hop, so this is accepted and noted in code.
- `stop` (EndFrame): `session.half_stopped()` — by the time
  `BaseOutputTransport.stop()` returns, `MediaSender` has drained its queue
  and appended end-silence, so audio already handed to the SDK is all that
  remains in flight. `cancel` (CancelFrame): `session.close()` immediately.
- Interruption clear tasks are tracked and cancelled in `stop`/`cancel` so a
  barge-in shortly before hangup can't leave a task dangling past the worker
  (production workers warn on dangling tasks by default).

### Session half-latch

`_AgentDuetSession` gains `register_half()` (called once per constructed
transport half) and `half_stopped()` (EndFrame path). When every registered
half has reported stopped, the session runs `close()`. `close()` itself stays
public and immediate for the cancel path. Idempotence rules are unchanged —
`_teardown()` remains the single choke point.

**Open item for live validation (§8):** even with output-side close, audio
already handed to the SDK sits in its client ring buffer (~32 s capacity,
written faster than real time); `call.close()` drops that buffer locally.
Whether a farewell fully plays out depends on how fast the server pull drains
it — measure on a live call; if it truncates, the SDK may need a
drain-before-close, which is an SDK feature request, not transport code.

## 4. Teardown matrix

The invariants, then the rows the tests pin one by one:

- Connected events fire **at most once**, only after a truthy `answer()`, and
  **never after** disconnect events.
- Disconnect events fire **at most once**, from `_teardown()` only, and only
  for calls whose connected events fired (the connected/disconnected pair is
  an alias layer; a call that never connected reports through
  `on_dialin_error` instead, never through a disconnect without a connect).
- `on_before_disconnect` completes before disconnect events fire.
- Worker cancel is pushed whenever teardown was **not** self-initiated
  (`_self_initiated` unset) — that covers remote hangups and every failed or
  dead answer path; a teardown triggered by our own `close()` never pushes it.
- `TERMINATED` is sticky; every path treats it as terminal.

| Termination arrives | Behaviour |
|---|---|
| Before `start()` (call already `TERMINATED`) | skip `answer()`; fire `on_dialin_error` with synthesized `CommandResult(success=False, error_code="CALL_TERMINATED")`; push `CancelWorkerFrame`; connected/disconnect events never fire |
| Mid-`answer()` | `answer()` raises `CallClosedError` → caught; fire `on_dialin_error` with the same synthesized `CALL_TERMINATED` result; push `CancelWorkerFrame`; connected/disconnect events never fire (the SDK's `on_hangup` still fires, but `_teardown()` sees connected-never-fired and skips disconnect events) |
| `answer()` returns falsy | fire `on_dialin_error` with the real `CommandResult`; push `CancelWorkerFrame`; connected/disconnect events never fire |
| `answer()` truthy but teardown latch already set (hangup raced the answer) | suppress connected events; disconnect path has already run or will run from the hangup event |
| Answered, remote hangup | `on_hangup` → `_teardown()`: `on_before_disconnect` → disconnect events → cancel pump → push `CancelWorkerFrame` |
| Pipeline **ends** first (EndFrame) | each half reports `half_stopped()` as EndFrame reaches it; when the last registered half stops, the session runs `close()` (sets `_self_initiated`) → `call.close()` → `_teardown()`: events fire once, **no** worker cancel. Audio in flight between input and output is preserved. |
| Pipeline **cancelled** first (CancelFrame) | first half to see it calls `session.close()` immediately (sets `_self_initiated`); same event behaviour, everything in flight is dropped by design |

## 5. Event surface (spike subset)

All v1 event names from the parent spec §6 are registered (quickstart bots bind
without error); only inbound paths fire this round. Handler signature:
`async def handler(transport, payload)`.

| Event | Fires | Payload |
|---|---|---|
| `on_client_connected` / `on_dialin_connected` | truthy `answer()`, gated on teardown latch | frozen dataclass: `participant: Address`, `call_id: str`, `state: CallState` |
| `on_client_disconnected` / `on_dialin_stopped` | exactly once, from `_teardown()` | same shape |
| `on_dialin_error` | falsy/raising `answer()`, or dead-before-start | `CommandResult` |
| `on_error` | `CallEvent.ERROR` | SDK error payload |
| `on_call_state_updated` | each transition the session drives or observes | `CallState` |
| `on_before_disconnect` | first step of `_teardown()`, awaited | same shape as connected |
| `on_dialout_*` | registered; never fire until the outbound round | — |

Generic and native names fire in one code path with one payload object
(structural alias, §3). Payloads reuse SDK types; no parallel type system.

## 6. Example: `examples/inbound_tone_bot.py`

Raw-SDK arrival per parent spec §4.2 (`SessionManager` + `@sm.on_incoming_call`
→ `open_session` → `process_call` → transport), then:

```
input() → VADProcessor(SileroVADAnalyzer) → UserTurnProcessor()
        → ToneBot → output()
```

(`UserTurnProcessor()` bare: interruptions default to enabled via the start
strategy, see §1.)

`ToneBot` (~30 lines): on user-turn-stopped, streams N seconds of generated
sine tone as `OutputAudioRawFrame`s. Barge-in is audible (tone stops when the
caller speaks) and measurable: log the timestamp of `InterruptionFrame`
reaching the output transport and the `bytes_cleared` payload returned by
`clear_send_audio_buffer` — the two numbers the parent spec wants for the
docs. Connected/disconnected handlers log, proving the generic aliases fire.

Constraint: the spike connector runs at 16 kHz (the default) — Silero VAD
supports only 8 k/16 k. A 24 k connector needs an input resample stage; that is
a docs note, not spike scope.

## 7. Tests

`FakeCall` fakes only the public `Call` surface the transport touches:
scriptable `answer()` result/latency, manual `trigger_hangup()`, recorded
`send_audio` / `clear_send_audio_buffer` / `close` calls, `asyncio.Queue`-backed
party streams.

- `test_session.py`: every teardown-matrix row; every invariant in §4.
- `test_transport.py`: inbound bytes → `InputAudioRawFrame` tagged with the
  call's rate; `write_audio_frame` → `send_audio` byte-identical;
  `InterruptionFrame` → `clear_send_audio_buffer` called without blocking the
  frame path (asserted via a deliberately slow fake); `BufferFullError` →
  chunk dropped, pipeline alive; conflicting constructor rate → raises;
  remote-party selection on both call shapes (subscriber-as-caller and
  subscriber-as-callee).

## 8. Live validation (manual spike exit criteria)

Real inbound call to the 16 kHz connector, no third-party keys:

1. Call in → hear tone after speaking; speak over it → tone stops (barge-in);
   record the measured cutoff numbers.
2. Hang up mid-tone → clean pipeline teardown, disconnect events once.
3. Hang up while ringing (before/during answer) → clean teardown, no connected
   events.
4. One `session.make_call` + `dial()` probe, observing which track carries the
   callee's audio on outbound — settles the `VoiceAgent` discrepancy with
   data before the outbound round is designed.

## 9. Out of scope this round

Outbound dialing (`dial()`, dial-out events firing), whisper/barge/spy,
messaging, DTMF, recording/transcription, `bot(runner_args)` conformance,
publishing to PyPI. All per parent spec; none are unblocked by this spike
except outbound, which the §8 probe de-risks.

## Spike results (live validation, 2026-08-11)

Setup: 16 kHz connector, TELCO caller (+8497…), keyless pipeline
(`input → VADProcessor(Silero) → UserTurnProcessor(speech-timeout stop) →
ToneBot(5 s) → output`), Pipecat 1.7.0, agentduet 1.0.0b10.

### Barge-in (spec §8 measurement)

Five mid-tone interruptions; `clear_send_audio_buffer()` payload = bytes
flushed from the **client** ring buffer, ack = full `agent.interrupt`
round trip from the output transport:

| tone played | cleared (client) | implied server prefetch | ack |
|---|---|---|---|
| 3.22 s | 34 560 B (1.08 s) | 0.70 s | 53 ms |
| 1.87 s | 71 680 B (2.24 s) | 0.89 s | 60 ms |
| 2.07 s | 65 280 B (2.04 s) | 0.89 s | 72 ms |
| 3.25 s | 29 440 B (0.92 s) | 0.83 s | 64 ms |

Findings:

- **Ack round trip: 50–72 ms** across all interruptions (n=9, including
  empty-buffer ones). The clear is spawned, so none of this sits on the
  frame path.
- **Server flow control prefetches a consistent ~0.7–0.9 s** ahead of
  playout (expected-remaining minus client-cleared, stable across runs).
  That prefetched audio is precisely what the *server-side* flush kills;
  a client-only clear would leave ~0.9 s of stale audio playing after
  every barge-in. Confirms the spec §8 claim that
  `clear_send_audio_buffer()` must and does flush both sides.
- **Estimated speech-onset → audible-stop cutoff: ≈ 250–350 ms**,
  dominated by VAD detection (`start_secs=0.2`), plus one server hop
  (~25–35 ms one-way) and carrier playout residue. Caller-reported
  behavior matched the turn-taking design.

### Lifecycle

- Connected/disconnected alias events fired exactly once per call with
  correct payloads; remote hangup → `CancelWorkerFrame(reason: remote
  hangup)` → clean worker teardown; process served multiple sequential
  calls on fresh transports; SIGINT disconnected cleanly.
- "Few hundred lines" estimate: confirmed — 461 lines in
  `src/pipecat_agentduet/`.

### Lessons for the v1 examples/docs

- **Pipecat's default user-turn stop strategy (Smart Turn v3) judges
  test phrases as incomplete turns** and stalls the bot's reply by
  5–15 s (until the stop-timeout, `strategy: None`). Keyless demos must
  pin `SpeechTimeoutUserTurnStopStrategy(wait_for_transcript=False)`;
  real STT pipelines can keep the default. Worth a docs callout.
- Interruptions with an empty buffer (user speaks during bot silence)
  are normal and log `cleared 0` — harmless.

### Still open

- Hangup-while-ringing (matrix row 1/2 live) and the ring-buffer
  drain-on-close question (§3 open item) — not yet observed live.
- Outbound track probe (§8 item 4) — not yet run; the VoiceAgent
  caller/callee question remains open.
