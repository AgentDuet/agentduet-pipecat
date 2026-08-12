# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository state

**v1 is complete, merged, and live-validated** (2026-08-12): `AgentDuetTransport` handles inbound (answer) and outbound (dial) with the full parent-spec §6 event surface, plus four examples (keyless inbound/outbound tone bots, a Deepgram+Gemini showcase with an LLM `hang_up` tool, a multi-modal WA-follow-up bot) and a quickstart README. The plan was [specs/2026-08-11-v1-plan.md](specs/2026-08-11-v1-plan.md); live-validation results (spike + v1 rounds: barge-in numbers, outbound failure rows, drain-on-close resolved, the WA subscriber-identity split, one open inbound-stall intermittent) are recorded in [specs/2026-08-03-agentduet-transport-spike-design.md](specs/2026-08-03-agentduet-transport-spike-design.md) under "Spike results" and "v1 results". The parent spec is [specs/pipecat-transport-spec.md](specs/pipecat-transport-spec.md) (Draft v3; its code sketches are conceptual — where they disagree with the code, the code is right). Next per parent spec §11: upstream PRs once the API stops moving; follow-ups (DTMF, copilot, dial progress) per §10/§12.

Layout: `src/pipecat_agentduet/` (package: `transport.py`, `_session.py`), `tests/` (fakes + session/transport suites, no network), `examples/` (need real credentials in `examples/.env` — see `examples/.env.example`; `.env` is gitignored).

Commands (uv-managed; no CI yet):

- Tests: `uv run pytest`
- Lint: `uv run ruff check` (format: `uv run ruff format`)
- Examples: `uv run --group example python examples/inbound_tone_bot.py` (Silero/onnxruntime live in the `example` group, deliberately out of the test env)

`conftest.py` sets `NLTK_DISABLE_IMPORT_SECURITY=1` — nltk's import-security hook false-positives on the project-local `.venv` layout and breaks pipecat imports. Example scripts set it themselves before any pipecat import; any new entrypoint must do the same.

The deliverable is a public package (`pipecat-agentduet`) depending on `agentduet` (the AgentDuet Python SDK, on PyPI) and `pipecat-ai`. Public-repo mechanics (org, name, license) are still an open question (parent spec §12).

## What is being built

A single class, `AgentDuetTransport`: a Pipecat transport that moves audio and call lifecycle between a live AgentDuet `Call` and a Pipecat pipeline. It takes an attached-but-not-yet-answered `Call` and does pure frame plumbing plus lifecycle translation. It never touches connector state.

Core mapping (spec §3):

| Pipecat | AgentDuet SDK |
|---|---|
| `InputAudioRawFrame` | remote party's `audio_stream()` — `call.caller` inbound, `call.callee` outbound |
| `OutputAudioRawFrame` | `call.send_audio()` |
| `InterruptionFrame` | `call.clear_send_audio_buffer()` (flushes client ring buffer *and* server-side) |
| client disconnected | `call.on_hangup` |
| transport sample rate | `call.audio_config.sample_rate` |

## Architectural decisions that are settled — do not relitigate

These were decided in the spec with reasons; changing them requires a spec revision, not a code change.

- **Transport level, not a serializer.** The SDK dials outbound, so there is no inbound websocket for a serializer to translate. Build on the SDK, never against the wire protocol — the SDK already owns reconnect/backoff, the `session.claim` handshake, flow control, and the delivery gate, and a second wire-level client would cause protocol drift.
- **The adapter ships no arrival API.** No runner class, no host function, no adapter-level decorators. Arrival is the SDK's existing `@sm.on_incoming_call` / `@sm.on_incoming_message` on a `SessionManager`. The adapter package contains no arrival code at all.
- **No `bot(runner_args)` entrypoint in this package.** The convention is call-scoped with no slot for connector-level events, so `msg.in` could only be carried by non-portable kwargs of our own invention. Deferred to an upstream `pipecat/runner/agentduet.py` contribution.
- **Zero audio configuration on the transport.** Both sides are mono s16 PCM (8k/16k/24k, fixed connection-level in `CallAudioConfig`, default 16000). Bytes pass through untouched; the transport reads the rate from the call and declares the same on output. There must be no second place to type a sample rate.
- **Strict Pipecat event-name compliance.** Generic `on_client_connected` / `on_client_disconnected` are an *alias layer* fired in the same code path as the native dial-in/dial-out event — one signal, two names, one moment, identical payloads across inbound and outbound. LiveKit diverged here and silently breaks every standard example bot; a pasted Pipecat quickstart must work unmodified.
- **Payloads reuse SDK types** (`Address`, `CommandResult`, `CallState`). No parallel type system.
- **Events vs frames:** if Pipecat has a frame for it (audio, interruption, DTMF), it is a frame, never an event. Invent an event only when Pipecat has neither, and name it after the capability, not the wire protocol.
- **Don't expose wire/session states** (`session.claim`, reconnect, `OPENING`). Hiding these is the SDK's value; exposing them creates permanent API surface.

## Lifecycle rules the implementation must honor

- Answer/dial happen **on pipeline start** (`transport.start()`), so STT/LLM/TTS are wired before media flows and the caller's first words aren't dropped.
- Outbound `call.dial()` must be spawned **as a task**, not awaited inline — it doesn't return until answered or failed (default 60 s ring), which would stall pipeline startup. "Answered" is the resolution of that task; there is no SDK answered event.
- Pipeline end (or cancel) reaching the output transport ⇒ transport calls `call.close()`. No user code required.
- Hangup ⇒ cancel the pipeline worker and fire the disconnect events.
- **Hangup can arrive in any state**, including mid-`answer()` or mid-`dial()`. Clean teardown from every state is the fiddliest part of the work — the audio plumbing is easy by comparison. `TERMINATED` is sticky in the SDK; treat it as terminal everywhere.

## Pipecat API drift

Pipecat's pipeline execution API moves: current is `PipelineWorker` / `WorkerRunner` / `InterruptionFrame` / `CancelWorkerFrame` (older material says `PipelineTask` / `PipelineRunner` / `StartInterruptionFrame`). The spike pinned verified API facts for pipecat-ai 1.7.0 and agentduet 1.0.0b10 in the header of [specs/2026-08-03-agentduet-transport-spike-plan.md](specs/2026-08-03-agentduet-transport-spike-plan.md) — trust those and the code over the parent spec's sketches. When touching new API surface (or after a dependency bump), verify class names against the installed `pipecat-ai` rather than from memory.

## Explicit v1 non-goals

Whisper/barge/spy copilot pipelines (reachable via `transport.call`, but a follow-up), messaging (`msg.in` — belongs at the session/application level), DTMF (needs a wire-protocol addition), recording, transcription events, data channels, and Pipecat Cloud deployment claims. Don't add these opportunistically; each was excluded for a stated reason.
