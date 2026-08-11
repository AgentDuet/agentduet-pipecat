"""One-shot probe: dial out, then report which track (caller/callee) carries
the remote party's audio. Settles the VoiceAgent caller/callee discrepancy
with data before the outbound transport round is designed.

Env (read from the environment or a .env file in the repo root):
AGENTDUET_API_KEY, AGENTDUET_CONNECTOR_UUID, optional AGENTDUET_BASE_URL,
PROBE_SUBSCRIBER (the line to call from), PROBE_DEST (E.164 number to call).
Run:  PROBE_SUBSCRIBER=+65… PROBE_DEST=+65… \
          uv run --group example python examples/outbound_track_probe.py
Answer the phone and speak; the probe logs bytes per track for 10 s.
"""

import asyncio
import logging
import os
import uuid

from agentduet import Address, CallAudioConfig, SessionManager, SessionManagerConfig
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("probe")


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"set {name} in the environment or .env")
    return value


async def count_bytes(name: str, party, results: dict):
    total = 0
    try:
        async for chunk in party.audio_stream():
            total += len(chunk)
            results[name] = total
    except asyncio.CancelledError:
        results[name] = total
        raise


async def main():
    load_dotenv()  # same .env the tone-bot example uses
    api_key = _require_env("AGENTDUET_API_KEY")
    connector_uuid = _require_env("AGENTDUET_CONNECTOR_UUID")
    probe_subscriber = _require_env("PROBE_SUBSCRIBER")
    probe_dest = _require_env("PROBE_DEST")
    config = SessionManagerConfig.create(
        api_key=api_key,
        connector_uuid=connector_uuid,
        base_url=os.getenv("AGENTDUET_BASE_URL"),
        call_audio=CallAudioConfig(sample_rate=16000),
    )
    async with SessionManager(config) as sm:
        session = await sm.open_session(uuid.uuid4().hex, probe_subscriber)
        call = await session.make_call(Address.telco(probe_dest))
        logger.info("dialing %s (call %s)…", probe_dest, call.id)
        result = await call.dial()
        if not result:
            logger.error("dial failed: %s", result.error_code)
            return
        logger.info("answered. speak into the phone; sampling both tracks for 10 s…")
        results: dict[str, int] = {}
        tasks = [
            asyncio.create_task(count_bytes("caller(track0)", call.caller, results)),
            asyncio.create_task(count_bytes("callee(track1)", call.callee, results)),
        ]
        await asyncio.sleep(10)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("subscriber=%s caller=%s callee=%s", call.subscriber, call.caller, call.callee)
        logger.info("bytes per track: %s", results)
        logger.info(
            "=> the remote party's audio is on the track with the (much) larger count"
        )
        await call.close()


if __name__ == "__main__":
    asyncio.run(main())
