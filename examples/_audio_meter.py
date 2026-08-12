"""Optional diagnostic — not part of the package, not wired by default.

Wire AudioMeter right after transport.input() (plus
`logging.getLogger("agentduet").setLevel(logging.DEBUG)`) when
investigating inbound-audio problems, e.g. the intermittent mid-call
stall recorded in the design doc's v1 results. It logs, every 5 s (timer-driven,
so it keeps reporting even when NO frames arrive), how many inbound audio bytes
passed and the loudest sample seen in the window. It discriminates the three
inbound-stall shapes a plain pipeline log cannot:

  ~sample_rate*2 B/s, peak in the thousands -> audio + speech reaching the pipeline
  ~sample_rate*2 B/s, peak near 0           -> frames flowing but SILENT
                                               (carrier one-way audio / caller muted)
  0 B/s                                     -> media stopped reaching the client
                                               (server stopped forwarding, or the
                                               carrier stopped delivering RTP)

Only imported by scripts that set NLTK_DISABLE_IMPORT_SECURITY first.
"""

import audioop
import logging

from pipecat.frames.frames import CancelFrame, EndFrame, Frame, InputAudioRawFrame, StartFrame
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

logger = logging.getLogger("audio_meter")


class AudioMeter(FrameProcessor):
    def __init__(self, *, interval_secs: float = 5.0):
        super().__init__()
        self._interval = interval_secs
        self._window_bytes = 0
        self._window_peak = 0
        self._ticker = None

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, StartFrame) and self._ticker is None:
            self._ticker = self.create_task(self._tick())
        elif isinstance(frame, (EndFrame, CancelFrame)) and self._ticker is not None:
            await self.cancel_task(self._ticker)
            self._ticker = None
        elif isinstance(frame, InputAudioRawFrame):
            self._window_bytes += len(frame.audio)
            self._window_peak = max(self._window_peak, audioop.max(frame.audio, 2))
        await self.push_frame(frame, direction)

    async def _tick(self):
        import asyncio

        while True:
            await asyncio.sleep(self._interval)
            logger.info(
                "inbound: %d B/%.0fs (%.0f B/s), peak sample %d",
                self._window_bytes,
                self._interval,
                self._window_bytes / self._interval,
                self._window_peak,
            )
            self._window_bytes = 0
            self._window_peak = 0
