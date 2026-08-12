"""Shared ToneBot: plays a sine tone after each user turn, stops it on
barge-in. Used by both inbound_tone_bot.py and outbound_tone_bot.py.

Not guarded by the NLTK_DISABLE_IMPORT_SECURITY preamble itself — this
module is only ever imported by entry scripts that set the env var before
importing pipecat, so importing pipecat frame classes here is fine.
"""

import math
import struct

from pipecat.frames.frames import (
    Frame,
    InterruptionFrame,
    OutputAudioRawFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor


class ToneBot(FrameProcessor):
    """Plays a sine tone after each user turn; stops it on interruption."""

    def __init__(
        self,
        *,
        sample_rate: int,
        freq: float = 440.0,
        seconds: float = 5.0,
        amplitude: float = 0.3,
    ):
        super().__init__()
        self._sample_rate = sample_rate
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
        chunk_samples = self._sample_rate // 50  # 20 ms
        total_chunks = int(self._seconds * 50)
        peak = int(32767 * self._amplitude)
        sample_index = 0
        for _ in range(total_chunks):
            samples = [
                int(
                    peak
                    * math.sin(2 * math.pi * self._freq * (sample_index + i) / self._sample_rate)
                )
                for i in range(chunk_samples)
            ]
            sample_index += chunk_samples
            pcm = struct.pack(f"<{chunk_samples}h", *samples)
            await self.push_frame(
                OutputAudioRawFrame(audio=pcm, sample_rate=self._sample_rate, num_channels=1)
            )
