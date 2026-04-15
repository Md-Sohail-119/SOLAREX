"""Persistence helpers for saving processed sequences."""

from __future__ import annotations

import os

import numpy as np

from src.core.datatypes import FlareSequence


def save_sequence_to_disk(sequence: FlareSequence, output_dir: str) -> str:
    """Persist a processed flare sequence as a compressed NPZ file."""
    os.makedirs(output_dir, exist_ok=True)

    safe_event_id = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in sequence.event_id
    )
    peak_time_slug = sequence.peak_time.replace(":", "-")
    out_filename = os.path.join(
        output_dir,
        f"sequence_{safe_event_id}_{peak_time_slug}.npz",
    )

    np.savez_compressed(
        out_filename,
        image=sequence.processed_sequence,
        event_id=sequence.event_id,
        peak_time=sequence.peak_time,
        flare_class=sequence.flare_class,
        start_window=sequence.start_window,
        end_window=sequence.end_window,
    )
    return out_filename
