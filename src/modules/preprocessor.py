"""FITS preprocessing for machine learning sequence generation."""

from __future__ import annotations

import logging
import os
from typing import List, Tuple

import numpy as np
import sunpy.map
from skimage.transform import resize

from src.core.datatypes import FlareSequence
from src.core.interfaces import PipelineStep


class HMIPreprocessor(PipelineStep):
    """Convert raw FITS files to stacked, resized NumPy sequence arrays."""

    def __init__(self, target_shape: Tuple[int, int]) -> None:
        self.target_shape = target_shape

    def process(self, sequence: FlareSequence) -> FlareSequence:
        """Populate sequence.processed_sequence and remove raw FITS files."""
        processed_frames: List[np.ndarray] = []

        logging.info(
            "Preprocessing %d FITS files for event_id=%s into shape=%s",
            len(sequence.raw_fits_paths),
            sequence.event_id,
            self.target_shape,
        )

        for index, fits_file in enumerate(sequence.raw_fits_paths, start=1):
            try:
                logging.info("Preprocessing file %d/%d: %s", index, len(sequence.raw_fits_paths), fits_file)
                hmi_map = sunpy.map.Map(fits_file)
                raw_data = hmi_map.data

                # Replace NaN and inf values to prevent contamination during resize.
                clean_data = np.nan_to_num(raw_data, nan=0.0, posinf=0.0, neginf=0.0)

                downsampled_data = resize(
                    clean_data,
                    self.target_shape,
                    anti_aliasing=True,
                )

                processed_frames.append(downsampled_data.astype(np.float32, copy=False))
            except Exception as exc:
                logging.exception("Failed to preprocess FITS file %s: %s", fits_file, exc)
            finally:
                if os.path.exists(fits_file):
                    try:
                        os.remove(fits_file)
                        logging.info("Deleted raw FITS file: %s", fits_file)
                    except OSError as exc:
                        logging.warning("Could not delete FITS file %s: %s", fits_file, exc)

        if not processed_frames:
            sequence.is_valid = False
            sequence.processed_sequence = None
            logging.warning(
                "Sequence %s invalid: no frames successfully processed.",
                sequence.event_id,
            )
            return sequence

        sequence.processed_sequence = np.stack(processed_frames, axis=0)
        logging.info(
            "Preprocessing complete for %s: processed_frames=%d output_shape=%s",
            sequence.event_id,
            len(processed_frames),
            sequence.processed_sequence.shape,
        )
        return sequence
