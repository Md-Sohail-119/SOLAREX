"""HMI data acquisition for each flare sequence window."""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import List

from astropy.time import Time
from sunpy.net import Fido, attrs as a

from src.core.datatypes import FlareSequence
from src.core.interfaces import PipelineStep


class HMIFetcher(PipelineStep):
    """Download cadence-aligned HMI LOS magnetograms for a sequence window."""

    def __init__(
        self,
        output_dir: str,
        cadence_mins: int,
        max_missing_fraction: float = 0.2,
    ) -> None:
        self.output_dir = output_dir
        self.cadence_mins = cadence_mins
        self.max_missing_fraction = max_missing_fraction
        os.makedirs(self.output_dir, exist_ok=True)

    def process(self, sequence: FlareSequence) -> FlareSequence:
        """Populate sequence.raw_fits_paths with downloaded FITS files."""
        try:
            target_times = self._generate_target_times(
                sequence.start_window,
                sequence.end_window,
                self.cadence_mins,
            )
        except Exception as exc:
            logging.exception("Invalid sequence window for %s: %s", sequence.event_id, exc)
            sequence.is_valid = False
            return sequence

        logging.info(
            "Fetching HMI for event_id=%s start_window=%s end_window=%s cadence_mins=%d target_count=%d",
            sequence.event_id,
            sequence.start_window,
            sequence.end_window,
            self.cadence_mins,
            len(target_times),
        )

        downloaded_files: List[str] = []
        missing_frames = 0

        for index, target_time in enumerate(target_times, start=1):
            fits_path = self._download_hmi_for_target_time(target_time)
            if fits_path:
                downloaded_files.append(fits_path)
                logging.info(
                    "Target %d/%d at %s -> %s",
                    index,
                    len(target_times),
                    target_time.isot,
                    fits_path,
                )
            else:
                missing_frames += 1
                logging.warning(
                    "Target %d/%d at %s -> no FITS found",
                    index,
                    len(target_times),
                    target_time.isot,
                )

        expected = len(target_times)
        missing_fraction = (missing_frames / expected) if expected else 1.0

        sequence.raw_fits_paths = downloaded_files
        sequence.is_valid = expected > 0 and missing_fraction <= self.max_missing_fraction

        logging.info(
            "Fetch summary for %s: downloaded=%d missing=%d expected=%d missing_fraction=%.2f%% valid=%s",
            sequence.event_id,
            len(downloaded_files),
            missing_frames,
            expected,
            missing_fraction * 100,
            sequence.is_valid,
        )

        if not sequence.is_valid:
            logging.warning(
                "Sequence %s invalid due to missing frames: %d/%d (%.2f%%)",
                sequence.event_id,
                missing_frames,
                expected,
                missing_fraction * 100,
            )

        return sequence

    @staticmethod
    def _generate_target_times(
        start_window: str,
        end_window: str,
        cadence_mins: int,
    ) -> List[Time]:
        """Create cadence-aligned Time targets across the sequence window."""
        start = Time(start_window)
        end = Time(end_window)

        targets: List[Time] = []
        current = start.to_datetime()
        end_dt = end.to_datetime()

        while current <= end_dt:
            targets.append(Time(current))
            current += timedelta(minutes=cadence_mins)

        return targets

    def _download_hmi_for_target_time(self, target_time: Time) -> str | None:
        """Download the first matching HMI frame near a target time."""
        search_start = target_time
        search_end = target_time + timedelta(minutes=2)

        try:
            logging.info(
                "Searching HMI for %s to %s in %s",
                search_start.isot,
                search_end.isot,
                self.output_dir,
            )
            result = Fido.search(
                a.Time(search_start.isot, search_end.isot),
                a.Instrument.hmi,
                a.Physobs.los_magnetic_field,
            )

            if len(result) == 0 or len(result[0]) == 0:
                logging.warning("No HMI result found near %s", target_time.isot)
                return None

            fetched = Fido.fetch(result[0, 0], path=self.output_dir, max_conn=1)
            logging.info("Fido.fetch near %s returned: %s", target_time.isot, fetched)
            return fetched[0] if fetched else None
        except Exception as exc:
            logging.exception(
                "Failed to fetch HMI frame near %s: %s",
                target_time.isot,
                exc,
            )
            return None
