"""Flare event labeling and sequence window generation."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import List

from astropy.time import Time
from sunpy.net import Fido, attrs as a

from src.core.datatypes import FlareSequence


class HEKLabelGenerator:
    """Query HEK flare events and convert them into sequence windows."""

    def __init__(self, window_hours: int) -> None:
        self.window_hours = window_hours

    def get_target_events(self, start_time: str, end_time: str) -> List[FlareSequence]:
        """Return flare sequences with lookback windows ending at flare peak time."""
        logging.info(
            "Querying HEK for flare activity between %s and %s",
            start_time,
            end_time,
        )

        query = Fido.search(
            a.Time(start_time, end_time),
            a.hek.EventType("FL"),
            a.hek.OBS.Observatory == "GOES",
        )

        if len(query) == 0:
            logging.warning("No flares found in the specified time window.")
            return []

        hek_results = query[0]
        sequences: List[FlareSequence] = []

        logging.info("HEK returned %d candidate flare rows.", len(hek_results))

        for index, row in enumerate(hek_results, start=1):
            try:
                peak = self._extract_peak_time(row)
                if peak is None:
                    continue

                start_window = peak - timedelta(hours=self.window_hours)
                event_id = self._extract_event_id(row, peak)
                flare_class = self._extract_flare_class(row)

                logging.info(
                    "Flare %d: event_id=%s flare_class=%s peak_time=%s start_window=%s end_window=%s window_hours=%d",
                    index,
                    event_id,
                    flare_class,
                    peak.isot,
                    start_window.isot,
                    peak.isot,
                    self.window_hours,
                )

                sequences.append(
                    FlareSequence(
                        event_id=event_id,
                        peak_time=peak.isot,
                        flare_class=flare_class,
                        start_window=start_window.isot,
                        end_window=peak.isot,
                    )
                )
            except Exception as exc:
                logging.exception("Failed to build FlareSequence from HEK row: %s", exc)

        logging.info("Generated %d flare sequences.", len(sequences))
        return sequences

    @staticmethod
    def _extract_peak_time(row) -> Time | None:
        """Read peak timestamp from common HEK fields."""
        for field in ("event_peaktime", "event_starttime"):
            if field in row.colnames and row[field]:
                peak = Time(row[field])
                logging.info("Using HEK field %s=%s as flare time.", field, peak.isot)
                return peak

        logging.warning("Skipping flare row without peak/start time field.")
        return None

    @staticmethod
    def _extract_event_id(row, peak: Time) -> str:
        """Resolve a stable event identifier from HEK row fields."""
        for field in ("hek_id", "kb_archivid", "frm_name"):
            if field in row.colnames and row[field]:
                return str(row[field])
        return f"flare_{peak.isot}"

    @staticmethod
    def _extract_flare_class(row) -> str:
        """Resolve flare class from HEK row fields if present."""
        if "fl_goescls" in row.colnames and row["fl_goescls"]:
            return str(row["fl_goescls"])
        return "UNKNOWN"
