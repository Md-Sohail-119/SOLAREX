import os
import json
import logging
import glob
from datetime import timedelta
from sunpy.net import Fido, attrs as a
import astropy.units as u
from astropy.time import Time

# --- Phase 1: Orchestration ---


def setup_logging(log_file):
    """Configures logging to both the console and a file."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def load_config(config_path="config.json"):
    """Loads pipeline parameters from a JSON file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} not found.")
    with open(config_path, "r") as f:
        return json.load(f)


# --- Phase 2: Acquisition ---


def fetch_flare_events(start_time, end_time, min_class):
    """Queries HEK for GOES flare events within the time window."""
    logging.info(f"Querying HEK for flares between {start_time} and {end_time}...")

    query = Fido.search(
        a.Time(start_time, end_time),
        a.hek.EventType("FL"),
        a.hek.OBS.Observatory == "GOES",
        a.hek.FL.GOESCls >= min_class,
    )

    # Fido returns a unified response; the HEK results are usually at index 0 for this specific query
    hek_results = query[0]

    if len(query) == 0:
        logging.warning("No flares found in the specified time window.")
        return None

    logging.info(f"Found {len(hek_results)} flare events.")
    return hek_results


def download_hmi_for_target_time(target_time, output_dir, method="fido"):
    """
    Downloads a single HMI magnetogram closest to the target_time.
    Defaults to Fido, but leaves room for the DRMS method.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Create a small 2-minute search window around the target time to ensure we catch a frame
    search_start = target_time
    search_end = target_time + timedelta(minutes=2)

    logging.info(
        f"Searching for HMI data between {search_start.isot} and {search_end.isot}..."
    )

    if method == "fido":
        result = Fido.search(
            a.Time(search_start.isot, search_end.isot),
            a.Instrument.hmi,
            a.Physobs.los_magnetic_field,
        )

        if len(result) == 0 or len(result[0]) == 0:
            logging.error(f"No HMI data found for target time: {target_time.isot}")
            return None

        logging.info(f"HMI data found. Downloading to {output_dir}...")
        # Download just the first matching file to save bandwidth during pipeline testing
        downloaded_files = Fido.fetch(result[0, 0], path=output_dir, max_conn=1)
        return downloaded_files

    elif method == "drms":
        # Placeholder for your DRMS logic if you prefer JSOC email authentication later
        logging.error(
            "DRMS method selected but not yet fully integrated in this block."
        )
        return None


# --- Main Pipeline Execution ---


def main():
    # 1. Load config and setup logging
    config = load_config()
    setup_logging(config["pipeline"]["log_file"])
    logging.info("--- Starting Data Preparation Pipeline ---")

    output_dir = config["pipeline"]["output_dir"]
    offset_minutes = config["hmi"]["offset_minutes_before_flare"]

    # 2. Fetch metadata (Flare Labels)
    flares = fetch_flare_events(
        config["query"]["start_time"],
        config["query"]["end_time"],
        config["query"]["min_goes_class"],
    )

    if flares is None:
        logging.info("Pipeline finished: No flares to process.")
        return

    # 3. Targeted Downloading
    # For demonstration, we'll just process the FIRST flare found.
    # In a full run, you would iterate over `flares`.

    # 3. Targeted Downloading (Looping through all found flares)
    for index, flare in enumerate(flares):
        flare_start_str = flare["event_starttime"]
        flare_start_time = Time(flare_start_str, format="isot").datetime
        flare_class = flare["fl_goescls"]

        logging.info(f"--- Processing Flare {index + 1}/{len(flares)} ---")
        logging.info(
            f"Targeting Flare: Class {flare_class} starting at {flare_start_str}"
        )

        target_obs_time = flare_start_time - timedelta(minutes=offset_minutes)
        target_obs_time_astropy = Time(target_obs_time)

        logging.info(f"Calculated HMI target time: {target_obs_time_astropy.isot}")

        downloaded_files = download_hmi_for_target_time(
            target_time=target_obs_time_astropy,
            output_dir=output_dir,
            method=config["hmi"]["download_method"],
        )

        if downloaded_files:
            logging.info(f"Successfully acquired: {downloaded_files}")
        else:
            logging.error(
                f"Failed to acquire paired HMI data for flare at {flare_start_str}"
            )


if __name__ == "__main__":
    main()
