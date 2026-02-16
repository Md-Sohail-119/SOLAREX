import os
import json
import logging
from datetime import datetime, timedelta
from sunpy.net import Fido, attrs as a
from astropy.time import Time


# --- Phase 1: Orchestration ---
def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def load_config(config_path="config.json"):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} not found.")
    with open(config_path, "r") as f:
        return json.load(f)


# --- Phase 2: Acquisition ---
def fetch_all_flare_events(start_time, end_time):
    """Queries HEK for ALL GOES flare events (A, B, C, M, X) within the time window."""
    logging.info(
        f"Querying HEK for all flare activity between {start_time} and {end_time}..."
    )

    query = Fido.search(
        a.Time(start_time, end_time),
        a.hek.EventType("FL"),
        a.hek.OBS.Observatory == "GOES",
        # Notice we removed the a.hek.FL.GOESCls filter here to get everything!
    )

    if len(query) == 0:
        logging.warning("No flares found in the specified time window.")
        return None

    hek_results = query[0]
    logging.info(f"Found {len(hek_results)} total flare events.")
    return hek_results


def download_hmi_for_target_time(target_time, output_dir, method="fido"):
    """Downloads a single HMI magnetogram closest to the target_time."""
    os.makedirs(output_dir, exist_ok=True)

    search_start = target_time
    search_end = target_time + timedelta(minutes=2)

    if method == "fido":
        result = Fido.search(
            a.Time(search_start.isot, search_end.isot),
            a.Instrument.hmi,
            a.Physobs.los_magnetic_field,
        )

        if len(result) == 0 or len(result[0]) == 0:
            return None

        return Fido.fetch(result[0, 0], path=output_dir, max_conn=1)

    elif method == "drms":
        logging.error("DRMS method selected but not yet fully integrated.")
        return None


# --- Main Pipeline Execution ---
def main():
    config = load_config()
    setup_logging(config["pipeline"]["log_file"])
    logging.info("--- Starting Cadence-Driven Data Pipeline ---")

    output_dir = config["pipeline"]["output_dir"]
    cadence_mins = config["hmi"]["sampling_cadence_minutes"]

    # Parse times into Python datetime objects for easy math
    start_dt = datetime.strptime(config["query"]["start_time"], "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(config["query"]["end_time"], "%Y-%m-%d %H:%M:%S")

    # 1. Fetch all metadata (We will use this list later in Phase 3 to label the images)
    flares = fetch_all_flare_events(
        config["query"]["start_time"], config["query"]["end_time"]
    )

    # 2. Continuous Cadence Downloading
    current_dt = start_dt

    while current_dt <= end_dt:
        target_time_astropy = Time(current_dt)
        logging.info(f"--- Sampling Step: {target_time_astropy.isot} ---")

        downloaded_files = download_hmi_for_target_time(
            target_time=target_time_astropy,
            output_dir=output_dir,
            method=config["hmi"]["download_method"],
        )

        if downloaded_files:
            logging.info(f"Successfully acquired: {downloaded_files[0]}")
        else:
            logging.warning(
                f"Failed to acquire HMI data for {target_time_astropy.isot}"
            )

        # Step forward in time by the specified cadence
        current_dt += timedelta(minutes=cadence_mins)

    logging.info("--- Pipeline Execution Complete ---")


if __name__ == "__main__":
    main()
