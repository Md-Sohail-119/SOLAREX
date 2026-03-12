import os
import time
import json
import logging
from datetime import datetime, timedelta
from sunpy.net import Fido, attrs as a
from astropy.time import Time
import numpy as np
from skimage.transform import resize
import sunpy.map


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

        # Fido.fetch returns a Parfive Results object; extract the first file path
        fetched = Fido.fetch(result[0, 0], path=output_dir, max_conn=1)
        return fetched[0] if fetched else None

    elif method == "drms":
        logging.error("DRMS method selected but not yet fully integrated.")
        return None


# --- Phase 3: Preprocessing ---
def process_and_compress(fits_file, processed_dir, target_shape=(224, 224)):
    """Loads FITS, downsamples for the model, saves as .npz, and deletes raw file."""
    os.makedirs(processed_dir, exist_ok=True)
    logging.info(f"Processing and downsampling: {fits_file}")

    try:
        hmi_map = sunpy.map.Map(fits_file)
        raw_data = hmi_map.data

        # Anti-aliasing preserves solar feature integrity during severe downsampling
        downsampled_data = resize(raw_data, target_shape, anti_aliasing=True)

        obs_time = hmi_map.date.isot.replace(":", "-")
        out_filename = os.path.join(processed_dir, f"hmi_downsampled_{obs_time}.npz")

        np.savez_compressed(out_filename, image=downsampled_data, time=obs_time)
        logging.info(f"Saved compressed array: {out_filename}")

        os.remove(fits_file)
        logging.info(f"Deleted original raw FITS: {fits_file}")

        return out_filename

    except Exception as e:
        logging.error(f"Error processing {fits_file}: {e}")
        return None


# --- Main Pipeline Execution ---
def main():
    config = load_config()
    setup_logging(config["pipeline"]["log_file"])
    logging.info("--- Starting Cadence-Driven Data Pipeline ---")

    raw_dir = config["pipeline"]["output_dir"]
    # Automatically create a processed directory parallel to the raw one
    processed_dir = os.path.join(os.path.dirname(raw_dir), "processed")
    cadence_mins = config["hmi"]["sampling_cadence_minutes"]

    start_dt = datetime.strptime(config["query"]["start_time"], "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(config["query"]["end_time"], "%Y-%m-%d %H:%M:%S")

    # 1. Fetch metadata
    flares = fetch_all_flare_events(
        config["query"]["start_time"], config["query"]["end_time"]
    )

    processed_arrays = []

    # 2. Continuous Cadence Downloading & Immediate Processing
    current_dt = start_dt

    while current_dt <= end_dt:
        target_time_astropy = Time(current_dt)
        logging.info(f"--- Sampling Step: {target_time_astropy.isot} ---")

        downloaded_file = download_hmi_for_target_time(
            target_time=target_time_astropy,
            output_dir=raw_dir,
            method=config["hmi"]["download_method"],
        )

        if downloaded_file:
            logging.info(f"Successfully acquired: {downloaded_file}")

            # 3. Inline downsampling and cleanup
            npz_file = process_and_compress(downloaded_file, processed_dir)
            if npz_file:
                processed_arrays.append(npz_file)
        else:
            logging.warning(
                f"Failed to acquire HMI data for {target_time_astropy.isot}"
            )

        current_dt += timedelta(minutes=cadence_mins)
        time.sleep(1)

    logging.info(
        f"--- Pipeline Execution Complete. Generated {len(processed_arrays)} compressed arrays. ---"
    )


if __name__ == "__main__":
    main()
