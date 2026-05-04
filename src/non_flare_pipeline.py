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
from requests.exceptions import ChunkedEncodingError
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

# --- Phase 1: Flare Handling ---
def get_flare_ranges(flares, buffer_minutes=10):
    flare_ranges = []

    if flares is None:
        return flare_ranges

    for flare in flares:
        start = flare['event_starttime'].to_datetime()
        end = flare['event_endtime'].to_datetime()

        start -= timedelta(minutes=buffer_minutes)
        end += timedelta(minutes=buffer_minutes)

        flare_ranges.append((start, end))

    return flare_ranges


def is_non_flare(current_time, flare_ranges):
    for start, end in flare_ranges:
        if start <= current_time <= end:
            return False
    return True


# --- Phase 2: Setup ---
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


# --- Phase 3: Fetch flare metadata ---
def fetch_all_flare_events(start_time, end_time):
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


# --- Phase 4: HMI Download (FIXED) ---
def download_hmi_for_target_time(target_time, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    search_start = target_time
    search_end = target_time + timedelta(minutes=2)

    for attempt in range(3):
        try:
            result = Fido.search(
                a.Time(search_start, search_end),          # FIXED
                a.Instrument("HMI"),                       # FIXED
                a.Physobs("LOS_magnetic_field"),           # FIXED
            )

            if len(result) == 0 or len(result[0]) == 0:
                logging.warning("No HMI data found for this time.")
                return None

            fetched = Fido.fetch(result[0, 0], path=output_dir, max_conn=1)
            return fetched[0] if fetched else None

        except ChunkedEncodingError:
            logging.warning(f"Network error, retrying ({attempt+1}/3)...")
            time.sleep(3)

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return None

    logging.error("Failed after 3 retries.")
    return None


# --- Phase 5: Processing ---
def process_and_compress(fits_file, processed_dir, target_shape=(224, 224)):
    os.makedirs(processed_dir, exist_ok=True)
    logging.info(f"Processing and downsampling: {fits_file}")

    try:
        hmi_map = sunpy.map.Map(fits_file)
        raw_data = hmi_map.data

        downsampled_data = resize(raw_data, target_shape, anti_aliasing=True)

        obs_time = hmi_map.date.isot.replace(":", "-")
        out_filename = os.path.join(processed_dir, f"hmi_{obs_time}.npz")

        np.savez_compressed(out_filename, image=downsampled_data, time=obs_time)
        logging.info(f"Saved compressed array: {out_filename}")

        os.remove(fits_file)
        logging.info(f"Deleted original FITS: {fits_file}")

        return out_filename

    except Exception as e:
        logging.error(f"Error processing {fits_file}: {e}")
        return None


# --- Main Pipeline ---
def main():
    config = load_config()
    setup_logging(config["pipeline"]["log_file"])

    logging.info("--- Starting Cadence-Driven Data Pipeline ---")

    raw_dir = config["pipeline"]["output_dir"]
    processed_dir = os.path.join(os.path.dirname(raw_dir), "processed")
    cadence_mins = config["hmi"]["sampling_cadence_minutes"]

    start_dt = datetime.strptime(config["query"]["start_time"], "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(config["query"]["end_time"], "%Y-%m-%d %H:%M:%S")

    # Fetch flares
    flares = fetch_all_flare_events(
        config["query"]["start_time"], config["query"]["end_time"]
    )

    flare_ranges = get_flare_ranges(flares)

    processed_arrays = []
    current_dt = start_dt

    while current_dt <= end_dt:

        if not is_non_flare(current_dt, flare_ranges):
            current_dt += timedelta(minutes=cadence_mins)
            continue

        target_time_astropy = Time(current_dt)
        logging.info(f"--- Sampling Step: {target_time_astropy.isot} ---")

        downloaded_file = download_hmi_for_target_time(
            target_time_astropy,
            raw_dir,
        )

        if downloaded_file:
            logging.info(f"Downloaded: {downloaded_file}")

            npz_file = process_and_compress(downloaded_file, processed_dir)
            if npz_file:
                processed_arrays.append(npz_file)

                # --- Visualization (only for debugging, not for full runs) ---

                data = np.load(npz_file)
                img = data["image"]
                time = data["time"]

                plt.imshow(img, cmap="gray")
                plt.title(f"Non-Flare HMI Image\n{time}")
                plt.colorbar()
                plt.show()
        else:
            logging.warning(
                f"Failed to acquire HMI data for {target_time_astropy.isot}"
            )

        current_dt += timedelta(minutes=cadence_mins)
        time.sleep(1)

    logging.info(
        f"--- Pipeline Complete. Generated {len(processed_arrays)} compressed arrays. ---"
    )


if __name__ == "__main__":
    main()