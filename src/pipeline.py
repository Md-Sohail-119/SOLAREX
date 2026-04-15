import os
import logging

from src.modules.fetcher import HMIFetcher
from src.modules.label_generator import HEKLabelGenerator
from src.modules.preprocessor import HMIPreprocessor
from src.utils.config_loader import load_config
from src.utils.logger import setup_logging
from src.utils.persistence import save_sequence_to_disk


# Main Pipeline Execution
def main():
    config = load_config()
    setup_logging(config["pipeline"]["log_file"])
    logging.info("--- Starting Modular Flare Sequence Pipeline ---")

    raw_dir = config["pipeline"]["output_dir"]
    processed_dir = config["pipeline"].get(
        "processed_output_dir",
        os.path.join(os.path.dirname(raw_dir), "processed"),
    )

    window_hours = config["pipeline"].get("window_hours", 48)
    target_shape = tuple(config["pipeline"].get("target_shape", [224, 224]))
    cadence_mins = config["hmi"]["sampling_cadence_minutes"]
    max_missing_fraction = config["hmi"].get("max_missing_fraction", 0.2)

    logging.info(
        "Resolved config: raw_dir=%s processed_dir=%s window_hours=%d target_shape=%s cadence_mins=%d max_missing_fraction=%.2f",
        raw_dir,
        processed_dir,
        window_hours,
        target_shape,
        cadence_mins,
        max_missing_fraction,
    )
    logging.info(
        "Query range: start_time=%s end_time=%s",
        config["query"]["start_time"],
        config["query"]["end_time"],
    )

    labeler = HEKLabelGenerator(window_hours=window_hours)
    fetcher = HMIFetcher(
        output_dir=raw_dir,
        cadence_mins=cadence_mins,
        max_missing_fraction=max_missing_fraction,
    )
    processor = HMIPreprocessor(target_shape=target_shape)

    sequences = labeler.get_target_events(
        config["query"]["start_time"], config["query"]["end_time"]
    )

    generated_files = []
    for sequence in sequences:
        try:
            logging.info(
                "Processing sequence event_id=%s peak_time=%s start_window=%s end_window=%s",
                sequence.event_id,
                sequence.peak_time,
                sequence.start_window,
                sequence.end_window,
            )
            sequence = fetcher.process(sequence)
            if not sequence.is_valid:
                logging.warning("Skipping invalid sequence after fetch: %s", sequence.event_id)
                continue

            sequence = processor.process(sequence)
            if not sequence.is_valid or sequence.processed_sequence is None:
                logging.warning("Skipping invalid sequence after preprocessing: %s", sequence.event_id)
                continue

            out_file = save_sequence_to_disk(sequence, processed_dir)
            generated_files.append(out_file)
            logging.info("Saved processed sequence for %s to %s", sequence.event_id, out_file)
        except Exception as exc:
            logging.exception(
                "Unexpected failure while processing sequence %s: %s",
                sequence.event_id,
                exc,
            )

    logging.info(
        "--- Pipeline Execution Complete. Generated %d sequence files. ---",
        len(generated_files),
    )


if __name__ == "__main__":
    main()
