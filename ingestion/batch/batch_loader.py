"""Download the current NGA World Port Index CSV."""

from __future__ import annotations

import argparse
import io
from utils.logger import Logger 
from datetime import date
from pathlib import Path

import certifi
import pandas as pd
import requests


PORTS_URL = (
    "https://msi.nga.mil/api/publications/download"
    "?key=16920959/SFH00000/UpdatedPub150.csv&type=view"
)
 
LOGGER = Logger()
LOGGER.info('Ports data extraction initialized')


def download_ports(output_directory: Path) -> Path:
    output_directory.mkdir(parents=True, exist_ok=True)

    today = date.today()
    first_day_of_month = today.replace(day=1)

    output_file = (
        output_directory
        / f"ports_{first_day_of_month.isoformat()}.csv"
    )

    temporary_file = output_file.with_suffix(".csv.tmp")

    LOGGER.info("Downloading World Port Index data")

    # Use requests with certifi for SSL certificate verification.
    response = requests.get(
        PORTS_URL,
        verify=certifi.where(),
        timeout=60,
    )
    response.raise_for_status()

    # NGA returns the CSV as application/octet-stream,
    # so read the downloaded bytes directly with pandas.
    ports = pd.read_csv(io.BytesIO(response.content))

    if ports.empty:
        raise RuntimeError("The ports source returned no rows")

    ports.to_csv(temporary_file, index=False)
    temporary_file.replace(output_file)

    LOGGER.info(
        "Saved %s ports to %s",
        len(ports),
        output_file,
    )

    return output_file


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path(__file__).resolve().parent / "data" / "ports",
    )

    arguments = parser.parse_args()

    download_ports(arguments.output_directory)


if __name__ == "__main__":
    main()