import sunpy.map
import matplotlib.pyplot as plt
import sys
from pathlib import Path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please pass path to directory containing .fits files!")
        sys.exit(1)

    directory = Path(sys.argv[1])
    if not directory.is_dir():
        print("Provided path is not a directory!")
        sys.exit(1)

    fits_files = sorted(directory.glob("*.fits"))

    if not fits_files:
        print("No fits files found in this directory")
        sys.exit(1)

    maps: list = sunpy.map.Map(fits_files)
    print(f"Loaded {len(maps)} maps")

    for i, m in enumerate(maps):
        plt.figure()
        m.plot()
        plt.title(f"Map {i + 1}: {fits_files[i].name}")

    plt.show()
