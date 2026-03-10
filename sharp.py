import os
import glob
import socket
import matplotlib.pyplot as plt
from astropy.io import fits

from drms import Client

socket.setdefaulttimeout(120)

OUTPUT_DIR = "sharp_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

email = "kushalpatel36990@gmail.com"

client = Client(email=email)

print("Testing JSOC connection...")

test = client.query(
    "hmi.M_45s[2014.02.04_00:00:00_TAI]",
    key="T_REC"
)

print("Connection OK")
print(test)

print("\nSearching SHARP active regions...")

result = client.query(
    "hmi.sharp_cea_720s[][2014.02.04_00:00:00_TAI]",
    key="HARPNUM, T_REC"
)

print(result)

if result.empty:
    raise RuntimeError("No SHARP active regions found")

harpnum = int(result.iloc[0]["HARPNUM"])
t_rec = result.iloc[0]["T_REC"]

print("\nUsing HARPNUM:", harpnum)
print("Time:", t_rec)

query = f"hmi.sharp_cea_720s[{harpnum}][{t_rec}]"

print("\nExporting SHARP magnetogram...")

export = client.export(
    f"{query}{{magnetogram}}",
    method="url_quick",
    protocol="as-is"
)

export.download(OUTPUT_DIR)

print("\nDownload complete")

fits_files = glob.glob(os.path.join(OUTPUT_DIR, "*.fits"))

if not fits_files:
    raise RuntimeError("No FITS file downloaded")

fits_file = fits_files[0]

print("Opening:", fits_file)

hdul = fits.open(fits_file)

data = hdul[1].data

hdul.close()

print("Data shape:", data.shape)

plt.figure(figsize=(8,8))
plt.imshow(data, cmap="gray", origin="lower")
plt.colorbar(label="Magnetic Field (Gauss)")
plt.title(f"SHARP Magnetogram\nHARPNUM {harpnum}")
plt.xlabel("X Pixel")
plt.ylabel("Y Pixel")
plt.show()