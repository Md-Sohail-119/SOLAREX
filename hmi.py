import os
import glob
import matplotlib.pyplot as plt
from datetime import datetime

#option1 no mail- use fido
def download_using_fido():
    from sunpy.net import Fido, attrs as a
    import sunpy.map
    
    OUTPUT_DIR = "hmi_data"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result = Fido.search(
        a.Time('2024/01/01 00:00:00', '2024/01/01 00:01:00'), #can change just change the no.s
        a.Instrument.hmi,
        a.Physobs.los_magnetic_field  
    )
    
    print("Search results:")
    print(result)
    if len(result) == 0:
        raise RuntimeError("No HMI data found for the specified time range.")

    print("Downloading...")
    downloaded_files = Fido.fetch(result[0, 0], path=OUTPUT_DIR, max_conn=1)
    
    print(f"Downloaded: {downloaded_files}")
    return downloaded_files


#option2 use drms - advance d data from stanford ki site  but need ot regi mail 
def download_using_drms():
    from drms import Client
    import sunpy.map
    
    OUTPUT_DIR = "hmi_data"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Get email from environment variable
    email = os.environ.get("JSOC_EMAIL")
    if not email:
        raise RuntimeError(
            "email not set."
            "Resi a at: http://jsoc.stanford.edu/ajax/register_email.html"
        )
    
    T_REC = "2024.01.01_00:00:00_TAI"
    HMI_SERIES = "hmi.M_45s"
    client = Client(email=email)
    print("Querying JSOC...")
    query_string = f"{HMI_SERIES}[{T_REC}]"
    result = client.query(query_string, key="T_REC")
    if len(result) == 0:
        raise RuntimeError("No HMI data found. Check T_REC timestamp.")
    print(f"Found {len(result)} records. Downloading FITS...")
    export = client.export(
        f"{HMI_SERIES}[{T_REC}]{{magnetogram}}",
        method="url_quick",
        protocol="as-is"
    )
    export.download(OUTPUT_DIR)
    fits_files = glob.glob(os.path.join(OUTPUT_DIR, "*.fits"))
    return fits_files

def visualize_magnetogram(fits_file):
    import sunpy.map
    
    print(f"Loading: {fits_file}")
    hmi_map = sunpy.map.Map(fits_file)
    
    plt.figure(figsize=(10, 10))
    hmi_map.plot()
    plt.colorbar(label="Magnetic Field (Gauss)")
    plt.title(f"SDO/HMI Magnetogram - {hmi_map.date}")
    plt.tight_layout()
    plt.savefig("hmi_magnetogram.png", dpi=150)
    print("Saved to hmi_magnetogram.png")
    plt.show()


if __name__ == "__main__":
    print("=" * 60)
    print("SDO/HMI Magnetogram Data Acquisition")
    print("=" * 60)

    if os.environ.get("JSOC_EMAIL"):
        print("Using DRMS method (email found)...")
        downloaded = download_using_drms()
    else:
        print("Using SunPy Fido method (no email required)...")
        downloaded = download_using_fido()
    
    print("Download complete!")

    fits_files = glob.glob("hmi_data/*.fits")
    if fits_files:
        fits_file = fits_files[0]
        print(f"Found FITS file: {fits_file}")
        visualize_magnetogram(fits_file)
    else:
        print("No FITS files found to visualize.")