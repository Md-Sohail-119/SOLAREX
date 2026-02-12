from sunpy.net import Fido, attrs as a
import astropy.units as u

def get_hmi_and_flare_labels(st, et):
    query = Fido.search(
        a.Time(st, et),
        (a.Instrument.hmi & a.Physobs.los_magnetic_field & a.Sample(30 * u.s)) | 
        (a.hek.EventType("FL") & (a.hek.OBS.Observatory == "GOES"))
    )

    if len(query) < 2:
        print("Could not find both HMI data and Flare labels.")
        return
    
    hmi_results = query[0]
    flare_results = query[1]

    print("--- HMI Intervals (30s) ---")
    print(hmi_results["Start Time", "Instrument"])

    print("\n--- Flare Labels Found ---")
    if len(flare_results) > 0:
        print(flare_results["event_starttime", "fl_goescls"])
    else:
        print("No flares recorded.")

if __name__ == "__main__":
    start = input("Enter start (e.g., 2024-01-01 00:00:00): ")
    end = input("Enter end (e.g., 2024-01-01 01:00:00): ")
    get_hmi_and_flare_labels(start, end)