import os
import argparse
import numpy as np
import matplotlib.pyplot as plt

def process_and_plot(directory):
    # Verify the directory exists
    if not os.path.isdir(directory):
        print(f"Error: The directory '{directory}' does not exist.")
        return

    # Filter for .npz files
    npz_files = [f for f in os.listdir(directory) if f.endswith('.npz')]
    
    if not npz_files:
        print(f"No .npz files found in '{directory}'.")
        return

    print(f"Found {len(npz_files)} .npz file(s). Processing...")

    for filename in npz_files:
        filepath = os.path.join(directory, filename)
        
        try:
            # Load the .npz archive
            with np.load(filepath) as data:
                # Iterate through all the arrays stored in the archive
                for key in data.files:
                    array = data[key]
                    
                    # Remove single-dimensional entries from the shape (e.g., (1, 256, 256) -> (256, 256))
                    array = np.squeeze(array)

                    # We can only plot 2D arrays directly as grayscale images
                    if array.ndim == 2:
                        plt.figure(figsize=(6, 6))
                        plt.imshow(array, cmap='gray')
                        plt.title(f"File: {filename}\nArray Key: '{key}'")
                        plt.colorbar(label='Pixel Intensity')
                        plt.tight_layout()
                        plt.show()
                    else:
                        print(f"Skipping key '{key}' in {filename}: Expected a 2D array for an image, but got shape {array.shape}.")
                        
        except Exception as e:
            print(f"Failed to process {filename}: {e}")

if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="Parse .npz files in a directory and plot 2D arrays as grayscale images.")
    parser.add_argument("directory", type=str, help="Path to the directory containing the .npz files")
    
    args = parser.parse_args()
    process_and_plot(args.directory)