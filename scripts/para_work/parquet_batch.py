from glob import glob
import os
from .parquet_config import NC_DIR

def get_batches():
    """Get files and divide into 4 batches"""
    nc_files = sorted(glob(os.path.join(NC_DIR, "*.nc")), key=lambda x: os.path.basename(x))
    total_files = len(nc_files)

    if total_files == 0:
        print(f"❌ No .nc files found in {NC_DIR}")
        return None, None

    # Divide into 4 batches
    batch_size = total_files // 4
    batches = {
        1: nc_files[:batch_size],
        2: nc_files[batch_size:2*batch_size],
        3: nc_files[2*batch_size:3*batch_size],
        4: nc_files[3*batch_size:]
    }

    print(f"Found {total_files} .nc files in {NC_DIR}")
    print(f"Batch 1: {len(batches[1])} files")
    print(f"Batch 2: {len(batches[2])} files") 
    print(f"Batch 3: {len(batches[3])} files")
    print(f"Batch 4: {len(batches[4])} files")

    return nc_files, batches

def select_batches(batches, nc_files):
    """Handle user batch selection"""
    print("\nChoose batch(es) to process:")
    print("1 - Batch 1 only")
    print("2 - Batch 2 only") 
    print("3 - Batch 3 only")
    print("4 - Batch 4 only")
    print("all - All batches")
    print("1,2 - Multiple batches (comma separated)")

    choice = input("Enter your choice: ").strip().lower()

    if choice == "all":
        return nc_files
    elif "," in choice:
        selected_files = []
        for batch_num in choice.split(","):
            try:
                selected_files.extend(batches[int(batch_num.strip())])
            except:
                print(f"Invalid batch: {batch_num}")
        return selected_files
    else:
        try:
            return batches[int(choice)]
        except:
            print("Invalid choice!")
            return None
