import os
from concurrent.futures import ThreadPoolExecutor
from para_work.parquet_config import MAX_WORKERS, LOG_FILE, success, fails, warning_files
from para_work.parquet_batch import get_batches, select_batches
from para_work.parquet_display import update_display, print_summary
from para_work.parquet_processor import process_nc_file

def main():
    # Change to root directory (two levels up from scripts/)
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root_dir)
    
    # Get file batches
    nc_files, batches = get_batches()
    if nc_files is None:
        return

    # User selects batches
    selected_files = select_batches(batches, nc_files)
    if selected_files is None:
        return

    print(f"\nProcessing {len(selected_files)} files with {MAX_WORKERS} workers...")
    print("Starting processing...")

    # Prepare data for processing (file path + total count)
    file_data = [(nc_file, len(selected_files)) for nc_file in selected_files]

    # Process files concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_nc_file, file_data))

    # Final display and summary
    update_display(selected_files)
    
    with open(LOG_FILE, "a") as f:
        f.write("\n--- SUMMARY ---\n")
        f.write(f"✅ Success: {success}\n")
        f.write(f"⚠️ Warnings: {len(warning_files)}\n")
        f.write(f"❌ Failed: {fails}\n")

    print_summary()

if __name__ == "__main__":
    main()
