import os
from .parquet_config import counter_lock, MAX_WORKERS, success, fails, processed_files, warning_files

def update_display(selected_files):
    """Update tabular display"""
    with counter_lock:
        # Clear screen and show progress
        os.system('clear' if os.name == 'posix' else 'cls')
        print(f"Processing with {MAX_WORKERS} workers...")
        
        total_selected = len(selected_files) if selected_files else 0
        print(f"Progress: {success + fails}/{total_selected} | Success: {success} | Failed: {fails} | Warnings: {len(warning_files)}")
        print("-" * 80)
        
        # Show recent processed files (last 10)
        print("RECENT PROCESSED FILES:")
        for pf in processed_files[-10:]:
            print(f"✅ {pf}")
        
        print("\nFILES WITH MISSING VARIABLES:")
        for wf in warning_files[-10:]:
            print(f"⚠️ {wf}")

def print_summary():
    """Print final summary"""
    print("\n=== FINAL SUMMARY ===")
    print(f"✅ Success: {success}")
    print(f"⚠️ Warnings: {len(warning_files)}")
    print(f"❌ Failed: {fails}")
