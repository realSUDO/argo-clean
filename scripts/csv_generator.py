import pandas as pd
from pathlib import Path

# -------------------------------
# CONFIG
# -------------------------------
INPUT_CSV = "global_csvs/argo_index_india.csv"
OUTPUT_DIR = Path("yearly_csvs")
OUTPUT_DIR.mkdir(exist_ok=True)
YEARS = list(range(2025, 2014, -1))  # Descending order: 2025 → 2015

# -------------------------------
# Terminal multi-select menu using curses with fallback
# -------------------------------
def select_years_menu():
    try:
        import curses

        def _curses_menu(stdscr):
            curses.curs_set(0)
            selected = [False] * len(YEARS)
            pointer = 0

            while True:
                stdscr.clear()
                stdscr.addstr("Select year(s) to generate CSV (SPACE to select, ENTER to confirm)\n\n")

                for i, year in enumerate(YEARS):
                    sel_mark = "[*]" if selected[i] else "[ ]"
                    pointer_mark = "-> " if i == pointer else "   "
                    stdscr.addstr(f"{pointer_mark}{sel_mark} {year}\n")

                key = stdscr.getch()
                if key == curses.KEY_UP and pointer > 0:
                    pointer -= 1
                elif key == curses.KEY_DOWN and pointer < len(YEARS) - 1:
                    pointer += 1
                elif key == ord(" "):
                    selected[pointer] = not selected[pointer]
                elif key in [10, 13]:  # Enter key
                    break

            chosen_years = [year for sel, year in zip(selected, YEARS) if sel]
            return chosen_years

        return curses.wrapper(_curses_menu)

    except Exception:
        # Fallback for small terminals or curses failure
        print("⚠️ Terminal too small or curses unavailable. Using simple input fallback.")
        print("Select years by typing numbers separated by spaces, e.g.: 2020 2021 2023\n")
        for i, year in enumerate(YEARS, start=1):
            print(f"{i}. {year}")

        while True:
            try:
                choices = input("\nEnter your choices: ").strip().split()
                selected_years = [YEARS[int(c)-1] for c in choices if c.isdigit() and 1 <= int(c) <= len(YEARS)]
                if selected_years:
                    return selected_years
                else:
                    print("❌ No valid selections. Try again.")
            except Exception:
                print("❌ Invalid input. Try again.")

# -------------------------------
# Generate ONE CSV for all selected years
# -------------------------------
def generate_csv(selected_years):
    try:
        df = pd.read_csv(INPUT_CSV)
        df["date_update"] = pd.to_datetime(df["date_update"], format="%Y%m%d%H%M%S", errors="coerce")
        df["year"] = df["date_update"].dt.year
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return

    if not selected_years:
        print("⚠️ No year selected. Exiting.")
        return

    combined_df = df[df["year"].isin(selected_years)].copy()
    if combined_df.empty:
        print("⚠️ No data for the selected years.")
        return

    output_file = OUTPUT_DIR / f"argo_index_{min(selected_years)}_{max(selected_years)}.csv"
    combined_df.drop(columns=["year"], inplace=True)
    combined_df.to_csv(output_file, index=False)
    print(f"✅ {output_file.name} generated successfully ({len(combined_df)} rows)")

# -------------------------------
# Main
# -------------------------------
def main():
    selected_years = select_years_menu()
    generate_csv(selected_years)

if __name__ == "__main__":
    main()

