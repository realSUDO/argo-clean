# Argo-Clean 🌊📊

A streamlined pipeline for processing Argo oceanographic data from NetCDF files to SQL databases with beautiful visualizations.

![Workflow](https://img.shields.io/badge/Workflow-Data%20Processing-blueviolet)
![Python](https://img.shields.io/badge/Python-3.8%2B-success)
![License](https://img.shields.io/badge/License-MIT-green)

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/realSUDO/argo-clean && cd argo-clean

# Install dependencies
pip install -r requirements.txt
```

---

## 📁 Project Structure

```bash
argo-clean/
├── 📂 argo_nc_files_requests/       # Raw NetCDF4 files
├── 📂 argo_parquet/                 # Processed CSV & Parquet files
├── 📂 global_csvs/                  # Master CSV indices
│   ├── 📄 argo_index_india.csv         # All-time India Argo index
│   └── 📄 argo_index_last3yrs.csv      # Last 3 years India Argo index
├── 📂 scripts/                      # Python processing scripts
│   ├── 🐍 csv_generator.py             # Generate yearly CSVs
│   ├── 🐍 nc_downloader_all.py         # Download all .nc files
│   ├── 🐍 nc_downloader_ui.py          # Download selected date-range .nc files
│   ├── 🐍 nc_to_parquet.py             # Convert .nc → CSV + Parquet
│   └── 🐍 parquet_to_sql_db.py         # Convert Parquet → SQL database
├── 📂 SQL-DB/                       # Generated SQL database
└── 📂 yearly_csvs/                  # Generated yearly CSVs
```

---

## ⚡ Quick Commands Cheat Sheet

| Task | Command |
|------|---------|
| Download selected NC files | `python scripts/nc_downloader_ui.py` |
| Download all NC files | `python scripts/nc_downloader_all.py` |
| Generate yearly CSVs | `python scripts/csv_generator.py` |
| Convert to Parquet | `python scripts/nc_to_parquet.py` |
| Create SQL Database | `python scripts/parquet_to_sql_db.py` |

---

## 📊 Data Processing Pipeline

### 1. 📥 Download NetCDF Files

**Option A: Interactive Download (Recommended)**
```bash
python scripts/nc_downloader_ui.py
```
*Select specific date ranges interactively*

**Option B: Bulk Download**
```bash
python scripts/nc_downloader_all.py
```
*Downloads all files from the index*

> 💡 **Pro Tip**: Modify `CSV_FILE` path in the scripts to use custom CSV indices

### 2. 📋 Generate Custom CSV Files
```bash
python scripts/csv_generator.py
```
*Creates yearly aggregated CSV files for easier filtering*

### 3. 🔄 Convert to Parquet Format
```bash
python scripts/nc_to_parquet.py
```
*Transforms NetCDF → CSV + Parquet (optimized for analysis)*

### 4. 🗃️ Create SQL Database
```bash
python scripts/parquet_to_sql_db.py
```
*Builds query-optimized SQL database from Parquet files*

---

## 🌟 Features

- **Interactive UI** for selective data download
- **Batch processing** for large datasets
- **Multiple format support** (NetCDF, CSV, Parquet, SQL)
- **Optimized storage** with Parquet compression
- **SQL-ready** for immediate analysis
- **Customizable** workflow for different regions/time periods

---

## 🔧 Customization

### Using Different CSV Indices
Edit the `CSV_FILE` variable in the download scripts:
```python
# Change this path to use different CSV files
CSV_FILE = "global_csvs/your_custom_index.csv"
```

### Adding New Regions
1. Create new CSV index in `global_csvs/`
2. Update script paths to point to your new CSV
3. Run the pipeline with your custom data

---

## 📈 Output Structure

```
Raw Data → argo_nc_files_requests/
    ↓
Processed Data → argo_parquet/ (.csv + .parquet)
    ↓
Final Database → SQL-DB/argo_data.db
```

---

## 🎯 Use Cases

- **Oceanographic Research** - Analyze temperature, salinity patterns
- **Climate Studies** - Long-term trend analysis
- **Machine Learning** - Train models on ocean data
- **Visualization** - Create interactive maps and charts
- **Educational Purposes** - Learn about data processing pipelines

---

## 🤝 Contributing

We welcome contributions! Feel free to:
- Report bugs and issues
- Suggest new features
- Submit pull requests
- Improve documentation

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🚀 Next Steps

After setting up your database:
```python
# Connect to your SQL database
import sqlite3
conn = sqlite3.connect('SQL-DB/argo_data.db')

# Start querying!
results = conn.execute("SELECT * FROM argo_data LIMIT 10")
for row in results:
    print(row)
```

---

## 💡 Pro Tips

1. **Start small** - Use the interactive downloader for specific date ranges first
2. **Check storage** - NetCDF files can be large, ensure you have enough space
3. **Use Parquet** - For analysis, Parquet format offers better performance
4. **Backup data** - Keep copies of your raw NetCDF files
5. **Explore visually** - Use tools like Pandas, Matplotlib, and Seaborn for visualization

---

### Happy Data Processing! 🌊🔍

```python
# Ready to explore ocean data?
print("Dive deep into oceanographic data with Argo-Clean!")
```

*For questions and support, please open an issue on GitHub.*
