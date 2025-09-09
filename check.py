import netCDF4

path = "argo_nc_files_requests/R1900438_234.nc"
ds = netCDF4.Dataset(path)

print(ds.variables.keys())  # all variable names
print(ds.variables["TEMP"][:])  # temperature values
print(ds.variables["PRES"][:])  # pressure values

# check a suspicious attribute
print(ds.variables["CYCLE_NUMBER"][:])

