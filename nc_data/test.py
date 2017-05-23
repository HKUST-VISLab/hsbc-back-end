import numpy as np
from netCDF4 import Dataset

# All NetCDF exists with the expectation that it will be written to
# disk, so it's required to provide a file name when instantiating a
# Dataset (see http://unidata.github.io/netcdf4-python/netCDF4.Dataset-class.html)
file_name = "nc_data/pres_temp_4D.nc"

# default is to overwrite existing file. Can create in-memory via diskless=True
dataset = Dataset(file_name, 'w', )

### Create our four dimensions ###
# work with a 6 x 12 grid
NLAT = 6
NLON = 12
# The 'None's mean that there is no limit to their number
# see http://unidata.github.io/netcdf4-python/netCDF4.Dataset-class.html#createDimension
time_dim = dataset.createDimension('time', None)
level_dim = dataset.createDimension('level', None)
lat_dim = dataset.createDimension('lat', NLAT)
lon_dim = dataset.createDimension('lon', NLON)

# Now we create variables. All four dimensions are variables, and we'll add
# two more non-dimensional variables.
# The second argument is the data type: 4-byte int, 8-byte float, etc.
time_var = dataset.createVariable('time', 'f8', ('time', ))
level_var = dataset.createVariable('level', 'i4', ('level', ))
lat_var = dataset.createVariable('lat', 'f4', ('lat', ))
lon_var = dataset.createVariable('lon', 'f4', ('lon', ))
# for non-dimensional variables, we must declare the dimensions they vary over
pres_var = dataset.createVariable('pres', 'f4',
                                  ('time', 'level', 'lat', 'lon'))
temp_var = dataset.createVariable('temp', 'f4',
                                  ('time', 'level', 'lat', 'lon'))

# now we set some attributes: units and other metadata
# Possibly confusingly, these are set using variable methods, not dimension
# Though this makes sense since we need metadata not only for dimensions
dataset.description = 'Sample NetCDF File ported from NetCDF-C tutorial'
dataset.history = 'Created ' + time.ctime(time.time())
dataset.source  = 'Rebirth Galaxy\'s tutorial'
lat_var.units = 'degrees north'
lon_var.units = 'degrees east'
level_var.units = 'meters'  # could be water table data, for example
temp_var.units = 'C'
time_var.units = 'hours since 2010-01-01 00:00:00'
time_var.calendar = 'gregorian'  # duh, (http://unidata.github.io/netcdf4-python/)

start_lat = 25.0
start_lon = -120.0
# lat_var[:] = np.arange([start_lat + 0.5*i for i in range(NLAT)])
lat_var[:] = np.array([start_lat + 0.5*i for i in range(NLAT)])
lon_var[:] = np.array([start_lon + 0.5*i for i in range(NLON)])
# not too worried about realistic times and levels for now
time_var[:] = np.array([0,1])
level_var[:] = np.array([0,1])

# Now let's create some data for each element of the temperature and pressure
# we will use the same 3D data for each time step.
temp_data = np.zeros((2, NLAT, NLON))
pres_data = np.zeros((2, NLAT, NLON))

start_temp = 9.0
start_pres = 900

i = 0
for lvl in range(len(level_var[:])):
    for lat in range(len(lat_var[:])):
        for lon in range(len(lon_var[:])):
            temp_data[lvl, lat, lon] = start_temp + i
            pres_data[lvl, lat, lon] = start_pres + i
            i += 1

full_temp = np.array([temp_data]*2)
full_pres = np.array([pres_data]*2)

temp_var[:] = full_temp
pres_var[:] = full_pres

# closing the Dataset saves the file
dataset.close()