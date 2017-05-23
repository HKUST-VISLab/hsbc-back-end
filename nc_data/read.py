import numpy as np
from netCDF4 import Dataset
nc_file = Dataset('nc_data/Contour_3D_5m.nc', 'r')

# clarify the structure

print("groups: ", nc_file.groups)

print("the number of dimensions: ", len(nc_file.dimensions))
for val in nc_file.dimensions.values():
    print("dimension: ", val)

print("the number of variables: ", len(nc_file.variables))
for val in nc_file.variables.values():
    print("variable: ", val)
print(nc_file.variables.keys())

print("the number of attributes: ", len(nc_file.ncattrs()))
for attr in nc_file.ncattrs():
    print("attr: ",attr, getattr(nc_file, attr))

for val in nc_file.variables.values():
    shape = len(val.shape)
    print(shape)

for i in range(7):
    dataset = nc_file.variables['Dataset'+str(i+1)]
    shape = dataset.shape
    for x in shape[0]:
        for y in shape[1]:
            for z in shape[2]:
                data = dataset[x][y][z]
