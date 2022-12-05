import pydap
from pydap.client import open_url

databaseURLlist = []
with open('urls_sorted.txt', newline='') as file:
    for url in file:
        databaseURLlist.append(url)
        break

print(databaseURLlist)

import netCDF4
import xarray as xr

for url in databaseURLlist:
#    print(url)
    dataName = url[url.rfind('/') +1:url.rfind('.')]
    print("grabbing and storing ", dataName)
    nc = xr.open_dataset(url)
    nc.to_dataframe().to_csv(dataName + '.csv')
    print("finished with ", dataName)