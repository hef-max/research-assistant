
import pandas as pd
import numpy as np
import xarray
from functools import reduce

country_code = pd.read_excel("/vol/milkunC/achaidir/Country Grids/ISO-3166-Country-Code_Final.xlsx", engine="openpyxl")
luh_static = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/gpw_v4_national_identifier_grid_rev11_5_min_finall.nc", engine="netcdf4")
garea = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GAREA.NC", engine="netcdf4")

gfrac_5min = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC.NC", engine="netcdf4")
gfrac_5min = gfrac_5min.drop_sel(NGFBFC=b'grass                                             ')
gfrac_5min = gfrac_5min.drop_sel(NGFBFC=b'Grains (biofuel)                                  ')
gfrac_5min = gfrac_5min.drop_sel(NGFBFC=b'Oil crops (biofuel)                               ')
gfrac_5min = gfrac_5min.drop_sel(NGFBFC=b'Sugar cane (biofuel)                              ')
gfrac_5min = gfrac_5min.drop_sel(NGFBFC=b'Woody biofuel                                     ')
gfrac_5min = gfrac_5min.drop_sel(NGFBFC=b'Non-woody biofuel                                 ')
gfrac_5min = gfrac_5min.isel(time=slice(0, 11))
gfrac_ngfbfc = [element.strip() for element in gfrac_5min.coords['NGFBFC'].data.astype('str').tolist()]

ccode_iso = list(country_code['country-code'])
cname_iso = list(country_code['ISO Country'])

ccode_dict = {}
for i, ccode in enumerate(ccode_iso):
    ccode_dict[ccode] = cname_iso[i]

ccode_worldwide_int = luh_static['ccode'].to_numpy().astype('int64')

ccode_convert = np.zeros((2160, 4320), dtype="<U64") #rubah ke <U64

for i in range(2160):
    for j in range(4320):
        if (ccode_worldwide_int[i][j] in ccode_dict.keys()):
            ccode_convert[i][j] = ccode_dict[ccode_worldwide_int[i][j]]
        else:
            ccode_convert[i][j] = "ocean"
            
country = xarray.Dataset({"country": (["latitude", "longitude"], ccode_convert)},
                         coords={ "longitude": gfrac_5min.coords["longitude"].to_numpy(), "latitude": gfrac_5min.coords["latitude"].to_numpy()})

gfrac_newnetcdf = xarray.Dataset(
    coords={
        "time": pd.date_range(start='1970-01-01', end='2020-01-01', freq='5YS'),
        "latitude": gfrac_5min.coords["latitude"].to_numpy(),
        "longitude": gfrac_5min.coords["longitude"].to_numpy()
    })
coords = ("time", "latitude", "longitude")
data_vars = {
    ngfbc: (coords, gfrac_5min['GFRAC'].isel(NGFBFC=i).values) for i, ngfbc in enumerate(gfrac_ngfbfc)
}
gfrac_newnetcdf = gfrac_newnetcdf.assign(data_vars)

garea_ha = np.nan_to_num(np.multiply(garea['GAREA'].isel(time=slice(0, 11)), 100))
gfracarea = np.zeros((32, 11, 2160, 4320), dtype="float32")

for i, ngfbfc in enumerate(gfrac_ngfbfc):
    for n in range(11):
        gfracarea[i][n] = np.nan_to_num(np.multiply(gfrac_newnetcdf[ngfbfc].isel(time=n), garea_ha[n]))

gfrac_newnetcdf2 = xarray.Dataset(
    coords={
        "time": pd.date_range(start='1970-01-01', end='2020-01-01', freq='5YS'),
        "latitude": gfrac_5min.coords["latitude"].to_numpy(),
        "longitude": gfrac_5min.coords["longitude"].to_numpy(),
        "country": country['country']
    })
coords = ("time", "latitude", "longitude")
data_vars = {
    ngfbc: (coords, gfracarea[i]) for i, ngfbc in enumerate(gfrac_ngfbfc)
}
gfrac_newnetcdf2 = gfrac_newnetcdf2.assign(data_vars)
gfrac_newnetcdf2.to_netcdf("/vol/milkunarc/cadlan/stream_2/Step5/GFRACarea_32.NC", engine="netcdf4", mode="w")


