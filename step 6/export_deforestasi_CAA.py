import numpy as np
import xarray
import pandas as pd

GFRAC = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC_5_mins_combined_run2.NC", engine="netcdf4")
GAREA = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GAREA.NC", engine="netcdf4")
GLCT_nick = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GLCT_nick.NC", engine="netcdf4")
GLCT_1st = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/glct_trans_1st.NC", engine="netcdf4")

#garea dan gfrac area untuk 1970-2020 saja
garea_2020 = GAREA.isel(time=slice(0, 10))
garea_ha = np.nan_to_num(np.multiply(garea_2020['GAREA'].to_numpy(), 100))
gfrac_2020 = GFRAC.isel(time=slice(0, 11)) 
gfrac_area = np.zeros((11, 17, 2160, 4320), dtype="float32")

#pada grid dan tahun yang natveg to agri, copy kelas GLCT as origin, otherwise " "
GLCT_nav2agri = np.zeros((10, 2160, 4320), dtype="<U14")
for n in range(10):
    GLCT_nav2agri[n] = np.where(GLCT_1st['GLCT_1st'].isel(time=n) == 'natveg_to_agri', GLCT_nick['GLCT_trans'].isel(time=n), '')

GLCT_nav2agrinetcdf = xarray.Dataset({
    "GLCT_nav2agri":(["time","latitude", "longitude"], GLCT_nav2agri)
},coords={
        "time": pd.date_range(start='1975-01-01', end='2020-01-01', freq='5YS'),
        "latitude": GFRAC.coords["latitude"].to_numpy(),
        "longitude": GFRAC.coords["longitude"].to_numpy()
})

#pada grid dan tahun yang natveg to agri masukkan gfrac area nya (dengan pengetahuan seluruh natveg convert to agri, shg equal to luas grid)
GAREA_nav2agri = np.zeros((10, 2160, 4320), dtype='float32')
for n in range(10):
    GAREA_nav2agri[n] = np.where(GLCT_1st['GLCT_1st'].isel(time=n) == 'natveg_to_agri', garea_ha[n], 0)

# n+1 = 1970 ambil 1975
natveg2crops = np.zeros((17, 10, 2160, 4320), dtype="float32")
for i in range(17):
    for n in range(10):
        natveg2crops[i][n] = np.nan_to_num(np.multiply(gfrac_2020['GFRAC_combined'].isel(time=n+1, NGFBFC=i), GAREA_nav2agri[n]))

country_code = pd.read_excel("/vol/milkunC/achaidir/Country Grids/ISO-3166-Country-Code_Final.xlsx")
luh_static = xarray.open_dataset("/vol/milkunC/achaidir/Country Grids/gpw-v4-national-identifier-grid-rev11_2pt5_min_tif_NEW/CCODE_RASTER.nc", engine="netcdf4")

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
            
country = xarray.Dataset({"country": (["latitude", "longitude"], ccode_worldwide_int)},
                         coords={ "longitude": GFRAC.coords["longitude"].to_numpy(), "latitude": GFRAC.coords["latitude"].to_numpy()})

gfrac_ngfbfc = [element.strip() for element in GFRAC.coords['NGFBFC'].data.astype('str').tolist()]
natveg2cropsnetcdf = xarray.Dataset(
coords={
        "time": pd.date_range(start='1975-01-01', end='2020-01-01', freq='5YS'),
        "latitude": GFRAC.coords["latitude"].to_numpy(),
        "longitude": GFRAC.coords["longitude"].to_numpy(),
        "origin": GLCT_nav2agrinetcdf['GLCT_nav2agri'],
        "country": country['country']
    })

coords = ("time", "latitude", "longitude")
data_vars = {
    ngfbc: (coords, natveg2crops[i]) for i, ngfbc in enumerate(gfrac_ngfbfc)
}
natveg2cropsnetcdf = natveg2cropsnetcdf.assign(data_vars)

natveg2cropsnetcdf.to_netcdf("/vol/milkunC/achaidir/IMAGE PBL/maps/natveg_to_crops_country_ccode.NC", mode="w", format="NETCDF4")