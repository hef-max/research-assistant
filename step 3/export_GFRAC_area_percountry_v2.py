
import numpy as np
import pandas as pd
import xarray

# gfrac = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC_5_mins_combined_run2.NC", engine="netcdf4")
# garea = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GAREA.NC", engine="netcdf4")
# gfrac_ngfbfc = ['grass', 'wheat', 'rice', 'maize', 'tropical cereals', 'other temperate cereals', 'pulses', 'soybeans', 'temperate oil crops',
#                 'tropical oil crops', 'temperate roots & tubers', 'tropical roots & tubers', 'sugar crops', 'oil & palm fruit',
#                 'vegetables & fruits', 'other non-food & luxury & spices', 'plant based fibres']

# garea_2020 = garea.isel(time=slice(0, 11)) #slice 1970 to 2020
# garea_ha = np.nan_to_num(np.multiply(garea_2020['GAREA'].to_numpy(), 100))
# gfrac_2020 = gfrac.isel(time=slice(0, 11)) #slice 1970 to 2020
# gfrac_area = np.zeros((17, 11, 2160, 4320), dtype="float32")

# for i in range(17):
#     gfrac_area[i] = np.nan_to_num(np.multiply(gfrac_2020['GFRAC_combined'].isel(NGFBFC=i), garea_ha))

# gfrac_area = gfrac_area.reshape(11, 17, 2160, 4320)

# gfrac_area_netcdf = xarray.Dataset({
#     "GFRAC_area_combined":(["time","NGFBFC", "latitude", "longitude"], gfrac_area)
# },coords={
#         "time": pd.date_range(start='1970-01-01', end='2020-01-01', freq='5YS'),
#         "NGFBFC" : list(gfrac_ngfbfc),
#         "latitude": gfrac.coords["latitude"].to_numpy(),
#         "longitude": gfrac.coords["longitude"].to_numpy()
#     })
# gfrac_area_netcdf.to_netcdf("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC_area_ha_v2.nc", mode="w", format="NETCDF4")

# gfrac_area_ha = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC_area_ha_v2.nc", engine="netcdf4")

# kategori = ['grass', 'wheat', 'rice', 'maize', 'tropical cereals', 'other temperate cereals', 'pulses', 'soybeans', 'temperate oil crops',
#                 'tropical oil crops', 'temperate roots & tubers', 'tropical roots & tubers', 'sugar crops', 'oil & palm fruit',
#                 'vegetables & fruits', 'other non-food & luxury & spices', 'plant based fibres']

# GFRAC_area_occur = gfrac_area.reshape(17, 11, 2160, 4320)

# GFRAC_area_per5years_netcdf = xarray.Dataset(
#     coords={
#         "time": pd.date_range(start='1970-01-01', end='2020-01-01', freq='5YS'),
#         "latitude": gfrac.coords["latitude"].to_numpy(),
#         "longitude": gfrac.coords["longitude"].to_numpy(),
#     })
# coords = ("time", "latitude", "longitude")
# data_vars = {
#     kategori: (coords, GFRAC_area_occur[i]) for i, kategori in enumerate(kategori)
# }
# GFRAC_area_per5years_netcdf = GFRAC_area_per5years_netcdf.assign(data_vars)
# GFRAC_area_per5years_netcdf.to_netcdf("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC_area_per5years.nc", mode="w", format="NETCDF4")

##Export to excel

GFRAC_country = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC_area_per5years.nc", engine="netcdf4")
GFRAC_country = GFRAC_country['maize']

gfrac_ngfbfc = ['maize']

country_code = pd.read_excel("/vol/milkunC/achaidir/LUH2 2022/ISO-3166-Country-Code_REV.xlsx", engine="openpyxl")
staticcode = "/vol/milkunC/achaidir/IMAGE PBL/gpw_v4_national_identifier_grid_rev11_5_min_finall.nc"
country_grids = xarray.open_dataset(staticcode, engine="netcdf4")

ccode_iso = list(country_code['country-code'])
cname_iso = list(country_code['ISO Country'])
csub_iso = list(country_code['Hong Sub Region (PAKAI YANG INI)'])
cregion_iso = list(country_code['ISO Region'])
ccode_worldwide_int = country_grids['ccode'].to_numpy().astype('int64')

ccode_convert = np.zeros((2160, 4320), dtype="<U50")
ccode_convert_sub = np.zeros((2160, 4320), dtype="<U30")
ccode_convert_cregion = np.zeros((2160, 4320), dtype="<U12")

#country
ccode_dict = {}
for i, ccode in enumerate(ccode_iso):
    ccode_dict[ccode] = cname_iso[i]

for i in range(2160):
    for j in range(4320):
        if (ccode_worldwide_int[i][j] in ccode_dict.keys()):
            ccode_convert[i][j] = ccode_dict[ccode_worldwide_int[i][j]]
        else:
            ccode_convert[i][j] = "ocean"

country = xarray.Dataset({"country": (["latitude", "longitude"], ccode_convert)},
                         coords={ "longitude": GFRAC_country.coords["longitude"].to_numpy(), "latitude": GFRAC_country.coords["latitude"].to_numpy()})
GFRAC_country = GFRAC_country.assign_coords(country_name=country.country)

#subregion
ccode_dict = {}
for i, ccode in enumerate(ccode_iso):
    ccode_dict[ccode] = csub_iso[i]

for i in range(2160):
    for j in range(4320):
        if (ccode_worldwide_int[i][j] in ccode_dict.keys()):
            ccode_convert_sub[i][j] = ccode_dict[ccode_worldwide_int[i][j]]
        else:
            ccode_convert_sub[i][j] = "ocean"
            
subregion = xarray.Dataset({"subregion": (["latitude", "longitude"], ccode_convert_sub)},
                           coords={ "longitude": GFRAC_country.coords["longitude"].to_numpy(), "latitude": GFRAC_country.coords["latitude"].to_numpy()})
GFRAC_country = GFRAC_country.assign_coords(subregion_name=subregion.subregion)

#region
ccode_dict = {}
for i, ccode in enumerate(ccode_iso):
    ccode_dict[ccode] = cregion_iso[i]

for i in range(2160):
    for j in range(4320):
        if (ccode_worldwide_int[i][j] in ccode_dict.keys()):
            ccode_convert_cregion[i][j] = ccode_dict[ccode_worldwide_int[i][j]]
        else:
            ccode_convert_cregion[i][j] = "ocean"

region = xarray.Dataset({"region": (["latitude", "longitude"], ccode_convert_cregion)},
                        coords={ "longitude": GFRAC_country.coords["longitude"].to_numpy(), "latitude": GFRAC_country.coords["latitude"].to_numpy()})
GFRAC_country = GFRAC_country.assign_coords(region_name=region.region)


#Export to dataframe
df = GFRAC_country.isel(time=0).to_dataframe()
df1 = GFRAC_country.isel(time=1).to_dataframe()
df2 = GFRAC_country.isel(time=2).to_dataframe()
df3 = GFRAC_country.isel(time=3).to_dataframe()
df4 = GFRAC_country.isel(time=4).to_dataframe()
df5 = GFRAC_country.isel(time=5).to_dataframe()
df6 = GFRAC_country.isel(time=6).to_dataframe()
df7 = GFRAC_country.isel(time=7).to_dataframe()
df8 = GFRAC_country.isel(time=8).to_dataframe()
df9 = GFRAC_country.isel(time=9).to_dataframe()
df10 = GFRAC_country.isel(time=10).to_dataframe()

import pandas as pd
table = pd.pivot_table(df, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table1 = pd.pivot_table(df1, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table2 = pd.pivot_table(df2, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table3 = pd.pivot_table(df3, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table4 = pd.pivot_table(df4, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table5 = pd.pivot_table(df5, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table6 = pd.pivot_table(df6, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table7 = pd.pivot_table(df7, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table8 = pd.pivot_table(df8, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table9 = pd.pivot_table(df9, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)

table10 = pd.pivot_table(df10, values=gfrac_ngfbfc,
                       index=["country_name"], columns=['time'], aggfunc="sum", fill_value=0)


df_index = table.stack(level=0)
df_index1 = table1.stack(level=0)
df_index2 = table2.stack(level=0)
df_index3 = table3.stack(level=0)
df_index4 = table4.stack(level=0)
df_index5 = table5.stack(level=0)
df_index6 = table6.stack(level=0)
df_index7 = table7.stack(level=0)
df_index8 = table8.stack(level=0)
df_index9 = table9.stack(level=0)
df_index10 = table10.stack(level=0)

data = pd.to_datetime(df_index.columns, format='%d/%m/%Y %H.%M.%S')
df_index.columns = data.year
data1 = pd.to_datetime(df_index1.columns, format='%d/%m/%Y %H.%M.%S')
df_index1.columns = data1.year
data2 = pd.to_datetime(df_index2.columns, format='%d/%m/%Y %H.%M.%S')
df_index2.columns = data2.year
data3 = pd.to_datetime(df_index3.columns, format='%d/%m/%Y %H.%M.%S')
df_index3.columns = data3.year
data4 = pd.to_datetime(df_index4.columns, format='%d/%m/%Y %H.%M.%S')
df_index4.columns = data4.year
data5 = pd.to_datetime(df_index5.columns, format='%d/%m/%Y %H.%M.%S')
df_index5.columns = data5.year
data6 = pd.to_datetime(df_index6.columns, format='%d/%m/%Y %H.%M.%S')
df_index6.columns = data6.year
data7 = pd.to_datetime(df_index7.columns, format='%d/%m/%Y %H.%M.%S')
df_index7.columns = data7.year
data8 = pd.to_datetime(df_index8.columns, format='%d/%m/%Y %H.%M.%S')
df_index8.columns = data8.year
data9 = pd.to_datetime(df_index9.columns, format='%d/%m/%Y %H.%M.%S')
df_index9.columns = data9.year
data10 = pd.to_datetime(df_index10.columns, format='%d/%m/%Y %H.%M.%S')
df_index10.columns = data10.year

df_index = df_index.reset_index()
df_index1 = df_index1.reset_index()
df_index2 = df_index2.reset_index()
df_index3 = df_index3.reset_index()
df_index4 = df_index4.reset_index()
df_index5 = df_index5.reset_index()
df_index6 = df_index6.reset_index()
df_index7 = df_index7.reset_index()
df_index8 = df_index8.reset_index()
df_index9 = df_index9.reset_index()
df_index10 = df_index10.reset_index()

df_index.rename(columns={'level_1': "type"}, inplace=True)
df_index1.rename(columns={'level_1': "type"}, inplace=True)
df_index2.rename(columns={'level_1': "type"}, inplace=True)
df_index3.rename(columns={'level_1': "type"}, inplace=True)
df_index4.rename(columns={'level_1': "type"}, inplace=True)
df_index5.rename(columns={'level_1': "type"}, inplace=True)
df_index6.rename(columns={'level_1': "type"}, inplace=True)
df_index7.rename(columns={'level_1': "type"}, inplace=True)
df_index8.rename(columns={'level_1': "type"}, inplace=True)
df_index9.rename(columns={'level_1': "type"}, inplace=True)
df_index10.rename(columns={'level_1': "type"}, inplace=True)

frames = [df_index, df_index1, df_index2, df_index3, df_index4, df_index5, df_index6, df_index7, df_index8, df_index9, df_index10]

from functools import reduce

# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
merged_df = reduce(lambda left, right: pd.merge(left, right, on=['country_name', 'type']), frames)
merged_df = merged_df.replace([np.inf, -np.inf], np.nan)
merged_df = merged_df.fillna(0)
# merged_df
#pivot table tahun jadi baris

merged_df.to_excel("/vol/milkunarc/cadlan/stream_2/GFRAC_area_per5years_maize.xlsx", index=False)


