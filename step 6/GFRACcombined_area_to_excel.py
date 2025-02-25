import xarray
import numpy as np
import pandas as pd
from functools import reduce

gfrac = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC_5_mins_combined_run2.NC", engine="netcdf4")
country_code = pd.read_excel("/vol/milkunC/achaidir/Country Grids/ISO-3166-Country-Code_Final.xlsx")
luh_static = xarray.open_dataset("/vol/milkunC/achaidir/Country Grids/gpw-v4-national-identifier-grid-rev11_2pt5_min_tif_NEW/CCODE_RASTER.nc", engine="netcdf4")
garea = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GAREA.NC", engine="netcdf4")
gfrac_ngfbfc = [element.strip() for element in gfrac.coords['NGFBFC'].data.astype('str').tolist()]

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
                         coords={ "longitude": gfrac.coords["longitude"].to_numpy(), "latitude": gfrac.coords["latitude"].to_numpy()})

garea_ha = np.nan_to_num(np.multiply(garea['GAREA'].isel(time=slice(0, 11)), 100))
gfracarea = np.zeros((17, 11, 2160, 4320), dtype="float32")

# gfrac = gfrac.isel(time=slice(0, 11))

for i, ngfbfc in enumerate(gfrac_ngfbfc):
    for n in range(11):
        gfracarea[i][n] = np.nan_to_num(np.multiply(gfrac['GFRAC_combined'].isel(NGFBFC=i, time=n), garea_ha[n]))

gfrac_newnetcdf2 = xarray.Dataset(
    coords={
        "time": pd.date_range(start='1970-01-01', end='2020-01-01', freq='5YS'),
        "latitude": gfrac.coords["latitude"].to_numpy(),
        "longitude": gfrac.coords["longitude"].to_numpy(),
        "country": country['country']
    })
coords = ("time", "latitude", "longitude")
data_vars = {
    ngfbc: (coords, gfracarea[i]) for i, ngfbc in enumerate(gfrac_ngfbfc)
}
gfrac_newnetcdf2 = gfrac_newnetcdf2.assign(data_vars)
gfrac_newnetcdf2.to_netcdf("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRACcombined_area.NC", engine="netcdf4", mode="w")

Grass = gfrac_newnetcdf2['grass']
Wheat = gfrac_newnetcdf2['Wheat']
Rice = gfrac_newnetcdf2['Rice']
Maize = gfrac_newnetcdf2['Maize']
Tropical = gfrac_newnetcdf2['Tropical cereals']
Othertemperatecereals = gfrac_newnetcdf2['Other temperate cereals']
Pulses = gfrac_newnetcdf2['Pulses']
Soybeans = gfrac_newnetcdf2['Soybeans']
Temperateoilcrops = gfrac_newnetcdf2['Temperate oil crops']
Tropicaloilcrops = gfrac_newnetcdf2['Tropical oil crops']
Temperaterootstubers = gfrac_newnetcdf2['Temperate roots & tubers']
Tropicalrootstubers = gfrac_newnetcdf2['Tropical roots & tubers']
Sugarcrops = gfrac_newnetcdf2['Sugar crops']
Oilpalmfruit = gfrac_newnetcdf2['Oil & palm fruit']
Vegetablesfruits = gfrac_newnetcdf2['Vegetables & fruits']
Othernonfooduxuryspices = gfrac_newnetcdf2['Other non-food & luxury & spices']
Plantbasedfibres = gfrac_newnetcdf2['Plant based fibres']

df = Grass.isel(time=0).to_dataframe()
df1 = Grass.isel(time=1).to_dataframe()
df2 = Grass.isel(time=2).to_dataframe()
df3 = Grass.isel(time=3).to_dataframe()
df4 = Grass.isel(time=4).to_dataframe()
df5 = Grass.isel(time=5).to_dataframe()
df6 = Grass.isel(time=6).to_dataframe()
df7 = Grass.isel(time=7).to_dataframe()
df8 = Grass.isel(time=8).to_dataframe()
df9 = Grass.isel(time=9).to_dataframe()
df10 = Grass.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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

# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Grass_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Grass_df = Grass_df.replace([np.inf, -np.inf], np.nan)
Grass_df = Grass_df.fillna(0)

df = Wheat.isel(time=0).to_dataframe()
df1 = Wheat.isel(time=1).to_dataframe()
df2 = Wheat.isel(time=2).to_dataframe()
df3 = Wheat.isel(time=3).to_dataframe()
df4 = Wheat.isel(time=4).to_dataframe()
df5 = Wheat.isel(time=5).to_dataframe()
df6 = Wheat.isel(time=6).to_dataframe()
df7 = Wheat.isel(time=7).to_dataframe()
df8 = Wheat.isel(time=8).to_dataframe()
df9 = Wheat.isel(time=9).to_dataframe()
df10 = Wheat.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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

# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Wheat_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Wheat_df = Wheat_df.replace([np.inf, -np.inf], np.nan)
Wheat_df = Wheat_df.fillna(0)

df = Rice.isel(time=0).to_dataframe()
df1 = Rice.isel(time=1).to_dataframe()
df2 = Rice.isel(time=2).to_dataframe()
df3 = Rice.isel(time=3).to_dataframe()
df4 = Rice.isel(time=4).to_dataframe()
df5 = Rice.isel(time=5).to_dataframe()
df6 = Rice.isel(time=6).to_dataframe()
df7 = Rice.isel(time=7).to_dataframe()
df8 = Rice.isel(time=8).to_dataframe()
df9 = Rice.isel(time=9).to_dataframe()
df10 = Rice.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Rice_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Rice_df = Rice_df.replace([np.inf, -np.inf], np.nan)
Rice_df = Rice_df.fillna(0)

df = Maize.isel(time=0).to_dataframe()
df1 = Maize.isel(time=1).to_dataframe()
df2 = Maize.isel(time=2).to_dataframe()
df3 = Maize.isel(time=3).to_dataframe()
df4 = Maize.isel(time=4).to_dataframe()
df5 = Maize.isel(time=5).to_dataframe()
df6 = Maize.isel(time=6).to_dataframe()
df7 = Maize.isel(time=7).to_dataframe()
df8 = Maize.isel(time=8).to_dataframe()
df9 = Maize.isel(time=9).to_dataframe()
df10 = Maize.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Maize_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Maize_df = Maize_df.replace([np.inf, -np.inf], np.nan)
Maize_df = Maize_df.fillna(0)

df = Tropical.isel(time=0).to_dataframe()
df1 = Tropical.isel(time=1).to_dataframe()
df2 = Tropical.isel(time=2).to_dataframe()
df3 = Tropical.isel(time=3).to_dataframe()
df4 = Tropical.isel(time=4).to_dataframe()
df5 = Tropical.isel(time=5).to_dataframe()
df6 = Tropical.isel(time=6).to_dataframe()
df7 = Tropical.isel(time=7).to_dataframe()
df8 = Tropical.isel(time=8).to_dataframe()
df9 = Tropical.isel(time=9).to_dataframe()
df10 = Tropical.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Tropical_cereals_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Tropical_cereals_df = Tropical_cereals_df.replace([np.inf, -np.inf], np.nan)
Tropical_cereals_df = Tropical_cereals_df.fillna(0)

df = Othertemperatecereals.isel(time=0).to_dataframe()
df1 = Othertemperatecereals.isel(time=1).to_dataframe()
df2 = Othertemperatecereals.isel(time=2).to_dataframe()
df3 = Othertemperatecereals.isel(time=3).to_dataframe()
df4 = Othertemperatecereals.isel(time=4).to_dataframe()
df5 = Othertemperatecereals.isel(time=5).to_dataframe()
df6 = Othertemperatecereals.isel(time=6).to_dataframe()
df7 = Othertemperatecereals.isel(time=7).to_dataframe()
df8 = Othertemperatecereals.isel(time=8).to_dataframe()
df9 = Othertemperatecereals.isel(time=9).to_dataframe()
df10 = Othertemperatecereals.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Othertemperatecereals_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Othertemperatecereals_df = Othertemperatecereals_df.replace([np.inf, -np.inf], np.nan)
Othertemperatecereals_df = Othertemperatecereals_df.fillna(0)

df = Pulses.isel(time=0).to_dataframe()
df1 = Pulses.isel(time=1).to_dataframe()
df2 = Pulses.isel(time=2).to_dataframe()
df3 = Pulses.isel(time=3).to_dataframe()
df4 = Pulses.isel(time=4).to_dataframe()
df5 = Pulses.isel(time=5).to_dataframe()
df6 = Pulses.isel(time=6).to_dataframe()
df7 = Pulses.isel(time=7).to_dataframe()
df8 = Pulses.isel(time=8).to_dataframe()
df9 = Pulses.isel(time=9).to_dataframe()
df10 = Pulses.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Pulses_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Pulses_df = Pulses_df.replace([np.inf, -np.inf], np.nan)
Pulses_df = Pulses_df.fillna(0)

df = Soybeans.isel(time=0).to_dataframe()
df1 = Soybeans.isel(time=1).to_dataframe()
df2 = Soybeans.isel(time=2).to_dataframe()
df3 = Soybeans.isel(time=3).to_dataframe()
df4 = Soybeans.isel(time=4).to_dataframe()
df5 = Soybeans.isel(time=5).to_dataframe()
df6 = Soybeans.isel(time=6).to_dataframe()
df7 = Soybeans.isel(time=7).to_dataframe()
df8 = Soybeans.isel(time=8).to_dataframe()
df9 = Soybeans.isel(time=9).to_dataframe()
df10 = Soybeans.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Soybeans_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Soybeans_df = Soybeans_df.replace([np.inf, -np.inf], np.nan)
Soybeans_df = Soybeans_df.fillna(0)

df = Temperateoilcrops.isel(time=0).to_dataframe()
df1 = Temperateoilcrops.isel(time=1).to_dataframe()
df2 = Temperateoilcrops.isel(time=2).to_dataframe()
df3 = Temperateoilcrops.isel(time=3).to_dataframe()
df4 = Temperateoilcrops.isel(time=4).to_dataframe()
df5 = Temperateoilcrops.isel(time=5).to_dataframe()
df6 = Temperateoilcrops.isel(time=6).to_dataframe()
df7 = Temperateoilcrops.isel(time=7).to_dataframe()
df8 = Temperateoilcrops.isel(time=8).to_dataframe()
df9 = Temperateoilcrops.isel(time=9).to_dataframe()
df10 = Temperateoilcrops.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Temperateoilcrops_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Temperateoilcrops_df = Temperateoilcrops_df.replace([np.inf, -np.inf], np.nan)
Temperateoilcrops_df = Temperateoilcrops_df.fillna(0)

df = Tropicaloilcrops.isel(time=0).to_dataframe()
df1 = Tropicaloilcrops.isel(time=1).to_dataframe()
df2 = Tropicaloilcrops.isel(time=2).to_dataframe()
df3 = Tropicaloilcrops.isel(time=3).to_dataframe()
df4 = Tropicaloilcrops.isel(time=4).to_dataframe()
df5 = Tropicaloilcrops.isel(time=5).to_dataframe()
df6 = Tropicaloilcrops.isel(time=6).to_dataframe()
df7 = Tropicaloilcrops.isel(time=7).to_dataframe()
df8 = Tropicaloilcrops.isel(time=8).to_dataframe()
df9 = Tropicaloilcrops.isel(time=9).to_dataframe()
df10 = Tropicaloilcrops.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Tropicaloilcrops_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Tropicaloilcrops_df = Tropicaloilcrops_df.replace([np.inf, -np.inf], np.nan)
Tropicaloilcrops_df = Tropicaloilcrops_df.fillna(0)

df = Temperaterootstubers.isel(time=0).to_dataframe()
df1 = Temperaterootstubers.isel(time=1).to_dataframe()
df2 = Temperaterootstubers.isel(time=2).to_dataframe()
df3 = Temperaterootstubers.isel(time=3).to_dataframe()
df4 = Temperaterootstubers.isel(time=4).to_dataframe()
df5 = Temperaterootstubers.isel(time=5).to_dataframe()
df6 = Temperaterootstubers.isel(time=6).to_dataframe()
df7 = Temperaterootstubers.isel(time=7).to_dataframe()
df8 = Temperaterootstubers.isel(time=8).to_dataframe()
df9 = Temperaterootstubers.isel(time=9).to_dataframe()
df10 = Temperaterootstubers.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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

# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Temperaterootstubers_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Temperaterootstubers_df = Temperaterootstubers_df.replace([np.inf, -np.inf], np.nan)
Temperaterootstubers_df = Temperaterootstubers_df.fillna(0)

df = Tropicalrootstubers.isel(time=0).to_dataframe()
df1 = Tropicalrootstubers.isel(time=1).to_dataframe()
df2 = Tropicalrootstubers.isel(time=2).to_dataframe()
df3 = Tropicalrootstubers.isel(time=3).to_dataframe()
df4 = Tropicalrootstubers.isel(time=4).to_dataframe()
df5 = Tropicalrootstubers.isel(time=5).to_dataframe()
df6 = Tropicalrootstubers.isel(time=6).to_dataframe()
df7 = Tropicalrootstubers.isel(time=7).to_dataframe()
df8 = Tropicalrootstubers.isel(time=8).to_dataframe()
df9 = Tropicalrootstubers.isel(time=9).to_dataframe()
df10 = Tropicalrootstubers.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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

# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Tropicalrootstubers_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Tropicalrootstubers_df = Tropicalrootstubers_df.replace([np.inf, -np.inf], np.nan)
Tropicalrootstubers_df = Tropicalrootstubers_df.fillna(0)

df = Sugarcrops.isel(time=0).to_dataframe()
df1 = Sugarcrops.isel(time=1).to_dataframe()
df2 = Sugarcrops.isel(time=2).to_dataframe()
df3 = Sugarcrops.isel(time=3).to_dataframe()
df4 = Sugarcrops.isel(time=4).to_dataframe()
df5 = Sugarcrops.isel(time=5).to_dataframe()
df6 = Sugarcrops.isel(time=6).to_dataframe()
df7 = Sugarcrops.isel(time=7).to_dataframe()
df8 = Sugarcrops.isel(time=8).to_dataframe()
df9 = Sugarcrops.isel(time=9).to_dataframe()
df10 = Sugarcrops.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Sugarcrops_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Sugarcrops_df = Sugarcrops_df.replace([np.inf, -np.inf], np.nan)
Sugarcrops_df = Sugarcrops_df.fillna(0)

df = Oilpalmfruit.isel(time=0).to_dataframe()
df1 = Oilpalmfruit.isel(time=1).to_dataframe()
df2 = Oilpalmfruit.isel(time=2).to_dataframe()
df3 = Oilpalmfruit.isel(time=3).to_dataframe()
df4 = Oilpalmfruit.isel(time=4).to_dataframe()
df5 = Oilpalmfruit.isel(time=5).to_dataframe()
df6 = Oilpalmfruit.isel(time=6).to_dataframe()
df7 = Oilpalmfruit.isel(time=7).to_dataframe()
df8 = Oilpalmfruit.isel(time=8).to_dataframe()
df9 = Oilpalmfruit.isel(time=9).to_dataframe()
df10 = Oilpalmfruit.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Oilpalmfruit_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Oilpalmfruit_df = Oilpalmfruit_df.replace([np.inf, -np.inf], np.nan)
Oilpalmfruit_df = Oilpalmfruit_df.fillna(0)

df = Vegetablesfruits.isel(time=0).to_dataframe()
df1 = Vegetablesfruits.isel(time=1).to_dataframe()
df2 = Vegetablesfruits.isel(time=2).to_dataframe()
df3 = Vegetablesfruits.isel(time=3).to_dataframe()
df4 = Vegetablesfruits.isel(time=4).to_dataframe()
df5 = Vegetablesfruits.isel(time=5).to_dataframe()
df6 = Vegetablesfruits.isel(time=6).to_dataframe()
df7 = Vegetablesfruits.isel(time=7).to_dataframe()
df8 = Vegetablesfruits.isel(time=8).to_dataframe()
df9 = Vegetablesfruits.isel(time=9).to_dataframe()
df10 = Vegetablesfruits.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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


# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Vegetablesfruits_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Vegetablesfruits_df = Vegetablesfruits_df.replace([np.inf, -np.inf], np.nan)
Vegetablesfruits_df = Vegetablesfruits_df.fillna(0)

df = Othernonfooduxuryspices.isel(time=0).to_dataframe()
df1 = Othernonfooduxuryspices.isel(time=1).to_dataframe()
df2 = Othernonfooduxuryspices.isel(time=2).to_dataframe()
df3 = Othernonfooduxuryspices.isel(time=3).to_dataframe()
df4 = Othernonfooduxuryspices.isel(time=4).to_dataframe()
df5 = Othernonfooduxuryspices.isel(time=5).to_dataframe()
df6 = Othernonfooduxuryspices.isel(time=6).to_dataframe()
df7 = Othernonfooduxuryspices.isel(time=7).to_dataframe()
df8 = Othernonfooduxuryspices.isel(time=8).to_dataframe()
df9 = Othernonfooduxuryspices.isel(time=9).to_dataframe()
df10 = Othernonfooduxuryspices.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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

# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Othernonfooduxuryspices_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Othernonfooduxuryspices_df = Othernonfooduxuryspices_df.replace([np.inf, -np.inf], np.nan)
Othernonfooduxuryspices_df = Othernonfooduxuryspices_df.fillna(0)

df = Plantbasedfibres.isel(time=0).to_dataframe()
df1 = Plantbasedfibres.isel(time=1).to_dataframe()
df2 = Plantbasedfibres.isel(time=2).to_dataframe()
df3 = Plantbasedfibres.isel(time=3).to_dataframe()
df4 = Plantbasedfibres.isel(time=4).to_dataframe()
df5 = Plantbasedfibres.isel(time=5).to_dataframe()
df6 = Plantbasedfibres.isel(time=6).to_dataframe()
df7 = Plantbasedfibres.isel(time=7).to_dataframe()
df8 = Plantbasedfibres.isel(time=8).to_dataframe()
df9 = Plantbasedfibres.isel(time=9).to_dataframe()
df10 = Plantbasedfibres.isel(time=10).to_dataframe()

table = pd.pivot_table(df, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table1 = pd.pivot_table(df1, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table2 = pd.pivot_table(df2, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table3 = pd.pivot_table(df3, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table4 = pd.pivot_table(df4, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table5 = pd.pivot_table(df5, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table6 = pd.pivot_table(df6, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table7 = pd.pivot_table(df7, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table8 = pd.pivot_table(df8, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table9 = pd.pivot_table(df9, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
table10 = pd.pivot_table(df10, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)

df_index = table.stack(level=0) # type: ignore
df_index1 = table1.stack(level=0)# type: ignore
df_index2 = table2.stack(level=0)# type: ignore
df_index3 = table3.stack(level=0)# type: ignore
df_index4 = table4.stack(level=0)# type: ignore
df_index5 = table5.stack(level=0)# type: ignore
df_index6 = table6.stack(level=0)# type: ignore
df_index7 = table7.stack(level=0)# type: ignore
df_index8 = table8.stack(level=0)# type: ignore
df_index9 = table9.stack(level=0)# type: ignore
df_index10 = table10.stack(level=0)# type: ignore

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

# Gabungkan semua DataFrame secara berurutan menggunakan reduce()
Plantbasedfibres_df = reduce(lambda left, right: pd.merge(left, right, on=["country", "type"]), frames)
Plantbasedfibres_df = Plantbasedfibres_df.replace([np.inf, -np.inf], np.nan)
Plantbasedfibres_df = Plantbasedfibres_df.fillna(0)

with pd.ExcelWriter("/vol/milkunarc/cadlan/stream_2/Step5/GFRACcombined_area.xlsx", engine='xlsxwriter') as writer:
    Grass_df.to_excel(writer, sheet_name=" Grass", index=False)
    Wheat_df.to_excel(writer, sheet_name=" Wheat", index=False)
    Rice_df.to_excel(writer, sheet_name=" Rice", index=False)
    Maize_df.to_excel(writer, sheet_name=" Maize", index=False)
    Tropical_cereals_df.to_excel(writer, sheet_name=" Tropical cereals", index=False)
    Othertemperatecereals_df.to_excel(writer, sheet_name=" Other temperate cereals", index=False)
    Pulses_df.to_excel(writer, sheet_name=" Pulses", index=False)
    Soybeans_df.to_excel(writer, sheet_name=" Soybeans", index=False)
    Temperateoilcrops_df.to_excel(writer, sheet_name=" Temperate oil crops", index=False)
    Tropicaloilcrops_df.to_excel(writer, sheet_name=" Tropical oil crops", index=False)
    Temperaterootstubers_df.to_excel(writer, sheet_name=" Temperate roots & tubers", index=False)
    Tropicalrootstubers_df.to_excel(writer, sheet_name=" Tropical roots & tubers", index=False)
    Sugarcrops_df.to_excel(writer, sheet_name=" Sugar crops", index=False)
    Oilpalmfruit_df.to_excel(writer, sheet_name=" Oil & palm fruit", index=False)
    Vegetablesfruits_df.to_excel(writer, sheet_name=" Vegetables & fruits", index=False)
    Othernonfooduxuryspices_df.to_excel(writer, sheet_name=" Othernon-food&luxury spices", index=False)
    Plantbasedfibres_df.to_excel(writer, sheet_name=" Plant based fibres", index=False)


