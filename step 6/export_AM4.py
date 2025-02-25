import matplotlib.pyplot as plt
import numpy as np
import xarray
import pandas as pd

GFRAC = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC_5_mins_combined_run2.NC", engine="netcdf4")
GAREA = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GAREA.NC", engine="netcdf4")
GLCT_1st = xarray.open_dataset("/vol/milkunarc/cadlan/stream_2/Step1/AM1/glct_trans_1st.NC", engine="netcdf4")
luc_agri_emissions_per_crops_AM3_v4 = xarray.open_dataset("/vol/milkunarc/cadlan/stream_2/Step3/AM3/luc_agri_emissions_per_crops_AM3_v4.NC", engine="netcdf4")
emission_CO2_netcdf_AM23 = xarray.open_dataset("/vol/milkunarc/cadlan/stream_2/Step2/AM23/emission_CO2_AM23.nc", engine="netcdf4")

garea_2020 = GAREA.isel(time=slice(0, 11))
garea_ha = np.nan_to_num(np.multiply(garea_2020['GAREA'].to_numpy(), 100))

gfrac_area = np.zeros((11, 17, 2160, 4320), dtype="float32")

for n in range(11):
    for i in range(17):
        gfrac_area[n][i] = np.nan_to_num(np.multiply(GFRAC['GFRAC_combined'].isel(time=n, NGFBFC=i), garea_ha[n]))

gfrac_ngfbfc = [element.strip() for element in GFRAC.coords['NGFBFC'].data.astype('str').tolist()]

gfrac_area_netcdf = xarray.Dataset({
    "GFRAC_area":(["time", "NGFBFC", "latitude", "longitude"], gfrac_area)
},coords={
        "time": pd.date_range(start='1970-01-01', end='2020-01-01', freq='5YS'),
        "NGFBFC": gfrac_ngfbfc,
        "latitude": GFRAC.coords["latitude"].to_numpy(),
        "longitude": GFRAC.coords["longitude"].to_numpy()
})
gfrac_area_netcdf

## validasi
x = [-51.706, -55.974643, 101.457, 109.38, -96.6289, 48.966]
y = [-22.698, -21.208333, 31.4420, -7.11, 18.549639, -18.07]
n = 2
emission_CO2_netcdf_AM23['LUC Crops Emission'].sel(latitude=y[n], longitude=x[n], method='nearest').to_pandas()
GFRAC['GFRAC_combined'].isel(time=slice(0, 11)).sel(latitude=y[n], longitude=x[n], method='nearest').to_pandas()

#rumus rujukan

# Define the parameters
debt = 100
interval = 5
decay_rate = 0.10  # Increased decay rate

# Generate emissions
emissions = []
tahun = list(range(1975, 2025, 5))
values = GLCT_1st['GLCT_1st'].sel(latitude=y[n], longitude=x[n], method='nearest').values
index = np.argwhere(values == 'natveg_to_agri')
start_year = tahun[index[0][0]]  # Start year
years = len(list(range(start_year, 2025, 1)))  # Changed from 50 to 45 to match the years from 1975 to 2020

# Calculate the total sum of the decay series
total_sum = sum(1 / (1 + decay_rate) ** (t * interval) for t in range(int(years / interval)))

for t in range(int(years / interval)):
    emission = debt / total_sum * (1 / (1 + decay_rate) ** (t * interval))  # Calculate scaled emissions
    emissions.append(emission)

# Scale emissions so that year 0 is about 40, while keeping the total sum equal to debt
scaling_factor = 40 / emissions[0]
scaled_emissions = [emission * scaling_factor for emission in emissions]
total_scaled_emissions = sum(scaled_emissions)

# Adjust the scaled emissions to ensure the total sum equals debt
adjusted_scaled_emissions = np.divide([emission * debt / total_scaled_emissions for emission in scaled_emissions], 100)

proporsi_emisi = GFRAC['GFRAC_combined'].isel(time=slice(index[0][0]+1, 11)).sel(latitude=y[n], longitude=x[n], method='nearest').to_numpy() *\
      np.array(adjusted_scaled_emissions[: len(list(range(start_year, 2025, 5)))])[:, np.newaxis] *\
          emission_CO2_netcdf_AM23['LUC Crops Emission'].sel(latitude=y[n], longitude=x[n], method='nearest').values

total_emisi_percrops = pd.DataFrame(proporsi_emisi).sum().values
print(total_emisi_percrops.sum())

# Uji coba
# Define the parameters
debt = 100
interval = 5
decay_rate = 0.10  # Increased decay rate

tahun = list(range(1975, 2025, 5))
GLCT_1st = GLCT_1st.isel(time=slice(0, 10))
index_glct = np.argwhere(GLCT_1st['GLCT_1st'].values == 'natveg_to_agri')

total_emisi_percrops = np.zeros((10, 2160, 4320, 17), dtype='float32')

for year, lat, lon in index_glct:
    emissions = []
    start_year = tahun[year]  # Start year
    years = len(list(range(start_year, 2025, 1)))  # Changed from 50 to 45 to match the years from 1975 to 2020

    # Calculate the total sum of the decay series
    total_sum = sum(1 / (1 + decay_rate) ** (t * interval) for t in range(int(years / interval)))

    for t in range(int(years / interval)):
        emission = debt / total_sum * (1 / (1 + decay_rate) ** (t * interval))  # Calculate scaled emissions
        emissions.append(emission)

    # Scale emissions so that year 0 is about 40, while keeping the total sum equal to debt
    scaling_factor = 40 / emissions[0]
    scaled_emissions = [emission * scaling_factor for emission in emissions]
    total_scaled_emissions = sum(scaled_emissions)

    # Adjust the scaled emissions to ensure the total sum equals debt
    adjusted_scaled_emissions = np.divide([emission * debt / total_scaled_emissions for emission in scaled_emissions], 100)

    proporsi_emisi = (GFRAC['GFRAC_combined'].isel(time=slice(year+1, 11), latitude=lat, longitude=lon).to_numpy() *\
      np.array(adjusted_scaled_emissions[: len(list(range(start_year, 2025, 5)))])[:, np.newaxis] *\
          emission_CO2_netcdf_AM23['LUC Crops Emission'].isel(latitude=lat, longitude=lon).to_numpy())
    
    total_emisi_percrops[year][lat][lon] = pd.DataFrame(proporsi_emisi).sum().values # menyatukan semua dalam 1 tahun dari 1975 - 2020

gfrac_ngfbfc = [element.strip() for element in GFRAC.coords['NGFBFC'].data.astype('str').tolist()]

total_emisi_percrops_netcdf = xarray.Dataset({
    "total_emisi": (["time", "latitude", "longitude", "NGFBFC"], total_emisi_percrops)
},coords={
        "time": pd.date_range(start='1975-01-01', end='2020-01-01', freq='5YS'),
        "latitude": GFRAC.coords["latitude"].to_numpy(),
        "longitude": GFRAC.coords["longitude"].to_numpy(),
        "NGFBFC": gfrac_ngfbfc
})
total_emisi_percrops_netcdf

total_emisi_percrops_netcdf.to_netcdf("/vol/milkunarc/cadlan/stream_2/Step3/AM4/total_emisi_percrops_netcdf1.nc", mode="w", format="NETCDF4")

import xarray
import numpy as np
import pandas as pd

total_emisi_percrops_netcdf = xarray.open_dataset("/vol/milkunarc/cadlan/stream_2/Step3/AM4/total_emisi_percrops_netcdf1.nc", engine="netcdf4")

# x = [-51.706, -55.974643, 101.457, 109.38, -96.6289, 48.966]
# y = [-22.698, -21.208333, 31.4420, -7.11, 18.549639, -18.07]
# n = 2
# total_emisi_percrops_netcdf['total_emisi'].sel(latitude=y[n], longitude=x[n], method='nearest').to_pandas().iloc[1, :].sum()

sum_total_emisi_percrops = np.zeros((17, 1, 2160, 4320), dtype='float32')
for i in range(17):
    for n in range(10):
        sum_total_emisi_percrops[i][0] += total_emisi_percrops_netcdf['total_emisi'].isel(time=n, NGFBFC=i).values

country_code = pd.read_excel("/vol/milkunC/achaidir/Country Grids/ISO-3166-Country-Code_Final.xlsx", engine="openpyxl")
luh_static = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/gpw_v4_national_identifier_grid_rev11_5_min_finall.nc", engine="netcdf4")

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
                         coords={ "longitude": GFRAC.coords["longitude"].to_numpy(), "latitude": GFRAC.coords["latitude"].to_numpy()})

len(country_code[country_code['IMAGE Region Name'] == "Korea"]['ISO Country'].values[0])

gfrac_ngfbfc = [element.strip() for element in GFRAC.coords['NGFBFC'].data.astype('str').tolist()]

sum_total_emisi_percrops_netcdf = xarray.Dataset(
    coords={
        "time": pd.to_datetime(['2020-01-01']),
        "latitude": GFRAC.coords["latitude"].to_numpy(),
        "longitude": GFRAC.coords["longitude"].to_numpy(),
        "country": country["country"]
})

coords = ("time", "latitude", "longitude")
data_vars = {
    ngfbc: (coords, sum_total_emisi_percrops[i]) for i, ngfbc in enumerate(gfrac_ngfbfc)
}
sum_total_emisi_percrops_netcdf = sum_total_emisi_percrops_netcdf.assign(data_vars)
sum_total_emisi_percrops_netcdf

sum_total_emisi_percrops_netcdf.to_netcdf("/vol/milkunarc/cadlan/stream_2/Step3/AM4/sum_total_emisi_percrops_netcdf.nc", mode="w", format="NETCDF4")

import xarray
import numpy as np
import pandas as pd

sum_total_emisi_percrops_netcdf = xarray.open_dataset("/vol/milkunarc/cadlan/stream_2/Step3/AM4/sum_total_emisi_percrops_netcdf.nc", engine="netcdf4")
country_code = pd.read_excel("/vol/milkunC/achaidir/Country Grids/ISO-3166-Country-Code_Final.xlsx", engine="openpyxl")
gfrac_ngfbfc = [element.strip() for element in GFRAC.coords['NGFBFC'].data.astype('str').tolist()]
df = sum_total_emisi_percrops_netcdf.isel(time=0).to_dataframe()
table = pd.pivot_table(df, values=gfrac_ngfbfc, index=["country"], columns=['time'], aggfunc="sum", fill_value=0)
df_index = table.stack(level=0)
data = pd.to_datetime(df_index.columns, format='%d/%m/%Y %H.%M.%S')
df_index.columns = data.year
df_index = df_index.reset_index()
df_index.rename(columns={'level_1': "type"}, inplace=True)
df_index_merge = pd.merge(left=df_index, right=country_code, left_on='country', right_on='ISO Country')
df_index_merge = df_index_merge.replace([np.inf, -np.inf], np.nan)
df_index_merge = df_index_merge.fillna(0)                       
df_index_merge = df_index_merge[['IMAGE Region Name', 'type', 2020]]
df_index_merge['type'] = df_index_merge['type'].str.lower()
df_index_merge.head()
df_index_merge[df_index_merge['IMAGE Region Name'] == "Korea"]
df_index_merge.to_excel("/vol/milkunarc/cadlan/stream_2/Step3/AM4/emisi_total_per_crops_AM4.xlsx", index=False)