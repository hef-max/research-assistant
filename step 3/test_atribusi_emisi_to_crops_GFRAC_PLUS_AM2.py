import numpy as np
import pandas as pd
import xarray

emission_CO2_netcdf_AM2 = xarray.open_dataset("/vol/milkunarc/cadlan/stream_2/Step2/AM23/emission_CO2_AM23.nc", engine="netcdf4")
gfrac = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GFRAC_5_mins_combined_run2.NC", engine="netcdf4")
garea = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GAREA.NC", engine="netcdf4")
gfrac_list = gfrac.coords['NGFBFC'].data.tolist()

gfrac_ngfbfc = ['grass', 'wheat', 'rice', 'maize', 'tropical cereals', 'other temperate cereals', 'pulses', 'soybeans', 'temperate oil crops',
                'tropical oil crops', 'temperate roots & tubers', 'tropical roots & tubers', 'sugar crops', 'oil & palm fruit',
                'vegetables & fruits', 'other non-food & luxury & spices', 'plant based fibres']

garea_7020 = garea.isel(time=slice(0, 12))
garea_ha = np.nan_to_num(np.multiply(garea_7020['GAREA'].to_numpy(), 100))

gfrac_7020 = gfrac.isel(time=slice(0, 12)) 
gfrac_area = np.zeros((12, 17, 2160, 4320), dtype="float32")
for n in range (12):
    for i in range(17):
        gfrac_area[n][i] = np.nan_to_num(np.multiply(gfrac_7020['GFRAC_combined'].isel(time=n, NGFBFC=i), garea_ha[n]))

gfrac_area_netcdf = xarray.Dataset({
    "GFRAC_combined":(["time","NGFBFC","latitude", "longitude"], gfrac_area)
},coords={
        "time": pd.date_range(start='1970-01-01', end='2025-01-01', freq='5YS'),
        "NGFBFC" : gfrac_list,
        "latitude": gfrac.coords["latitude"].to_numpy(),
        "longitude": gfrac.coords["longitude"].to_numpy()
    })

#1975-2020
# gfrac_area_netcdf_atribusi = gfrac_area_netcdf.isel(time=slice(1, 12))

#catatan penting
#gfrac_area_netcdf untuk general purpose
#gfrac_area_netcdf_atribusi untuk proses atribusi saja, karena index 0 nya start di 1975 sama spt emisi (both LUC Crops and Agri to Agri)

#1 LUC Crops

def divide(arr1, arr2):
    return np.divide(arr1, arr2)

def multiple(arr1, arr2):
    return np.multiply(arr1, arr2)


'''
#Bagian yang perlu direvisi untuk AM3 ada di line 51 dan 58, lihat petunjuk setelah -->

#1A. Proporsi crops tahun 2020 (akhir saja) --> Bedanya untuk AM3 kita buat count year, akumulasi, dan proporsi
prop_crops_GFRAC = np.zeros((17, 1, 2160, 4320), dtype="float32")

for i, crop in enumerate(gfrac_list):
    prop_crops_GFRAC[i][0] = np.nan_to_num(divide(gfrac_area_netcdf['GFRAC_combined'].isel(time=10).sel(NGFBFC=crop).to_numpy(), 
                                                            gfrac_area_netcdf['GFRAC_combined'].isel(time=10).sum(dim='NGFBFC').to_numpy()))

#1B. Emisi per crops (emisi tahun 2020 x prop tahun 2020)--> Bedanya untuk AM3 kita buat emisi berdsarkan proporsi AM3
luc_crops_prop_crop = np.zeros((17, 1, 2160, 4320), dtype="float32")

for i, crop in enumerate(gfrac_list):
    luc_crops_prop_crop[i][0] = np.nan_to_num(multiple(emission_CO2_netcdf_AM2['LUC Crops Emission'].isel(time=0).to_numpy(), 
                                                       prop_crops_GFRAC[i][0]))

'''

#1C. Export emisi per crops dari kategori luc crops ke netcdf
luc_crops_prop_crop_netcdf = xarray.Dataset(
coords={
        "time": pd.to_datetime(["2020-01-01"]),
        "latitude": gfrac.coords["latitude"].to_numpy(),
        "longitude": gfrac.coords["longitude"].to_numpy()
    })

coords = ("time", "latitude", "longitude")
data_vars = {
    ngfbc: (coords, luc_crops_prop_crop[i]) for i, ngfbc in enumerate(gfrac_ngfbfc)
}
luc_crops_prop_crop_netcdf = luc_crops_prop_crop_netcdf.assign(data_vars)
luc_crops_prop_crop_netcdf.to_netcdf("/vol/milkunarc/cadlan/stream_2/Step3/AM2/crop_emission_from_agri2agri.NC", mode="w", format="NETCDF4")


#2Agri to Agri (untuk AM3 persis spt AM2)
#2A Menghitung selisihnya (delta) nya dulu
glct_trans_1st = xarray.open_dataset("/vol/milkunarc/cadlan/stream_2/Step1/AM23/glct_trans_1st_AM23.NC", engine="netcdf4")
selisih_gfrac = np.zeros((1, 17, 2160, 4320), dtype="float32")

for i, crop in enumerate(gfrac_list):
    selisih_gfrac[0][i] = np.where(glct_trans_1st['GLCT_1st'].isel(time=0).values == 'agri_to_agri',
                                                np.where(gfrac_area_netcdf['GFRAC_combined'].isel(time=10).sel(NGFBFC=crop).values > gfrac_area_netcdf['GFRAC_combined'].isel(time=0).sel(NGFBFC=crop).values,
                                                      gfrac_area_netcdf['GFRAC_combined'].isel(time=10).sel(NGFBFC=crop).values - gfrac_area_netcdf['GFRAC_combined'].isel(time=0).sel(NGFBFC=crop).values, 0), 0)

selisih_gfrac_area_netcdf = xarray.Dataset({
        "selisih_gfrac":(["time", "NGFBFC", "latitude", "longitude"], selisih_gfrac)
    },
    coords={
        "time": pd.to_datetime(["2020-01-01"]),
        "NGFBFC": list(gfrac_list),
        "latitude": gfrac.coords["latitude"].to_numpy(),
        "longitude": gfrac.coords["longitude"].to_numpy()
    })

#2B Menghitung proporsi dari selisih
proporsi_delta = np.zeros((1, 17, 2160, 4320), dtype="float32")

for i, crop in enumerate(gfrac_list):
        proporsi_delta[0][i] = np.nan_to_num(divide(selisih_gfrac_area_netcdf['selisih_gfrac'].isel(time=0).sel(NGFBFC=crop).to_numpy(), 
                                                    selisih_gfrac_area_netcdf['selisih_gfrac'].isel(time=0).sum(dim='NGFBFC').to_numpy()))

#2C Distribusi emisi sesuai delta
agri_to_agri_prop_crop = np.zeros((17, 1, 2160, 4320), dtype="float32")

agri_trans = emission_CO2_netcdf_AM2['Agricultural Transition Emission'].to_numpy()

for i, crop in enumerate(gfrac_list):
        agri_to_agri_prop_crop[i][0] = np.nan_to_num(multiple(agri_trans[0], proporsi_delta[0][i]))

#2D Export emisi per crops dari kategori agri2agri ke netcdf
agri2agri_prop_crop_netcdf = xarray.Dataset(
coords={
        "time": pd.to_datetime(["2020-01-01"]),
        "latitude": gfrac.coords["latitude"].to_numpy(),
        "longitude": gfrac.coords["longitude"].to_numpy()
    })

coords = ("time", "latitude", "longitude")
data_vars = {
    ngfbc: (coords, agri_to_agri_prop_crop[i]) for i, ngfbc in enumerate(gfrac_ngfbfc)
}
agri2agri_prop_crop_netcdf = agri2agri_prop_crop_netcdf.assign(data_vars)
agri2agri_prop_crop_netcdf.to_netcdf("/vol/milkunarc/cadlan/stream_2/Step3/AM2/crop_emission_from_agri2agri.NC", mode="w", format="NETCDF4")
