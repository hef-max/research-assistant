import numpy as np
import xarray
import pandas as pd
np.seterr(divide='ignore', invalid='ignore')

max_year = 52
path_LUH2 = '/vol/milkunC/achaidir/LUH2 2022/states.nc'
luh2_states = xarray.open_dataset(path_LUH2, engine="netcdf4", decode_times=False)

luh2_states_worldwide = luh2_states.isel(time=slice(1120, 1173))
luh2_states_worldwide['time'] = pd.date_range(start="1970-01-01", end="2021-01-01", freq='YS')

## add the projection
luh2_added_states_worldwide = xarray.open_dataset("/vol/milkunC/achaidir/LUH2 2022/multiple-states_input4MIPs_landState_ScenarioMIP_UofMD-IMAGE-ssp119-2-1-f_gn_2015-2100.nc",\
                                        engine="netcdf4", decode_times=False)
luh2_added_trans = luh2_added_states_worldwide.drop_vars(['lat_bounds', 'lon_bounds', 'time_bnds'])
luh2_added_trans.coords['time'] = pd.date_range(start='2015-01-01', end='2100-01-01', freq='YS')

static = '/vol/milkunC/achaidir/LUH2 2022/staticData_quarterdeg.nc'
luh2_static = xarray.open_dataset(static, engine="netcdf4")

#open data selisih 1970-2021
carbon_emission_selisih = xarray.open_dataset("/vol/milkunarc/cadlan/Analysis3_LUC/CARBON_EQU_selisih_1970_2021.nc", engine="netcdf4")

#open active driver carbon density
c3ann_carbon_density = xarray.open_dataset("/vol/milkunarc/cadlan/Analysis2b_interpolate/CARBON-STOCK-CROP-C3ANN-INTERPOLATION-WORLDWIDE-1970_2100.nc", engine="netcdf4")
c4ann_carbon_density = xarray.open_dataset("/vol/milkunarc/cadlan/Analysis2b_interpolate/CARBON-STOCK-CROP-C4ANN-INTERPOLATION-WORLDWIDE-1970_2100.nc", engine="netcdf4")
c3per_carbon_density = xarray.open_dataset("/vol/milkunarc/cadlan/Analysis2b_interpolate/CARBON-STOCK-CROP-C3PER-INTERPOLATION-WORLDWIDE-1970_2100.nc", engine="netcdf4")
c4per_carbon_density = xarray.open_dataset("/vol/milkunarc/cadlan/Analysis2b_interpolate/CARBON-STOCK-CROP-C4PER-INTERPOLATION-WORLDWIDE-1970_2100.nc", engine="netcdf4")
c3nfx_carbon_density = xarray.open_dataset("/vol/milkunarc/cadlan/Analysis2b_interpolate/CARBON-STOCK-CROP-C3NFX-INTERPOLATION-WORLDWIDE-1970_2100.nc", engine="netcdf4")
range_carbon_density = xarray.open_dataset("/vol/milkunarc/cadlan/Analysis2b_interpolate/CARBON-STOCK-RANGE-INTERPOLATION-WORLDWIDE-1970_2100.nc", engine="netcdf4")
pastr_carbon_density = xarray.open_dataset("/vol/milkunarc/cadlan/Analysis2b_interpolate/CARBON-STOCK-PASTURE-INTERPOLATION-WORLDWIDE-1970_2100.nc", engine="netcdf4")

#general list
landuse_class_crop = ['c3ann', 'c4ann', 'c3per', 'c4per', 'c3nfx', 'pastr', 'range']
carstock_all = ['branches', 'stems', 'leaves', 'roots', 'litter', 'humus', 'charcoal']

#am2
#area (fraction * carea)
area_c3ann_2021 = np.zeros((1, 720, 1440), dtype="float32")
area_c4ann_2021 = np.zeros((1, 720, 1440), dtype="float32")
area_c3per_2021 = np.zeros((1, 720, 1440), dtype="float32")
area_c4per_2021 = np.zeros((1, 720, 1440), dtype="float32")
area_c3nfx_2021 = np.zeros((1, 720, 1440), dtype="float32")
area_pastr_2021 = np.zeros((1, 720, 1440), dtype="float32")
area_range_2021 = np.zeros((1, 720, 1440), dtype="float32")
area_all_2021 = np.zeros((1, 7, 720, 1440), dtype="float32")

#am2
#proporsi area (area crop/ total crop)
proporsi_area_c3ann_AM2 = np.zeros((1,720, 1440), dtype=float)
proporsi_area_c4ann_AM2 = np.zeros((1,720, 1440), dtype=float)
proporsi_area_c3per_AM2 = np.zeros((1,720, 1440), dtype=float)
proporsi_area_c4per_AM2 = np.zeros((1,720, 1440), dtype=float)
proporsi_area_c3nfx_AM2 = np.zeros((1,720, 1440), dtype=float)
proporsi_area_pastr_AM2 = np.zeros((1,720, 1440), dtype=float)
proporsi_area_range_AM2 = np.zeros((1,720, 1440), dtype=float)
total_proporsi_luas = np.zeros((1, 720, 1440), dtype=float)

#am2
#carbon emission (carbon emisi selisih 1970-2021 * proporsi area crop masing2)
carbon_emission_c3ann_AM2 = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_c4ann_AM2 = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_c3per_AM2 = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_c4per_AM2 = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_c3nfx_AM2 = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_pastr_AM2 = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_range_AM2 = np.zeros((1, 720, 1440), dtype="float32")
carbon_equ_all_am2 = np.zeros((1, 720, 1440), dtype="float32")

#area masing-masing LU active driver utk AM2 (khusus tahun akhir accounting 2021)
#index -1 means tahun terakhir
for index_corps, corps in enumerate(landuse_class_crop):
    #c3ann
    if index_corps == 0:
        area_c3ann_2021 += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=max_year-1).to_numpy())))
        area_all_2021[0][index_corps] += area_c3ann_2021[0]
    #c4ann
    elif index_corps == 1:
        area_c4ann_2021 += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=max_year-1).to_numpy())))
        area_all_2021[0][index_corps] += area_c4ann_2021[0]
    # c3per
    elif index_corps == 2:
        area_c3per_2021 += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=max_year-1).to_numpy())))
        area_all_2021[0][index_corps] += area_c3per_2021[0]
    #c4per
    elif index_corps == 3:
        area_c4per_2021 += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=max_year-1).to_numpy())))
        area_all_2021[0][index_corps] += area_c4per_2021[0]
    #c3nfx
    elif index_corps == 4:
        area_c3nfx_2021 += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=max_year-1).to_numpy())))
        area_all_2021[0][index_corps] += area_c3nfx_2021[0]
    #pastr
    elif index_corps == 5:
        area_pastr_2021 += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=max_year-1).to_numpy())))
        area_all_2021[0][index_corps] += area_pastr_2021[0]
    #range
    elif index_corps == 6:
        area_range_2021 += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=max_year-1).to_numpy())))
        area_all_2021[0][index_corps] += area_range_2021[0]

area_all_2021 = area_all_2021.reshape(7, 1, 720, 1440)

#Proporsi area
#c3ann
proporsi_area_c3ann_AM2 = np.nan_to_num(area_c3ann_2021 / area_all_2021[0])
total_proporsi_luas += proporsi_area_c3ann_AM2[0]
#c4ann
proporsi_area_c4ann_AM2 = np.nan_to_num(area_c4ann_2021 / area_all_2021[1])
total_proporsi_luas += proporsi_area_c4ann_AM2[0]
#c3per
proporsi_area_c3per_AM2 = np.nan_to_num(area_c3per_2021 / area_all_2021[2])
total_proporsi_luas += proporsi_area_c3per_AM2[0]
#c4per
proporsi_area_c4per_AM2 = np.nan_to_num(area_c4per_2021 / area_all_2021[3])
total_proporsi_luas += proporsi_area_c4per_AM2[0]
#c3nfx
proporsi_area_c3nfx_AM2 = np.nan_to_num(area_c3nfx_2021 / area_all_2021[4])
total_proporsi_luas += proporsi_area_c3nfx_AM2[0]
#pastr
proporsi_area_pastr_AM2 = np.nan_to_num(area_pastr_2021 / area_all_2021[5])
total_proporsi_luas += proporsi_area_pastr_AM2[0]
#range
proporsi_area_range_AM2 = np.nan_to_num(area_range_2021 / area_all_2021[6])
total_proporsi_luas += proporsi_area_range_AM2[0]

#Calculate the carbon emission
#c3ann
carbon_emission_c3ann_AM2 = np.multiply(carbon_emission_selisih, proporsi_area_c3ann_AM2[0]) 
carbon_equ_all_am2[0] += carbon_emission_c3ann_AM2[0]
#c4ann
carbon_emission_c4ann_AM2 = np.multiply(carbon_emission_selisih, proporsi_area_c4ann_AM2[0])
carbon_equ_all_am2[0] += carbon_emission_c4ann_AM2[0]
#c3per
carbon_emission_c3per_AM2 = np.multiply(carbon_emission_selisih, proporsi_area_c3per_AM2[0])
carbon_equ_all_am2[0] += carbon_emission_c3per_AM2[0]
#c4per
carbon_emission_c4per_AM2 = np.multiply(carbon_emission_selisih, proporsi_area_c4per_AM2[0])
carbon_equ_all_am2[0] += carbon_emission_c4per_AM2[0]
#c3nfx
carbon_emission_c3nfx_AM2 = np.multiply(carbon_emission_selisih, proporsi_area_c3nfx_AM2[0])
carbon_equ_all_am2[0] += carbon_emission_c3nfx_AM2[0]
#pastr
carbon_emission_pastr_AM2 = np.multiply(carbon_emission_selisih, proporsi_area_pastr_AM2[0])
carbon_equ_all_am2[0] += carbon_emission_pastr_AM2[0]
#range
carbon_emission_range_AM2 = np.multiply(carbon_emission_selisih, proporsi_area_range_AM2[0])
carbon_equ_all_am2[0] += carbon_emission_range_AM2[0]

#export to netcdf
carbon_equ_am2 = xarray.Dataset(
    coords={
        "lon": luh2_states_worldwide.coords["lon"].to_numpy(),
        "lat": luh2_states_worldwide.coords["lat"].to_numpy()
    }
)
data_vars = {
    "c_emision_c3ann_am2": (["lat", "lon"], carbon_emission_c3ann_AM2),
    "c_emision_c4ann_am2": (["lat", "lon"], carbon_emission_c4ann_AM2),
    "c_emision_c3per_am2": (["lat", "lon"], carbon_emission_c3per_AM2),
    "c_emision_c4per_am2": (["lat", "lon"], carbon_emission_c4per_AM2),
    "c_emision_c3nfx_am2": (["lat", "lon"], carbon_emission_c3nfx_AM2),
    "c_emision_pastr_am2": (["lat", "lon"], carbon_emission_pastr_AM2),
    "c_emision_range_am2": (["lat", "lon"], carbon_emission_range_AM2)
}
carbon_equ_am2_ntcdf = carbon_equ_am2.assign(data_vars)
carbon_equ_am2_ntcdf.to_netcdf("/vol/milkunarc/cadlan/Analysis3_LUC/CARBON_EMISSION-AM2_run1.nc", mode='w', format="NETCDF4") 

############################################

#AM3
#area active drivers atau occupation year (hectare/ year)
area_c3ann_2021_AM3 = np.zeros((max_year, 720, 1440), dtype="float32")
area_c4ann_2021_AM3 = np.zeros((max_year, 720, 1440), dtype="float32")
area_c3per_2021_AM3 = np.zeros((max_year, 720, 1440), dtype="float32")
area_c4per_2021_AM3 = np.zeros((max_year, 720, 1440), dtype="float32")
area_c3nfx_2021_AM3 = np.zeros((max_year, 720, 1440), dtype="float32")
area_pastr_2021_AM3 = np.zeros((max_year, 720, 1440), dtype="float32")
area_range_2021_AM3 = np.zeros((max_year, 720, 1440), dtype="float32")
akumulasi_area = np.zeros((max_year, 7, 720, 1440), dtype="float32")

#proporsi occupation year (area active driver/ akumulasi area active driver)
prop_occupyear_c3ann = np.zeros((max_year, 720, 1440), dtype="float32")
prop_occupyear_c4ann = np.zeros((max_year, 720, 1440), dtype="float32")
prop_occupyear_c3per = np.zeros((max_year, 720, 1440), dtype="float32")
prop_occupyear_c4per = np.zeros((max_year, 720, 1440), dtype="float32")
prop_occupyear_c3nfx = np.zeros((max_year, 720, 1440), dtype="float32")
prop_occupyear_pastr = np.zeros((max_year, 720, 1440), dtype="float32")
prop_occupyear_range = np.zeros((max_year, 720, 1440), dtype="float32")
prop_occupyear_all = np.zeros((max_year, 7, 720, 1440), dtype="float32")

#Persentase AM3
#am3
carbon_equ_c3ann_am3 = np.zeros((max_year, 720, 1440), dtype="float32")
carbon_equ_c4ann_am3 = np.zeros((max_year, 720, 1440), dtype="float32")
carbon_equ_c3per_am3 = np.zeros((max_year, 720, 1440), dtype="float32")
carbon_equ_c4per_am3 = np.zeros((max_year, 720, 1440), dtype="float32")
carbon_equ_c3nfx_am3 = np.zeros((max_year, 720, 1440), dtype="float32")
carbon_equ_pastr_am3 = np.zeros((max_year, 720, 1440), dtype="float32")
carbon_equ_range_am3 = np.zeros((max_year, 720, 1440), dtype="float32")
carbon_equ_all_am3 = np.zeros((max_year, 7, 720, 1440), dtype="float32")

#area active drivers (occupation year (hectare/year) each active drivers)
for time_idx in range(max_year - 1):
    for index_corps, corps in enumerate(landuse_class_crop):
            #c3ann
            if index_corps == 0:
                area_c3ann_2021_AM3[time_idx] += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=time_idx).to_numpy())))
                akumulasi_area[time_idx][index_corps] += area_c3ann_2021_AM3[time_idx]
            #c4ann
            elif index_corps == 1:
                area_c4ann_2021_AM3[time_idx] += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=time_idx).to_numpy())))
                akumulasi_area[time_idx][index_corps] += area_c4ann_2021_AM3[time_idx]
            #c3per
            elif index_corps == 2:
                area_c3per_2021_AM3[time_idx] += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=time_idx).to_numpy())))
                akumulasi_area[time_idx][index_corps] += area_c3per_2021_AM3[time_idx]
            #c4per
            elif index_corps == 3:
                area_c4per_2021_AM3[time_idx] += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=time_idx).to_numpy())))
                akumulasi_area[time_idx][index_corps] += area_c4per_2021_AM3[time_idx]
            #c3nfx
            elif index_corps == 4:
                area_c3nfx_2021_AM3[time_idx] += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=time_idx).to_numpy())))
                akumulasi_area[time_idx][index_corps] += area_c3nfx_2021_AM3[time_idx]
            #pastr
            elif index_corps == 5:
                area_pastr_2021_AM3[time_idx] += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=time_idx).to_numpy())))
                akumulasi_area[time_idx][index_corps] += area_pastr_2021_AM3[time_idx]
            #range
            elif index_corps == 6:
                area_range_2021_AM3[time_idx] += np.multiply.reduce((luh2_static['carea'].to_numpy(),
                                                            np.nan_to_num(luh2_states_worldwide[corps].isel(time=time_idx).to_numpy())))
                akumulasi_area[time_idx][index_corps] += area_range_2021_AM3[time_idx]

#Proporsi occupation year         
for time_idx in range(max_year - 1):
    for index_corps, corps in enumerate(landuse_class_crop):
            #c3ann
            if index_corps == 0:
                prop_occupyear_c3ann[time_idx] += np.nan_to_num(area_c3ann_2021_AM3[time_idx] / akumulasi_area[time_idx][index_corps])
                prop_occupyear_all[time_idx][index_corps] += prop_occupyear_c3ann[time_idx]
            
            elif index_corps == 1:
                #c4ann
                prop_occupyear_c4ann[time_idx] += np.nan_to_num(area_c4ann_2021_AM3[time_idx] / akumulasi_area[time_idx][index_corps])
                prop_occupyear_all[time_idx][index_corps] += prop_occupyear_c4ann[time_idx]
            elif index_corps == 2:
                # #c3per
                prop_occupyear_c3per[time_idx] += np.nan_to_num(area_c3per_2021_AM3[time_idx] / akumulasi_area[time_idx][index_corps])
                prop_occupyear_all[time_idx][index_corps] += prop_occupyear_c3per[time_idx]
            elif index_corps == 3:
            #c4per
                prop_occupyear_c4per[time_idx] += np.nan_to_num(area_c4per_2021_AM3[time_idx] / akumulasi_area[time_idx][index_corps])
                prop_occupyear_all[time_idx][index_corps] += prop_occupyear_c4per[time_idx]
            elif index_corps == 4:
            #c3nfx
                prop_occupyear_c3nfx[time_idx] += np.nan_to_num(area_c3nfx_2021_AM3[time_idx] / akumulasi_area[time_idx][index_corps])
                prop_occupyear_all[time_idx][index_corps] += prop_occupyear_c3nfx[time_idx]
            elif index_corps == 5:
            #pastr
                prop_occupyear_pastr[time_idx] += np.nan_to_num(area_pastr_2021_AM3[time_idx] / akumulasi_area[time_idx][index_corps])
                prop_occupyear_all[time_idx][index_corps] += prop_occupyear_pastr[time_idx]
            elif index_corps == 6:
            #range
                prop_occupyear_range[time_idx] += np.nan_to_num(area_range_2021_AM3[time_idx] / akumulasi_area[time_idx][index_corps])
                prop_occupyear_all[time_idx][index_corps] += prop_occupyear_range[time_idx]

#am3
#carbon emission
carbon_emission_c3ann_st = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_c4ann_st = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_c3per_st = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_c4per_st = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_c3nfx_st = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_pastr_st = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_range_st = np.zeros((1, 720, 1440), dtype="float32")
carbon_emission_st_AM3 = np.zeros((1, 7, 720, 1440), dtype="float32")

for time_idx in range(max_year - 1):
    for index_corps, corps in enumerate(landuse_class_crop):
        for carstock_idx, carstock_class in enumerate(carstock_all):
            if index_corps == 0:
                #c3ann
                carbon_emission_c3ann_st[0] = np.multiply(prop_occupyear_all[time_idx][index_corps], carbon_emission_selisih)
                carbon_emission_st_AM3[0][index_corps] += carbon_emission_c3ann_st[0]
            elif index_corps == 1:
                #c4ann
                carbon_emission_c4ann_st[0] = np.multiply(prop_occupyear_all[time_idx][index_corps], carbon_emission_selisih)
                carbon_emission_st_AM3[0][index_corps] += carbon_emission_c4ann_st[0]
            elif index_corps == 2:
                #c3per
                carbon_emission_c3per_st[0] = np.multiply(prop_occupyear_all[time_idx][index_corps], carbon_emission_selisih) 
                carbon_emission_st_AM3[0][index_corps] += carbon_emission_c3per_st[0]
            elif index_corps == 3:
                #c4per
                carbon_emission_c4per_st[0] = np.multiply(prop_occupyear_all[time_idx][index_corps], carbon_emission_selisih) 
                carbon_emission_st_AM3[0][index_corps] += carbon_emission_c4per_st[0]
            elif index_corps == 4:
                #c3nfx
                carbon_emission_c3nfx_st[0] = np.multiply(prop_occupyear_all[time_idx][index_corps], carbon_emission_selisih) 
                carbon_emission_st_AM3[0][index_corps] += carbon_emission_c3nfx_st[0]
            elif index_corps == 5:
                #pastr
                carbon_emission_pastr_st[0] = np.multiply(prop_occupyear_all[time_idx][index_corps], carbon_emission_selisih) 
                carbon_emission_st_AM3[0][index_corps] += carbon_emission_pastr_st[0]
            elif index_corps == 6:
                #range
                carbon_emission_range_st[0] = np.multiply(prop_occupyear_all[time_idx][index_corps], carbon_emission_selisih)
                carbon_emission_st_AM3[0][index_corps] += carbon_emission_range_st[0]

#reshape
carbon_emission_st_AM3 = carbon_emission_st_AM3.reshape(7, 1, 720, 1440)

#export netcdf
carbon_equ_am3 = xarray.Dataset(
    coords={
        "time": pd.to_datetime(['2021']),
        "lon": luh2_states_worldwide.coords["lon"].to_numpy(),
        "lat": luh2_states_worldwide.coords["lat"].to_numpy()
    }
)

data_vars = {
    "c_emision_c3ann_am3": (["time", "lat", "lon"], carbon_emission_st_AM3[0]),
    "c_emision_c4ann_am3": (["time", "lat", "lon"], carbon_emission_st_AM3[1]),
    "c_emision_c3per_am3": (["time", "lat", "lon"], carbon_emission_st_AM3[2]),
    "c_emision_c4per_am3": (["time", "lat", "lon"], carbon_emission_st_AM3[3]),
    "c_emision_c3nfx_am3": (["time", "lat", "lon"], carbon_emission_st_AM3[4]),
    "c_emision_pastr_am3": (["time", "lat", "lon"], carbon_emission_st_AM3[5]),
    "c_emision_range_am3": (["time", "lat", "lon"], carbon_emission_st_AM3[6])
}
carbon_equ_am3_ntcdf = carbon_equ_am3.assign(data_vars)
carbon_equ_am3_ntcdf.to_netcdf("/vol/milkunarc/cadlan/Analysis3_LUC/CARBON_EMISSION-AM3_run1.nc", mode='w', format="NETCDF4") 


