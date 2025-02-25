import os
import re
import glob
import xarray
import rasterio
import rioxarray
import subprocess
import numpy as np
import pandas as pd
import netCDF4 as nc
import seaborn as sns
import multiprocessing as mp
import matplotlib.pyplot as plt

from functools import reduce
from collections import Counter
from osgeo import gdal, osr, ogr
from osgeo.gdal import InfoOptions
from rasterio.features import shapes
from geopy.geocoders import Nominatim
#from pyramids_gis.pyramids.dataset import Dataset
from numpy.lib.stride_tricks import sliding_window_view, as_strided

# Explorasi Data LUH V2
## Read Data
### States
path_LUH2 = '/vol/milkunC/achaidir/LUH2 2022/states.nc'
luh2_states = xarray.open_dataset(path_LUH2, engine="netcdf4", decode_times=False)
luh2_states_worldwide = luh2_states.isel(time=slice(1000, 1173))
luh2_states_worldwide['time'] = pd.date_range(start="1850-01-01", end="2022-01-01", freq='YS')

luh2_added_states_worldwide = xarray.open_dataset("/vol/milkunC/achaidir/LUH2 2022/multiple-states_input4MIPs_landState_ScenarioMIP_UofMD-IMAGE-ssp119-2-1-f_gn_2015-2100.nc",\
                                        engine="netcdf4", decode_times=False)
luh2_added_states_worldwide = luh2_added_states_worldwide.drop_vars(['lat_bounds', 'lon_bounds', 'time_bnds'])
luh2_added_states_worldwide.coords['time'] = pd.date_range(start='2015-01-01', end='2100-01-01', freq='YS')

"""### Static"""
path_static = path_LUH2 = '/vol/milkunC/achaidir/LUH2 2022/staticData_quarterdeg.nc'
luh2_static = xarray.open_dataset(path_static, engine="netcdf4")

"""## Carbon Density"""
# path_GBIOMASS="/vol/milkunC/achaidir/IMAGE PBL/SSP2/GBIOMASS.NC"
# gbiomass_5min_before = xarray.open_dataset(path_GBIOMASS, engine="netcdf4")
# gbiomass_5min = gbiomass_5min_before.fillna(0.0)

"""## Area Fraction"""

# path_GLCT="/vol/milkunC/achaidir/IMAGE PBL/SSP2/GLCT.NC"
# glct = xarray.open_dataset(path_GLCT, engine="netcdf4")

nbp = ['stems', 'branches', 'leaves', 'roots', 'litter', 'humus', 'charcoal']

#crops
gbiomass15mins = xarray.open_dataset("/vol/milkunarc/cadlan/perbaikan/step1/GLCT1-AGRICULTURE-BIOMASS-RESAMPLE-1970-2100_run2.nc", engine="netcdf4")


"""#### Old"""
# Agriculture
agri_stems_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
agri_branch_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
agri_leaves_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
agri_roots_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
agri_litter_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
agri_humus_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
agri_charcoal_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")

#### New

# Agriculture
new_agri_stems_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
new_agri_branch_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
new_agri_leaves_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
new_agri_roots_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
new_agri_litter_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
new_agri_humus_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")
new_agri_charcoal_gbiomass15min = np.zeros((27, 720, 1440), dtype="float64")


"""#### For Agriculture"""

for time in range(27):
        agri_stems_gbiomass15min[time] = np.where(gbiomass15mins.sel(NBP="stems").isel(time=time)['agriculture'].to_numpy() != np.nan,\
                                (gbiomass15mins.sel(NBP="stems").isel(time=time)['agriculture'].to_numpy()),0.0)

        agri_branch_gbiomass15min[time] = np.where(gbiomass15mins.sel(NBP="branches").isel(time=time)['agriculture'].to_numpy() != np.nan,\
                                (gbiomass15mins.sel(NBP="branches").isel(time=time)['agriculture'].to_numpy()),0.0)
        
        agri_leaves_gbiomass15min[time] = np.where(gbiomass15mins.sel(NBP="leaves").isel(time=time)['agriculture'].to_numpy() != np.nan,\
                                (gbiomass15mins.sel(NBP="leaves").isel(time=time)['agriculture'].to_numpy()),0.0)

        agri_roots_gbiomass15min[time] = np.where(gbiomass15mins.sel(NBP="roots").isel(time=time)['agriculture'].to_numpy() != np.nan,\
                                (gbiomass15mins.sel(NBP="roots").isel(time=time)['agriculture'].to_numpy()),0.0)
        
        agri_litter_gbiomass15min[time] = np.where(gbiomass15mins.sel(NBP="litter").isel(time=time)['agriculture'].to_numpy() != np.nan,\
                                (gbiomass15mins.sel(NBP="litter").isel(time=time)['agriculture'].to_numpy()),0.0)
        
        agri_humus_gbiomass15min[time] = np.where(gbiomass15mins.sel(NBP="humus").isel(time=time)['agriculture'].to_numpy() != np.nan,\
                                (gbiomass15mins.sel(NBP="humus").isel(time=time)['agriculture'].to_numpy()),0.0)
        
        agri_charcoal_gbiomass15min[time] = np.where(gbiomass15mins.sel(NBP="charcoal").isel(time=time)['agriculture'].to_numpy() != np.nan,\
                                (gbiomass15mins.sel(NBP="charcoal").isel(time=time)['agriculture'].to_numpy()),0.0)

"""##### Agriculture for C3ann"""

total_area_stems = np.zeros((720, 1440), dtype="float64")
total_area_branch = np.zeros((720, 1440), dtype="float64")
total_area_leaves = np.zeros((720, 1440), dtype="float64")
total_area_roots = np.zeros((720, 1440), dtype="float64")
total_area_litter = np.zeros((720, 1440), dtype="float64")
total_area_humus = np.zeros((720, 1440), dtype="float64")
total_area_charcoal = np.zeros((720, 1440), dtype="float64")

crops_list = ["c3ann", "c4ann", "c3per", "c4per", "c3nfx","pastr"]

time_list = [dt.strftime('%Y-%m-%d') for dt in pd.date_range(start="1970-01-01", end="2100-01-01", freq='5YS')]

for i,time in enumerate(time_list):
    for crop in crops_list:
    #stems    
        if (i<10):
            total_area_stems += luh2_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
        else:
            total_area_stems += luh2_added_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
    if (i<10):
        new_agri_stems_gbiomass15min = np.nan_to_num(luh2_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_stems) * agri_stems_gbiomass15min
    else:
        new_agri_stems_gbiomass15min = np.nan_to_num(luh2_added_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_stems) * agri_stems_gbiomass15min

    for crop in crops_list:
    #branch    
        if (i<10):
            total_area_branch += luh2_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
        else:
            total_area_branch += luh2_added_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
    if (i<10):
        new_agri_branch_gbiomass15min = np.nan_to_num(luh2_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_branch) * agri_branch_gbiomass15min
    else:
        new_agri_branch_gbiomass15min = np.nan_to_num(luh2_added_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_branch) * agri_branch_gbiomass15min

    for crop in crops_list:
    #leaves    
        if (i<10):
            total_area_leaves += luh2_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
        else:
            total_area_leaves += luh2_added_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
    if (i<10):
        new_agri_leaves_gbiomass15min = np.nan_to_num(luh2_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_leaves) * agri_leaves_gbiomass15min
    else:
        new_agri_leaves_gbiomass15min = np.nan_to_num(luh2_added_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_leaves) * agri_leaves_gbiomass15min

    for crop in crops_list:
    #roots    
        if (i<10):
            total_area_roots += luh2_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
        else:
            total_area_roots += luh2_added_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
    if (i<10):
        new_agri_roots_gbiomass15min = np.nan_to_num(luh2_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_roots) * agri_roots_gbiomass15min
    else:
        new_agri_roots_gbiomass15min = np.nan_to_num(luh2_added_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_roots) * agri_roots_gbiomass15min

    for crop in crops_list:
    #litter    
        if (i<10):
            total_area_litter += luh2_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
        else:
            total_area_litter += luh2_added_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
    if (i<10):
        new_agri_litter_gbiomass15min = np.nan_to_num(luh2_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_litter) * agri_litter_gbiomass15min
    else:
        new_agri_litter_gbiomass15min = np.nan_to_num(luh2_added_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_litter) * agri_litter_gbiomass15min

    for crop in crops_list:
    #humus    
        if (i<10):
            total_area_humus += luh2_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
        else:
            total_area_humus += luh2_added_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
    if (i<10):
        new_agri_humus_gbiomass15min = np.nan_to_num(luh2_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_humus) * agri_humus_gbiomass15min
    else:
        new_agri_humus_gbiomass15min = np.nan_to_num(luh2_added_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_humus) * agri_humus_gbiomass15min

    for crop in crops_list:
    #charcoal    
        if (i<10):
            total_area_charcoal += luh2_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
        else:
            total_area_charcoal += luh2_added_states_worldwide.sel(time=time)[crop].to_numpy()*luh2_static['carea'].to_numpy()
    if (i<10):
        new_agri_charcoal_gbiomass15min = np.nan_to_num(luh2_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_charcoal) * agri_charcoal_gbiomass15min
    else:
        new_agri_charcoal_gbiomass15min = np.nan_to_num(luh2_added_states_worldwide.sel(time=time)['c3ann'].to_numpy()*luh2_static['carea'].to_numpy()/total_area_charcoal) * agri_charcoal_gbiomass15min

new_expand_agri_stems = np.expand_dims(new_agri_stems_gbiomass15min, axis=-1)
new_expand_agri_branch = np.expand_dims(new_agri_branch_gbiomass15min, axis=-1)
new_expand_agri_leaves = np.expand_dims(new_agri_leaves_gbiomass15min, axis=-1)
new_expand_agri_roots = np.expand_dims(new_agri_roots_gbiomass15min, axis=-1)
new_expand_agri_litter = np.expand_dims(new_agri_litter_gbiomass15min, axis=-1)
new_expand_agri_humus = np.expand_dims(new_agri_humus_gbiomass15min, axis=-1)
new_expand_agri_charcoal = np.expand_dims(new_agri_charcoal_gbiomass15min, axis=-1)

new_agri_resample = np.concatenate([new_expand_agri_stems, new_expand_agri_branch, new_expand_agri_leaves,
                                    new_expand_agri_roots, new_expand_agri_litter, new_expand_agri_humus,
                                    new_expand_agri_charcoal], axis=-1)

agri_resample = xarray.Dataset({
        "agriculture":(["time", "lat", "lon", "NBP"], new_agri_resample)
    },
    coords={
        "lon":luh2_states_worldwide.coords["lon"].to_numpy(),
        "lat":luh2_states_worldwide.coords["lat"].to_numpy(),
        "time":pd.date_range("1970-01-01", "2100-01-01", freq='5YS'),
        "NBP":nbp
    })

agri_resample.to_netcdf("/vol/milkunarc/cadlan/perbaikan/step2/AGRICULTURE-C3ANN-BIOMASS-RESAMPLE-1970-2100_run4.nc", mode='w', format="NETCDF4")