import xarray
import numpy as np
import pandas as pd

# ### States
path_LUH2 = 'D:/kerja/asisten riset/vol/milkunC/achaidir/LUH2 2022/states.nc'
luh2_states = xarray.open_dataset(path_LUH2, engine="netcdf4", decode_times=False)
luh2_states_worldwide = luh2_states.isel(time=slice(1000, 1173))
luh2_states_worldwide['time'] = pd.date_range(start="1850-01-01", end="2022-01-01", freq='YS')

# ### Transition
transition_LUH2 = 'D:/kerja/asisten riset/vol/milkunC/achaidir/LUH2 2022/transitions.nc'
luh2_trans = xarray.open_dataset(transition_LUH2, engine="netcdf4", decode_times=False)
luh2_trans_worldwide = luh2_trans.isel(time=slice(1000, 1173))
luh2_trans_worldwide['time'] = pd.date_range(start='1850-01-01', end='2021-01-01', freq='YS')
luh2_trans_worldwide = luh2_trans_worldwide.drop(['primf_bioh', 'primn_bioh','secmf_bioh','secyf_bioh','secnf_bioh', "urban_to_secdf", "urban_to_secdn","urban_to_c3ann","urban_to_c4ann", "urban_to_c3per",
    "urban_to_c4per", "urban_to_c3nfx", "urban_to_pastr", "c3ann_to_urban", "c4ann_to_urban", "c3per_to_urban", "c4per_to_urban", "c3nfx_to_urban", "pastr_to_urban", "range_to_urban"])

crop_class = pd.read_excel("D:/kerja/asisten riset/vol/milkunC/achaidir/FAO/Crop Classification_latest.xlsx", engine="openpyxl", skiprows=1)
crop_class = crop_class.drop('Unnamed: 0', axis=1)
crop_class.rename(columns={'FAO Crops': 'Item'}, inplace=True)

fao_stat = pd.read_excel("D:/kerja/asisten riset/vol/milkunC/achaidir/FAO/fao_all_data.xlsx", engine="openpyxl")
fao_stat.dropna(axis=0, inplace=True)
fao_stat_area_harv = fao_stat[fao_stat['Element']=="Area harvested"]
fao_stat_area_harv.insert(9, 'True Value', fao_stat_area_harv['Value']/100, True)
faostat_area_harvested = fao_stat_area_harv.merge(crop_class, on='Item')

gfrac = xarray.open_dataset("D:/kerja/asisten riset/vol/milkunarc/cadlan/GFRAC_GRAPC_Interpolation/GFRAC-RESAMPLE-NEW-1970-2100.nc", engine='netcdf4')
gfrac_ngfbfc = gfrac.coords['NGFBFC'].data.tolist()

static = 'D:/kerja/asisten riset/vol/milkunC/achaidir/LUH2 2022/staticData_quarterdeg.nc'
luh2_static = xarray.open_dataset(static, engine="netcdf4")

management_luh = xarray.open_dataset('D:/kerja/asisten riset/vol/milkunC/achaidir/LUH2 2022/management.nc', engine="netcdf4", decode_times=False)

country_code = pd.read_excel("D:/kerja/asisten riset/vol/milkunC/achaidir/LUH2 2022/ISO-3166-Country-Code.xlsx", engine="openpyxl")
ccode_iso = list(country_code['country-code'])
cname_iso = list(country_code['name'])

ccode_dict = {}
for i, ccode in enumerate(ccode_iso):
    ccode_dict[ccode] = cname_iso[i]
    
ccode_worldwide_int = luh2_static['ccode'].to_numpy().astype('int64')
ccode_convert = np.zeros((720, 1440), dtype="<U64")

worldwide_intensity = np.zeros((131, 27, 7, 720, 1440))

for i in range(720):
    for j in range(1440):
        if ccode_worldwide_int[i][j] in ccode_dict.keys():
            ccode_convert[i][j] = ccode_dict[ccode_worldwide_int[i][j]]
        else:
            ccode_convert[i][j] = "Unknown"

ccode_convert_v1 = np.unique(ccode_convert).astype(str)

luh2_trans_worldwide_list = []
for luh2_trans in luh2_trans_worldwide.data_vars:
        luh2_trans_worldwide_list.append(luh2_trans[9:])


def grid_check_process(grid_luh, grid_gfrac):

    min_year = faostat_area_harvested['Year'].min()
    max_year = faostat_area_harvested['Year'].max() 

    years = list(np.linspace(min_year, max_year, max_year + 1, dtype="int64"))

    # kondisi A if: grid 1,1 LUH2 exist = 1 variable & grid 1,1 GFRAC exist=1 variable
    # then emisi grid 1,1 dibagi oleh value GFRAC grid 1,1
    if len(grid_luh) == 1 and len(grid_gfrac) == 1:
        emisi_step3 = 0
        # grid_gfrac = np.array([(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac) &
        #                                         (faostat_area_harvested['Year'] == year) &
        #                                         (faostat_area_harvested['Area'] == country) &
        #                                         (faostat_area_harvested['LUH2Class'] == grid_luh[9:])]['True Value'])])
        
        grid_emisi = emisi_step3/grid_gfrac # emisi nilai dari step 3
        grid_intensity = np.array(grid_emisi) # -> save to array baru np zerros

    # Kondisi B - if: grid 1,1 LUH2 exist = 1 variable & grid 1,1 GFRAC exist>1 variable (minimal 2)
    # then emisi grid 1,1 dibagi oleh value GFRAC grid 1,1
    elif len(grid_luh) == 1 and len(grid_gfrac) > 1:
        # for loop jml_grid_vars in grid_gfrac.data_vars # ditahun ini
        # luas_aktual_ngfbfc = jml_grid_var * dm
        # luas aktual Temperate oil crops di grid 1,1 = 0.025 * 5184 km2 = 129 km2 
        corps =  np.array([(faostat_area_harvested[(faostat_area_harvested['Year'] == year) & 
                                                   (faostat_area_harvested['Area'] == country) &
                                                   (faostat_area_harvested['LUH2Class'] == grid_luh[9:])]['IMAGE Classification']),
                            (faostat_area_harvested[(faostat_area_harvested['Year'] == year) & 
                                                    (faostat_area_harvested['Area'] == country) &
                                                    (faostat_area_harvested['LUH2Class'] == grid_luh[9:])]['True Value'])])
        
        corps_sum =  np.array([(faostat_area_harvested[(faostat_area_harvested['Year'] == year) & 
                                                    (faostat_area_harvested['Area'] == country) &
                                                    (faostat_area_harvested['LUH2Class'] == grid_luh[9:])]['True Value'].sum())])
        for x in range(0, len(corps[0])):
            luas_aktual_grid_gfrac = np.array([corps[0][x], corps[1][x], (corps[1][x] * luh2_static['carea'].isel(time=1, lat=1, lon=1))]) 
            emisi_step3 = 0 
            grid_intensity = (luas_aktual_grid_gfrac[1][x] / corps_sum * 100) * emisi_step3 

    #Kondisi C
    #grid 1,1 LUH2>1 variable, & GFRAC=1 variabel
    elif len(grid_luh) > 1 and len(grid_gfrac) == 1:
        for year in years:
            for country in list(np.unique(ccode_convert)):
                for items in grid_luh:
                    corps = np.array([(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac[0]) &
                                        (faostat_area_harvested['Year'] == year) &
                                        (faostat_area_harvested['Area'] == country) &
                                        (faostat_area_harvested['LUH2Class'] == items[9:])]['Item']),
                                        
                                        (faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac[0]) &
                                        (faostat_area_harvested['Year'] == year) &
                                        (faostat_area_harvested['Area'] == country) &
                                        (faostat_area_harvested['LUH2Class'] == items[9:])]['True Value'])])

                    corps_sum = np.array(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac[0]) &
                                        (faostat_area_harvested['Year'] == year) &
                                        (faostat_area_harvested['Area'] == country) &
                                        (faostat_area_harvested['LUH2Class'] == items[9:])]['True Value'].sum())
                    
                    intensity_corps = []
                    if corps.any() > 0:
                        for x in range(0, len(corps)): 
                            corps_proportion = np.array([corps[0][x], corps[1][x], ((corps[1][x]/corps_sum)*100)])   

                            #step 3   
                            emisi_to_c3ann = 35  
                            intensity_corps.append(emisi_to_c3ann * corps_proportion[2])

                # step 3
                #note contoh -> nilai dari step 3 * proporsi sesuai nilai corpsnya
                # emisi_to_c3ann = 35
                # intensity_c3_annual = emisi_to_c3ann * proportion_crops_c3annual


        # c4_annual = np.array(np.where(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac) & (faostat_area_harvested['photosynthesis pathway'] == "C4") &
        # (faostat_area_harvested['Monfreda annual/ perennial'] == "annual")], country, grid_luh))
        # sum_c4_annual = np.array(np.where(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac) & (faostat_area_harvested['photosynthesis pathway'] == "C4") &
        # (faostat_area_harvested['Monfreda annual/ perennial'] == "annual")], country, grid_luh)).sum()
        # proportion_crops_c3annual = (c4_annual/sum_c4_annual) * 100
        
        # sum_c3_parrenial = np.array(np.where(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac) & (faostat_area_harvested['photosynthesis pathway'] == "C3") &
        # (faostat_area_harvested['Monfreda annual/ perennial'] == "perennial")], country, grid_luh)).sum()
        
        # sum_c4_parrenial = np.array(np.where(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac) & (faostat_area_harvested['photosynthesis pathway'] == "C4") &
        # (faostat_area_harvested['Monfreda annual/ perennial'] == "perennial")], country, grid_luh)).sum()
        
        # sum_c3_nfx = np.array(np.where(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac) & (faostat_area_harvested['photosynthesis pathway'] == "C3") &
        # (faostat_area_harvested['Monfreda annual/ perennial'] == "nfx")], country, grid_luh)).sum()

        # tabel 2 - proporsi

    
    #kondisi D1
    elif len(grid_gfrac) > 1 and len(grid_luh) > 1:
        # tiap var grid gfrac
        for country in list(np.unique(ccode_convert)):
            for items in grid_luh:
                for year in years:
                    corps = np.array([(faostat_area_harvested[(faostat_area_harvested['Year'] == year) &
                                        (faostat_area_harvested['Area'] == country) &
                                        (faostat_area_harvested['LUH2Class'] == items[9:])]['IMAGE Classification']),
                                    (faostat_area_harvested[(faostat_area_harvested['Year'] == year) &
                                        (faostat_area_harvested['Area'] == country) &
                                        (faostat_area_harvested['LUH2Class'] == items[9:])]['Item']),
                                    (faostat_area_harvested[(faostat_area_harvested['Year'] == year) &
                                        (faostat_area_harvested['Area'] == country) &
                                        (faostat_area_harvested['LUH2Class'] == items[9:])]['True Value'])])
                    
                    corps_sum = np.array(faostat_area_harvested[(faostat_area_harvested['Year'] == year) &
                                    (faostat_area_harvested['Area'] == country) &
                                    (faostat_area_harvested['LUH2Class'] == items[9:])]['True Value'].sum())
                    
                    for x in range(0, len(corps[0])):
                        corps_proportion = np.array([corps[0][x], corps[1][x], corps[2][x], ((corps[2][x]/corps_sum)*100)])
                    
                        #step 3
                        # emisi_to_c3ann = 35  
                        # intensity_c3_annual[cty_idx] = emisi_to_c3ann * corps_proportion[3]

        # loop_value = 0
        # list_grid = ['TOC', 'TRT']
        # while len(grid_gfrac) > loop_value:
        #     grid_ngfbfc = np.array(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac) &
        #                                                            (faostat_area_harvested['Year'] == years) &
        #                                                            (faostat_area_harvested['Area'] == country) &
        #                                                            (faostat_area_harvested['LUH2Class'] == 'corps')]['True Value'].sum())
        #     list_grid.append(grid_ngfbfc)

        #     loop_value += 1
        
        # sum_list_grid = list_grid.sum()

        # for i in range(0, len(list_grid)):
        #     proportion_crops_c3ann = (list_grid[i]/sum_list_grid) * 100
            
        #     # note contoh -> nilai dari step 3 * proporsi sesuai nilai corpsnya
            # emisi_to_c3ann = 35
            # intensity_c3_annual = emisi_to_c3ann * proportion_crops_c3ann

            # grid_intensity_c3_annual = np.array(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac) &
            #                                                        (faostat_area_harvested['Year'] == year) 
            #                                                        (faostat_area_harvested['photosynthesis pathway'] == "C3") &
            #                                                        (faostat_area_harvested['Monfreda annual/ perennial'] == "annual") &
            #                                                        (faostat_area_harvested['Area'] == country) &
            #                                                        (faostat_area_harvested['LUH2Class'] == 'corps')]).sum()
        # D2
        years = list(np.linspace(1970, 2025, 56, dtype="int64"))
        proporsi = list()
        list_mayor_grid = ['Oil, palm fruit', 'Wheat', 'Soybeans', 'Maize', 'Rice']
        for mayor_grid in list_mayor_grid:
                c3ann = np.array([(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] != mayor_grid) &
                                (faostat_area_harvested['Year'] == 2017) &
                                (faostat_area_harvested['Area'] == 'Ghana') &
                                (faostat_area_harvested['LUH2Class'] == 'c3per')]['IMAGE Classification']),

                                (faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] != mayor_grid) &
                                (faostat_area_harvested['Year'] == 2017) &
                                (faostat_area_harvested['Area'] == 'Ghana') &
                                (faostat_area_harvested['LUH2Class'] == 'c3per')]['Item']),
                                
                                (faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] != mayor_grid) &
                                (faostat_area_harvested['Year'] == 2017) &
                                (faostat_area_harvested['Area'] == 'Ghana') &
                                (faostat_area_harvested['LUH2Class'] == 'c3per')]['True Value'])])

                c3ann_sum = np.array(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] != mayor_grid) &
                                (faostat_area_harvested['Year'] == 2017) &
                                (faostat_area_harvested['Area'] == 'Ghana') &
                                (faostat_area_harvested['LUH2Class'] == 'c3per')]['True Value'].sum())

                for x in range(0, len(c3ann[0])):
                        corps_proportion = np.array([c3ann[0][x], c3ann[1][x], c3ann[2][x], ((c3ann[2][x]/c3ann_sum)*100)])
                        proporsi.append(float(corps_proportion[3]))
        
    return np.array(grid_intensity)



# proses calculation
# def grid_process(time, lat, lon):
    
#     grid_luh = luh2_trans_worldwide.isel(time=time).sel(lat=lat, lon=lon) # nearest tidak usah
#     grid_output = grid_luh.to_pandas()
#     hasil_luh= np.where(grid_output > 0, np.nan, grid_luh.data_vars)
#     grid_luh = [value for value in hasil_luh if not np.isreal(value) and value != 'nan']

#     grid_gfrac = gfrac['GFRAC_res'].isel(time=time).sel(lat=lat, lon=lon)
#     grid_gfrac_output = grid_gfrac.to_pandas()
#     hasil_gfrac = np.where(grid_gfrac_output.values > 0.000000, grid_gfrac.coords['NGFBFC'], grid_gfrac_output)
#     grid_gfrac = [value for value in hasil_gfrac if not np.isreal(value) and value != '0.0']

#     return grid_check_process(grid_luh, grid_gfrac)


# def grid_process_first(crop_luh2class):
#     crop_class_any = [] # -> (720, 53, 1, 1, 1440)
#     for lat in range(720):
#         for lon in range(1440):
#             crop_class_any.append(grid_process(time=, lat=lat, lon=lon))

#     return np.array(crop_class_any)


# for cty_idx, country in enumerate(list(np.unique(ccode_convert))):
#     worldwide_intensity.append([])
#     for crop_class_luh2 in luh2_trans_worldwide_list:
#         worldwide_intensity[cty_idx].append(grid_process_first(crop_class_luh2))

worldwide_intensity_arr = np.array(worldwide_intensity)


# output
grid_intensity_netcdf = xarray.Dataset({
        "grid_interp":(["country", 'items', "time", "lat", "lon"], worldwide_intensity_arr)
    },
    coords={
        "country": list(np.unique(ccode_convert)),
        'Items': luh2_trans_worldwide_list,
        "time":pd.date_range("1970-01-01", "2100-01-01", freq='YS'),
        "lon":luh2_states_worldwide.coords["lon"].to_numpy(),
        "lat":luh2_states_worldwide.coords["lat"].to_numpy()
    })


grid_intensity_netcdf.to_netcdf("D:/kerja/asisten riset/vol/milkunarc/cadlan/grid_intensity_netcdf-1970_2100.nc", mode='w', format="NETCDF4")



def grid_check_process(grid_luh, grid_gfrac, year, country):

    # kondisi A if: grid 1,1 LUH2 exist = 1 variable & grid 1,1 GFRAC exist=1 variable
    if len(grid_luh) == 1 and len(grid_gfrac) == 1:
        print("masuk A")
        if (faostat_area_harvested['LUH2Class'] == grid_luh[0][9:]).any():
            corps = np.array(faostat_area_harvested[(faostat_area_harvested['Area'] == country) & (faostat_area_harvested['Year'] == year) &
                                                    (faostat_area_harvested['Item'] == "Other fruits, n.e.c.") & 
                                                    (faostat_area_harvested['IMAGE Classification'] == f"{grid_gfrac[0]}") &
                                                    (faostat_area_harvested['LUH2Class'] == f'{grid_luh[0][9:]}')]['True Value'])
            
            gird_emisi = carbon_emission_1971[f'c_equ_{grid_luh[0][9:]}'].sel(lat=-7.125, lon=109.375).values / (corps * luh2_static['carea'].sel(lat=-7.125, lon=109.375).values)
            print(gird_emisi)


    # Kondisi B - if: grid 1,1 LUH2 exist = 1 variable & grid 1,1 GFRAC exist>1 variable (minimal 2)
    elif len(grid_luh) == 1 and len(grid_gfrac) > 1:
        print("masuk B")
        if (faostat_area_harvested['LUH2Class'] == grid_luh[0][9:]).any():
            for clss_gfrac in grid_gfrac:
                corps =  np.array([(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac[0]) &
                                                            (faostat_area_harvested['Year'] == year) &
                                                            (faostat_area_harvested['Area'] == country) &
                                                            (faostat_area_harvested['LUH2Class'] == grid_luh[0][9:])]['Item']),
                                    (faostat_area_harvested[(faostat_area_harvested['Year'] == year) & 
                                                            (faostat_area_harvested['IMAGE Classification'] == clss_gfrac) &
                                                            (faostat_area_harvested['Area'] == country) &
                                                            (faostat_area_harvested['LUH2Class'] == grid_luh[0][9:])]['IMAGE Classification']),
                                    (faostat_area_harvested[(faostat_area_harvested['Year'] == year) & 
                                                            (faostat_area_harvested['Area'] == country) &
                                                            (faostat_area_harvested['IMAGE Classification'] == clss_gfrac) &
                                                            (faostat_area_harvested['LUH2Class'] == grid_luh[0][9:])]['True Value'])])
                
                corps_sum =  np.array([(faostat_area_harvested[(faostat_area_harvested['Year'] == year) & 
                                                            (faostat_area_harvested['Area'] == country) &
                                                            (faostat_area_harvested['LUH2Class'] == grid_luh[0][9:])]['True Value'].sum())])
                for x in range(0, len(corps[0])):
                    luas_aktual_grid_gfrac = np.array([corps[0][x], corps[1][x], corps[2][x], (corps[2][x] * luh2_static['carea'].sel(lat=-7.125, lon=109.375).values), ((corps[2][x] / corps_sum) * 100)]) 
                    # emisi_intensity = (luas_aktual_grid_gfrac[3][0] * carbon_emission_1971[f'c_equ_{grid_luh[0][9:]}'].isel(time=1).sel(lat=-7.125, lon=109.375).values) / luas_aktual_grid_gfrac[1]
                    
                    print(corps[1][x], (carbon_emission_1971[f'c_equ_{grid_luh[0][9:]}'].sel(lat=-7.125, lon=109.375).values/(corps[2][x] * luh2_static['carea'].sel(lat=-7.125, lon=109.375).values)))
                    print(luas_aktual_grid_gfrac)

    #Kondisi C
    elif len(grid_luh) > 1 and len(grid_gfrac) == 1:
        print("masuk C")
        sum_proporsi = []
        tabel2 = []
        for items in grid_luh:
            corps = np.array([(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac[0]) &
                                (faostat_area_harvested['Year'] == year) &
                                (faostat_area_harvested['Area'] == country) &
                                (faostat_area_harvested['LUH2Class'] == items[9:])]['Item']),
                                
                                (faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac[0]) &
                                (faostat_area_harvested['Year'] == year) &
                                (faostat_area_harvested['Area'] == country) &
                                (faostat_area_harvested['LUH2Class'] == items[9:])]['True Value'])])

            corps_sum = np.array(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] == grid_gfrac[0]) &
                                (faostat_area_harvested['Year'] == year) &
                                (faostat_area_harvested['Area'] == country) &
                                (faostat_area_harvested['LUH2Class'] == items[9:])]['True Value'].sum())
            
            if len(corps[0]) > 1:
                for x in range(0, len(corps)):
                    corps_proportion = np.array([corps[0][x], corps[1][x], ((corps[1][x]/corps_sum)*100)])
                    # proporsi_carea = ((corps_sum/sum(ProporsitotalLUH2)) *  100)
                       
                    # emission_intensity = carbon_emission_1971[f'c_equ_{grid_luh[0][9:]}'].sel(lat=-7.125, lon=109.375).values/luh2_static['carea'].sel(lat=-7.125, lon=109.375).values
                    emission_intensity = carbon_emission_1971[f'c_equ_{grid_luh[0][9:]}'].sel(lat=-7.125, lon=109.375).values/(((corps[1][x]/corps_sum)*100) * gfrac['GFRAC_res'].isel(time=1).sel(lat=-7.125, lon=109.375).data.sum() *\
                                                                                                                                luh2_static['carea'].sel(lat=-7.125, lon=109.375).values)
                    print(corps_proportion, emission_intensity)
            else:
                sum_proporsi.append(corps_sum)
                #tabel 2
                tabel2.append([corps[0][0], corps[1][0], corps_sum, ((corps[1][0]/corps_sum) * 100)])
        
        for idx, i in enumerate(tabel2):
            # proporsi_carea = round(tabel2[idx][1]/sum(sum_proporsi), 2) * ((corps[1][0]/corps_sum) * 100)
            corps_proportion = np.array([tabel2[idx][0], tabel2[idx][1], tabel2[idx][3], tabel2[idx][1]/sum(sum_proporsi), tabel2[idx][1]/sum(sum_proporsi) * ((tabel2[idx][1]/tabel2[idx][2]) * 100)])
            print(corps_proportion)
        
    #kondisi D1 D2
    elif len(grid_gfrac) > 1 and len(grid_luh) > 1:
        print("masuk D")
        proporsi = list()
        list_mayor_grid = ['Oil, palm fruit', 'Wheat', 'Soybeans', 'Maize', 'Rice']
        for mayor_grid in list_mayor_grid:
                c3ann = np.array([(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] != mayor_grid) &
                                (faostat_area_harvested['Year'] == year) &
                                (faostat_area_harvested['Area'] == country) &
                                (faostat_area_harvested['LUH2Class'] == items[9:])]['IMAGE Classification']),

                                (faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] != mayor_grid) &
                                (faostat_area_harvested['Year'] == year) &
                                (faostat_area_harvested['Area'] == country) &
                                (faostat_area_harvested['LUH2Class'] == items[9:])]['Item']),
                                
                                (faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] != mayor_grid) &
                                (faostat_area_harvested['Year'] == year) &
                                (faostat_area_harvested['Area'] == country) &
                                (faostat_area_harvested['LUH2Class'] == items[9:])]['True Value'])])

                c3ann_sum = np.array(faostat_area_harvested[(faostat_area_harvested['IMAGE Classification'] != mayor_grid) &
                                (faostat_area_harvested['Year'] == year) &
                                (faostat_area_harvested['Area'] == country) &
                                (faostat_area_harvested['LUH2Class'] == items[9:])]['True Value'].sum())

                for x in range(0, len(c3ann[0])):
                        corps_proportion = np.array([c3ann[0][x], c3ann[1][x], c3ann[2][x], ((c3ann[2][x]/c3ann_sum)*100)])
                        proporsi.append(float(corps_proportion[3]))
        return proporsi