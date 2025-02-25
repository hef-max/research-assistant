
import numpy as np
import pandas as pd

#Step 1 data cleaning
fao_data = pd.read_csv("/vol/milkunC/achaidir/FAO/Production_Crops_Livestock_E_All_Data_1/Production_Crops_Livestock_E_All_Data.csv", delimiter=',', encoding='latin', low_memory=False)
df = fao_data[fao_data.columns[~fao_data.columns.str.contains('^Y.*[F|N]$')]]

df_long = pd.melt(df, id_vars=['Area Code', 'Area Code (M49)', 'Area', 'Item Code', 'Item Code (CPC)', 'Item', 'Element Code', 'Element', 'Unit'], 
                  var_name='Year', value_name='Value')
df_long['Year'] = df_long['Year'].str.replace('Y', '').astype(int)
df_long['Value'] = df_long['Value'].astype(float)
df_long['Value'] = df_long['Value'].fillna(0)
df_long.to_csv("/vol/milkunC/achaidir/FAO/Production_Crops_Livestock_E_All_Data_1/Production_Crops_Livestock_E_All_Data_analyze.csv", index=False)

fao_stat = pd.read_csv("/vol/milkunC/achaidir/FAO/Production_Crops_Livestock_E_All_Data_1/Production_Crops_Livestock_E_All_Data_analyze.csv")
crop_class = pd.read_excel("/vol/milkunC/achaidir/FAO/Crop Classification_latest.xlsx", engine="openpyxl", skiprows=1)
crop_class = crop_class.drop('Unnamed: 0', axis=1)
crop_class.rename(columns={'FAO Crops': 'Item'}, inplace=True)

fao_stat = fao_stat[~fao_stat["Area"].isin(["Americas", "Asia", "Australia and New Zealand", "Africa", "Belgium-Luxembourg", "Central America", "Central Asia", "Caribbean", "Czechoslovakia", "Eastern Africa", "Eastern Asia",
 "Eastern Europe", "European Union (27)","Land Locked Developing Countries", "Least Developed Countries", "Europe", "Low Income Food Deficit Countries", "Melanesia", "Middle Africa", 
  "Net Food Importing Developing Countries", "Northern Africa", "Northern America", "Northern Europe", "Oceania", "Polynesia", "Small Island Developing States", "Serbia and Montenegro",
  "South-eastern Asia", "Southern Africa", "Southern Asia", "Southern Europe", "Sudan (former)", "South America", "USSR", "Western Africa", "Western Asia", "Western Europe", "World", "Yugoslav SFR"])]

fao_stat_copy = fao_stat.copy()

prubahan_nama = {
    "China, Hong Kong SAR": "Hong Kong",
    "China, Macao SAR": "Macao", 
    "China, Taiwan Province of": "Taiwan, Province of China",
    "China, mainland": "China",
    "Democratic People's Republic of Korea": "Korea (Democratic People's Republic of)",
    "Democratic Republic of the Congo": "Congo, Democratic Republic of the",
    "Micronesia": "Micronesia (Federated States of)",
    "Micronesia (Federated States of) (Federated States of)": "Micronesia (Federated States of)",
    "Netherlands (Kingdom of the)": "Netherlands",
    "Republic of Korea": "Korea, Republic of",
    "Republic of Moldova": "Moldova, Republic of",
    "Palestine" : "Palestine, State of",
    "Türkiye": "Turkey",
    "United Republic of Tanzania": "Tanzania, United Republic of",
    "Ethiopia PDR" : "Eritrea"
}
keys_list = list(prubahan_nama.keys())
values_list = list(prubahan_nama.values())

for i in range(len(keys_list)):
    fao_stat_copy['Area'] = fao_stat_copy['Area'].astype(str).str.replace(keys_list[i], values_list[i])

#Step 2 agregasi item area harvested FAO dan create moving average 5 year
fao_stat_area_harv = fao_stat_copy[fao_stat_copy['Element'] == "Area harvested"]
fao_stat_area_harv = fao_stat_area_harv.fillna(0)
fao_stat_area_harv = fao_stat_area_harv.merge(crop_class, on='Item')

fao_stat_area_harv['IMAGE Classification'] = fao_stat_area_harv['IMAGE Classification'].apply(lambda x: x.replace('and', '&').replace('Plant-based fibers', 'Plant based fibres').replace('Oil, palm fruit', 'Oil & palm fruit').replace('Other non-food, luxury, spices', 'Other non-food & luxury & spices').replace('Vegetables or fruits', 'Vegetables & fruits'))

faostat_area_harvested_copy = fao_stat_area_harv.copy()
selected_rows = fao_stat_area_harv[fao_stat_area_harv['Year'] >= 1968]
selected_rows['Value'] = selected_rows.groupby(['Year', 'Area', 'IMAGE Classification'])['Value'].transform('sum')
faostat_area_harvested_copy = selected_rows.copy()

eight_year_ranges = [(y, y+4) for y in range(1968, 2022, 5)]
for year in eight_year_ranges:
    year_idx = faostat_area_harvested_copy[(faostat_area_harvested_copy['Year'] >= year[0]) & (faostat_area_harvested_copy['Year'] <= year[1])]['Value'].index
    faostat_area_harvested_copy.loc[year_idx, 'Value'] = faostat_area_harvested_copy[(faostat_area_harvested_copy['Year'] >= year[0]) & (faostat_area_harvested_copy['Year'] <= year[1])]['Value'].mean()

eight_year_ranges = [(y, y+4) for y in range(1968, 2022, 5)]
five_yearIncrements = range(1970, 2025, 5)
for year, five_year in zip(eight_year_ranges, five_yearIncrements):
    year_idx = faostat_area_harvested_copy[(faostat_area_harvested_copy['Year'] >= year[0]) & (faostat_area_harvested_copy['Year'] <= year[1])]['Year'].index
    faostat_area_harvested_copy.loc[year_idx, 'Year'] = five_year

faostat_area_harvested_copy_selected = faostat_area_harvested_copy[['Area', 'Element', 'Value', 'Year', 'IMAGE Classification']]
faostat_area_harvested_copy_unique = faostat_area_harvested_copy_selected.drop_duplicates(subset=['Area', 'Year', 'IMAGE Classification'])
faostat_area_harvested_copy_unique = faostat_area_harvested_copy_unique[faostat_area_harvested_copy_unique['Year'] >= 1970]
faostat_area_harvested_copy_unique.to_csv("/vol/milkunarc/cadlan/stream_2/Step5/faostat_area_harvested.csv", index=False)

#Step 3a Convert yield from unit of 100 g/ hectare to unit of kg
fao_stat_yield = fao_stat_copy[fao_stat_copy['Element'] == "Yield"]
fao_stat_yield = fao_stat_yield.fillna(0)
fao_stat_yield = fao_stat_yield.merge(crop_class, on='Item')

fao_stat_yield_absolute = fao_stat_yield.copy()
fao_stat_yield_absolute = fao_stat_yield.merge(fao_stat_area_harv, on=['Area', 'Item', 'Year'], suffixes=('_yield', '_harv'))
fao_stat_yield_absolute['Value'] = (fao_stat_yield_absolute['Value_yield'] * fao_stat_yield_absolute['Value_harv']) / 1000
fao_stat_yield_absolute.to_csv("/vol/milkunarc/cadlan/stream_2/Step5/fao_stat_yield_absolute.csv", index=False)

#Step 3b Agregasi item yield FAO ke IMAGE categories dan create moving average 5 year
fao_stat_yield_absolute = pd.read_csv("/vol/milkunarc/cadlan/stream_2/Step5/fao_stat_yield_absolute.csv")
fao_stat_yield_absolute['IMAGE Classification_yield'] = fao_stat_yield_absolute['IMAGE Classification_yield'].apply(lambda x: x.replace('and', '&').replace('Plant-based fibers', 'Plant based fibres').replace('Oil, palm fruit', 'Oil & palm fruit').replace('Other non-food, luxury, spices', 'Other non-food & luxury & spices').replace('Vegetables or fruits', 'Vegetables & fruits'))
faostat_yield_copy = fao_stat_yield_absolute.copy()
selected_rows = fao_stat_yield_absolute[fao_stat_yield_absolute['Year'] >= 1968]
selected_rows['Value'] = selected_rows.groupby(['Year', 'Area', 'IMAGE Classification_yield'])['Value'].transform('sum')
faostat_yield_copy = selected_rows.copy()

faostat_yield_copy = faostat_yield_copy.drop_duplicates(subset=['Area', 'Year', 'IMAGE Classification_yield'])

eight_year_ranges = [(y, y+4) for y in range(1968, 2022, 5)]
for year in eight_year_ranges:
    mask = (faostat_yield_copy['Year'] >= year[0]) & (faostat_yield_copy['Year'] <= year[1])
    faostat_yield_copy.loc[mask, 'Value'] = faostat_yield_copy[mask].groupby(['Area', 'IMAGE Classification_yield'])['Value'].transform('mean')

eight_year_ranges = [(y, y+4) for y in range(1968, 2022, 5)]
five_yearIncrements = range(1970, 2025, 5)
for year, five_year in zip(eight_year_ranges, five_yearIncrements):
    year_idx = faostat_yield_copy[(faostat_yield_copy['Year'] >= year[0]) & (faostat_yield_copy['Year'] <= year[1])]['Year'].index
    faostat_yield_copy.loc[year_idx, 'Year'] = five_year

faostat_yield_copy_selected = faostat_yield_copy[['Area', 'Element_yield', 'Value', 'Year', 'IMAGE Classification_yield']]

faostat_yield_copy_selected_unique = faostat_yield_copy_selected[faostat_yield_copy_selected['Year'] >= 1970]
faostat_yield_copy_selected_unique.rename(columns={"IMAGE Classification_yield" : "IMAGE Classification"})

faostat_yield_copy_selected_unique.to_csv("/vol/milkunarc/cadlan/stream_2/Step5/faostat_yield.csv", index=False)

