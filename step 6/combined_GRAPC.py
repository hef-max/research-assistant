import os
import xarray
import numpy as np
import pandas as pd

grapc5min = xarray.open_dataset("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GRAPC.NC", engine="netcdf4")
grapc_nfbfc = grapc5min.coords['NFBFC'].data.tolist()

grapc_nfbfc_ir_rf = []
grapc_nfbfc_rf = []
grapc_nfbfc_ir = []

for idx, grapc_cls in enumerate(grapc_nfbfc):
    if grapc_cls[0:2] == b'RF':
        grapc_nfbfc_rf.append(grapc_cls)
        grapc_nfbfc_ir_rf.append(grapc_cls[3:])
    elif grapc_cls[0:2] == b'IR':
        grapc_nfbfc_ir.append(grapc_cls)
        grapc_nfbfc_ir_rf.append(grapc_cls[3:])

grapc5mins_new = np.zeros((27, 16, 2160, 4320), dtype="float32")

for i in range(27):
    for idx, nfbfc_rfir in enumerate(zip(grapc_nfbfc_rf, grapc_nfbfc_ir)):
        if nfbfc_rfir[0][0:2] == b'RF' or nfbfc_rfir[1][0:2] == b'IR':
            grapc5mins_new[i][idx] = grapc5min["GRAPC"].isel(time=i).sel(NFBFC=nfbfc_rfir[0]).to_numpy() + grapc5min["GRAPC"].isel(time=i).sel(NFBFC=nfbfc_rfir[1]).to_numpy()
        else:
            grapc5mins_new[i][idx] = grapc5min["GRAPC"].isel(time=i).sel(NFBFC=grapc_nfbfc_rf[0]).to_numpy()

grapc5mins_new_netcdf = xarray.Dataset({
    "grapc_combined":(["time", "NFBFC", "latitude", "longitude"], grapc5mins_new)
},coords={
        "time": pd.date_range(start='1970-01-01', end='2100-01-01', freq='5AS'),
        "latitude": grapc5min.coords["latitude"].to_numpy(),
        "longitude": grapc5min.coords["longitude"].to_numpy(),
        "NFBFC": list(grapc_nfbfc_ir_rf[:16])
    })
grapc5mins_new_netcdf.to_netcdf("/vol/milkunC/achaidir/IMAGE PBL/SSP2/GRAPC_combined.NC", mode="w", format="NETCDF4")