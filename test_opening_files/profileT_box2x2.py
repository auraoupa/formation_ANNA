import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr
import numpy as np                                                     
import glob
import time

c = Client()
                                   

data_dir = '/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/'

tfiles = sorted(glob.glob(data_dir + '*/eNATL60-BLBT02_1h_*_gridT_20090701-20090701.nc'))

%time dst=xr.open_mfdataset(tfiles,parallel=True,concat_dim='time_counter',decode_cf=False) # 1 jour : 2.26s
%time dst=xr.open_mfdataset(tfiles,parallel=True,concat_dim='time_counter',decode_times=False) # 1 jour : 2.26s

def non_time_coords(ds):
    return [v for v in ds.data_vars
            if 'time_counter' not in ds[v].dims]

def drop_non_essential_vars_pop(ds):
    return ds.drop(non_time_coords(ds))

%time dst=xr.open_mfdataset(tfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop) # 1 jour : 40.7ms

%time dst=xr.open_mfdataset(tfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop,chunks={'x':120,'y':120,'time_counter':24}) # 1 jour : 154ms


temp=dst.votemper




