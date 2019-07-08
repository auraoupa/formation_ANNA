import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr
import numpy as np                                                     
import glob
import time

import linearfilters as tf

c = Client()
                                   

data_dir = '/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/'

wfiles = sorted(glob.glob(data_dir + '*/eNATL60-BLBT02_1h_*_gridW_20090701-20090701.nc'))

def non_time_coords(ds):
    return [v for v in ds.data_vars
            if 'time_counter' not in ds[v].dims]

def drop_non_essential_vars_pop(ds):
    return ds.drop(non_time_coords(ds))

%time dsw=xr.open_mfdataset(wfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop) # 1 jour : 40.7ms

def filt_w(w):
    win_box2D = w.win
    win_box2D.set(window_name='lanczos', n=[80, 80], dims=['x', 'y'], fc=0.0125)
    bw = win_box2D.boundary_weights(drop_dims=[])
    w_LS = win_box2D.apply(weights=bw)
    w_SS=w-w_LS
    return w_SS


import Box2x2 as bb

for ibox in bb.boxes:
   box = ibox
   if box.nb == '1':
      print(box.name)
      w=dsw.vovecrtz[:,:,box.jmin:box.jmax,box.imin:box.imax]
      %time wfilt=filt_w(w)




