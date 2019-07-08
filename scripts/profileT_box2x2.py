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

def non_time_coords(ds):
    return [v for v in ds.data_vars
            if 'time_counter' not in ds[v].dims]

def drop_non_essential_vars_pop(ds):
    return ds.drop(non_time_coords(ds))

%time dst=xr.open_mfdataset(tfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop) # 1 jour : 40.7ms

import Box2x2 as bb

def save_prof_box(imin,imax,jmin,jmax,name):
      temp=dst.votemper[:,:,jmin:jmax,imin:imax]
      prof_temp=temp.mean(dim={'x','y'})
      print('Save profile temp to netcdf')
      prof_temp.attrs['Name'] = 'eNATL60'+box.name+'-BLBT02_y2009m07d01_MeanTempProf.nc'
      prof_temp.to_dataset().to_netcdf(path='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/eNATL60'+name+'-BLBT02_y2009m07d01_MeanTempProf.nc',mode='w')



for ibox in bb.boxes:
   box = ibox
   if box.nb == '1':
      print(box.name)
      %time save_prof_box(box.imin,box.imax,box.jmin,box.jmax,box.name)

#8mn pour une boite ...

# test avec chunks au lieu des box

%time dstb=xr.open_mfdataset(tfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop,chunks={'x':120,'y':120,'time_counter':24}) # 1 jour : 40.7ms


def save_prof_all():
      temp=dstb.votemper
      prof_temp=temp.mean(dim={'x','y'})
      print('Save profile temp to netcdf')
      prof_temp.attrs['Name'] = 'eNATL60-BLBT02_y2009m07d01_MeanTempProf.nc'
      prof_temp.to_dataset().to_netcdf(path='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/eNATL60-BLBT02_y2009m07d01_MeanTempProf.nc',mode='w')




