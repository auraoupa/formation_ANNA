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
sfiles = sorted(glob.glob(data_dir + '*/eNATL60-BLBT02_1h_*_gridS_20090701-20090701.nc'))
wfiles = sorted(glob.glob(data_dir + '*/eNATL60-BLBT02_1h_*_gridW_20090701-20090701.nc'))

def non_time_coords(ds):
    return [v for v in ds.data_vars
            if 'time_counter' not in ds[v].dims]

def drop_non_essential_vars_pop(ds):
    return ds.drop(non_time_coords(ds))

%time dst=xr.open_mfdataset(tfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop) # 1 jour : 40.7ms
%time dss=xr.open_mfdataset(sfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop) # 1 jour : 40.7ms
%time dsw=xr.open_mfdataset(wfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop) # 1 jour : 40.7ms

import Box2x2 as bb

def buoy(t,s):
	rau0  = 1000
	grav  = 9.81
	buoy= -1*(grav/rau0)*sigma0(t,s)
	return buoy

def sigma0(t,s):
	zrau0=1000
	zsr=np.sqrt(np.abs(s))
	zs=s
	zt=t
	zr1 = ( ( ( ( 6.536332e-9*zt-1.120083e-6 )*zt+1.001685e-4)*zt - 9.095290e-3 )*zt+6.793952e-2 )*zt+999.842594
	zr2= ( ( ( 5.3875e-9*zt-8.2467e-7 )*zt+7.6438e-5 ) *zt - 4.0899e-3 ) *zt+0.824493
	zr3= ( -1.6546e-6*zt+1.0227e-4 ) *zt-5.72466e-3
	zr4= 4.8314e-4
	sigma0=( zr4*zs + zr3*zsr + zr2 ) *zs + zr1 - zrau0



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




