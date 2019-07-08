## Pb sur occigen les jobs se terminent a cause de workers killed ...

from dask_jobqueue import SLURMCluster 
from dask.distributed import Client 
  
cluster = SLURMCluster(cores=28,name='make_zarr',walltime='00:20:00',job_extra=['--constraint=BDW28','--exclusive','--nodes=1'],memory='20GB') 
print(cluster.job_script()) 
  
cluster.scale(1) 
cluster.adapt(minimum=1, maximum=4) 
  
from dask.distributed import Client 
client = Client(cluster) 
client 

import xarray as xr
import numpy as np
import glob
import time
                                   

data_dir = '/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/'

tfiles = sorted(glob.glob(data_dir + '*/eNATL60-BLBT02_1h_*_gridT_20090701-20090701.nc'))
sfiles = sorted(glob.glob(data_dir + '*/eNATL60-BLBT02_1h_*_gridS_20090701-20090701.nc'))

def non_time_coords(ds):
    return [v for v in ds.data_vars
            if 'time_counter' not in ds[v].dims]

def drop_non_essential_vars_pop(ds):
    return ds.drop(non_time_coords(ds))

%time dst=xr.open_mfdataset(tfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop,chunks={'x':120,'y':120,'deptht':1,'time_counter':24}) 
%time dss=xr.open_mfdataset(sfiles,parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop,chunks={'x':120,'y':120,'deptht':1,'time_counter':24})

def compute_buoy(t,s):
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



def save_buoy_all():
      temp=dst.votemper
      salt=dss.votemper
      buoy=compute_buoy(temp,salt)
      print('Save buoy to netcdf')
      prof_temp.attrs['Name'] = 'eNATL60-BLBT02_y2009m07d01_buoy.nc'
      prof_temp.to_dataset().to_netcdf(path='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/eNATL60-BLBT02_y2009m07d01_buoy.nc',mode='w')

%time save_buoy_all()


