import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
c = Client()


import xarray as xr
import numpy as np
import glob
import time
                                   
import xscale

data_dir = '/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/'


dst=xr.open_dataset('/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/00388801-00399600/eNATL60-BLBT02_1h_20090630_20090704_gridT_20090701-20090701.nc') 
dss=xr.open_dataset('/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/00388801-00399600/eNATL60-BLBT02_1h_20090630_20090704_gridS_20090701-20090701.nc')
dsw=xr.open_dataset('/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/00388801-00399600/eNATL60-BLBT02_1h_20090630_20090704_gridW_20090701-20090701.nc')

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
	return sigma0

def filt_w(w):
    win_box2D = w.window
    win_box2D.set(window='boxcar', cutoff=0.0125, dim=['x', 'y'], n=[80, 80])
window_name='lanczos', n=[80, 80], dims=['x', 'y'], fc=0.0125)
    bw = win_box2D.boundary_weights(drop_dims=[])
    w_LS = win_box2D.apply(weights=bw)
    w_SS=w-w_LS
    return w_SS

import Box2x2_box1 as bb

def compute_wprimebprime_1lev_1h(time,lev):
      temp=dst.votemper[time,lev]
      salt=dss.vosaline[time,lev]
      w=dsw.vovecrtz[time,lev]
      buoy=compute_buoy(temp,salt)
      wprime=filt_w(w)
      bprime=filt(buoy)
      wprimebprime=wprime*bprime
      for ibox in bb.boxes:
        box = ibox
        if box.nb == '1':
           print(box.name)      
	   print(wprimebprime[box.jmin:box.jmax,box.imin:box.imax].vlaues)

%time compute_wprimebprime_1lev_1h() ## 26.4s

def save_buoy_surf_24h():
      temp=dst.votemper
      salt=dss.vosaline
      temp0=temp[:25,0] 
      salt0=salt[:25,0]
      buoy0=compute_buoy(temp0,salt0)
      for ibox in bb.boxes:
        box = ibox
        if box.nb == '1':
           print(box.name)
           buoy_save=buoy0[:,box.jmin:box.jmax,box.imin:box.imax]
           buoy_save.rename('buoy').to_dataset().to_netcdf(path='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/eNATL60'+str(box.name)+'-BLBT02_y2009m07d01h24_surfbuoy.nc',mode='w')

%time save_buoy_surf_24h()  ## 33.8s

def save_buoy_surf_10z():
      temp=dst.votemper
      salt=dss.vosaline
      temp0=temp[0,0:10]
      salt0=salt[0,0:10]
      buoy0=compute_buoy(temp0,salt0)
      for ibox in bb.boxes:
        box = ibox
        if box.nb == '1':
           print(box.name)
           buoy_save=buoy0[:,box.jmin:box.jmax,box.imin:box.imax]
           buoy_save.rename('buoy').to_dataset().to_netcdf(path='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/eNATL60'+str(box.name)+'-BLBT02_y2009m07d01h01_buoy10z.nc',mode='w')

%time save_buoy_surf_10z() ## 30.8s

def save_buoy_surf_20z():
      temp=dst.votemper
      salt=dss.vosaline
      temp0=temp[0,0:20]
      salt0=salt[0,0:20]
      buoy0=compute_buoy(temp0,salt0)
      for ibox in bb.boxes:
        box = ibox
        if box.nb == '1':
           print(box.name)
           buoy_save=buoy0[:,box.jmin:box.jmax,box.imin:box.imax]
           buoy_save.rename('buoy').to_dataset().to_netcdf(path='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/eNATL60'+str(box.name)+'-BLBT02_y2009m07d01h01_buoy20z.nc',mode='w')

%time save_buoy_surf_20z()  ## abandon


def save_buoy_surf_24h5z():
      temp=dst.votemper
      salt=dss.vosaline
      temp0=temp[0:25,0:6]
      salt0=salt[0:25,0:6]
      buoy0=compute_buoy(temp0,salt0)
      for ibox in bb.boxes:
        box = ibox
        if box.nb == '1':
           print(box.name)
           buoy_save=buoy0[:,box.jmin:box.jmax,box.imin:box.imax]
           buoy_save.rename('buoy').to_dataset().to_netcdf(path='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/eNATL60'+str(box.name)+'-BLBT02_y2009m07d01_buoy5z.nc',mode='w')

%time save_buoy_surf_24h5z() ## 33.6s

def save_buoy_surf_24h10z():
      temp=dst.votemper
      salt=dss.vosaline
      temp0=temp[0:25,0:11]
      salt0=salt[0:25,0:11]
      buoy0=compute_buoy(temp0,salt0)
      for ibox in bb.boxes:
        box = ibox
        if box.nb == '1':
           print(box.name)
           buoy_save=buoy0[:,box.jmin:box.jmax,box.imin:box.imax]
           buoy_save.rename('buoy').to_dataset().to_netcdf(path='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/eNATL60'+str(box.name)+'-BLBT02_y2009m07d01_buoy10z.nc',mode='w')

%time save_buoy_surf_24h10z()  ## restarting workers ...





 save_buoy_all():
      temp=dst.votemper
      salt=dss.vosaline
      buoy=compute_buoy(temp,salt)
      print('Save buoy to netcdf')
      buoy.rename('buoy').to_dataset().to_netcdf(path='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/eNATL60-BLBT02_y2009m07d01_buoy.nc',mode='w')

%time save_buoy_all()

temp=dst.votemper
