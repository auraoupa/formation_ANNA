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

def filt(w):
    win_box2D = w.window
    win_box2D.set(window='boxcar', cutoff=80, dim=['x', 'y'], n=[80, 80])
    bw = win_box2D.boundary_weights(drop_dims=[])
    w_LS = win_box2D.convolve(weights=bw)
    w_SS=w-w_LS
    return w_SS

import Box2x2 as bb

def compute_wprimebprime_1lev_1h(time,lev):
      temp=dst.votemper[time,lev]
      salt=dss.vosaline[time,lev]
      w=dsw.vovecrtz[time,lev]
      buoy=compute_buoy(temp,salt)
      wprime=filt(w)
      bprime=filt(buoy)
      wprimebprime=wprime*bprime
      for ibox in bb.boxes:
        box = ibox
        print(box.name)      
	print(wprimebprime[box.jmin:box.jmax,box.imin:box.imax].mean(dim={'x','y'}).values)


dst=xr.open_dataset('/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/00388801-00399600/eNATL60-BLBT02_1h_20090630_20090704_gridT_20090701-20090701.nc',chunks={'x':1000,'y':1000})
dss=xr.open_dataset('/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/00388801-00399600/eNATL60-BLBT02_1h_20090630_20090704_gridS_20090701-20090701.nc',chunks={'x':1000,'y':1000})
dsw=xr.open_dataset('/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/00388801-00399600/eNATL60-BLBT02_1h_20090630_20090704_gridW_20090701-20090701.nc',chunks={'x':1000,'y':1000})

%time compute_wprimebprime_1lev_1h(10,10) ## 3min12




