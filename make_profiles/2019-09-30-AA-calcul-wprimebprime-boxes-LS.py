import xarray as xr 
import dask 
import numpy as np 
import os 
import time 
import glob
from datetime import date
import pandas as pd

today=date.today()

import sys
sys.path.insert(0,'/home/albert7a/git/xscale')
import xscale

data_dir = '/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/'
gridfile='/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-I/mesh_mask_eNATL60_3.6_lev1.nc4'
dsgrid=xr.open_dataset(gridfile,chunks={'x':1000,'y':1000})

boxes_LS=pd.read_csv('../make_boxes/boxes_LS_1x1_eNATL60.csv',sep = '\t',index_col=0)
imin_LS=boxes_LS['imin']
imax_LS=boxes_LS['imax']
jmin_LS=boxes_LS['jmin']
jmax_LS=boxes_LS['jmax']
box_name=boxes_LS.index


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
    win_box2D.set(window='hanning', cutoff=20, dim=['x', 'y'], n=[30, 30])
    bw = win_box2D.boundary_weights(drop_dims=[])
    w_LS = win_box2D.convolve(weights=bw)
    w_SS=w-w_LS
    return w_SS



def compute_mean_wprime_bprime(date,ibox):
    print('read the data')
    tfilename = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_gridT_'+date+'-'+date+'.nc'))
    tfile=tfilename[0]
    dst=xr.open_dataset(tfile,chunks={'x':1000,'y':1000,'time_counter':1,'deptht':1})
    tdata=dst['votemper']
    sfilename = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_gridS_'+date+'-'+date+'.nc'))
    sfile=sfilename[0]
    dss=xr.open_dataset(sfile,chunks={'x':1000,'y':1000,'time_counter':1,'deptht':1})
    sdata=dss['vosaline']
    wfilename = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_gridW_'+date+'-'+date+'.nc'))
    wfile=wfilename[0]
    dsw=xr.open_dataset(wfile,chunks={'x':1000,'y':1000,'time_counter':1,'depthw':1})
    wdata=dsw['vovecrtz']
    print('compute buoyancy')
    buoy=compute_buoy(tdata,sdata)
    print('filter w')
    wprime=filt(wdata)
    print('filter buoyancy')
    bprime=filt(buoy)
    wprimebprime=wprime*bprime
    profile=wprimebprime[:,:,jmin_LS[ibox]:jmax_LS[ibox],imin_LS[ibox]:imax_LS[ibox]].mean(dim={'x','y','time_counter'})
    print('to dataset')
    profile_name='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/LS/eNATL60LS'+box_name[ibox]+'-BLBT02_y'+date[0:4]+'m'+date[4:6]+'d'+date[6:9]+'_wprimebprime-profile.nc'
    dataset=profile.to_dataset(name='wprimebprime')
    dataset['wprimebprime'].attrs=tdata.attrs
    dataset['wprimebprime'].attrs['standard_name']='vertical flux of small scales buoyancy' 
    dataset['wprimebprime'].attrs['long_name']='wprimebprime'
    dataset['wprimebprime'].attrs['units']='m2/s3'        
    dataset.attrs['global_attribute']= 'vertical flux of small scales buoyancy profile averaged over 24h and in '+box_name[ibox]+' computed on occigen with dask-jobqueue '+str(today)
    print('to netcdf')
    dataset.to_netcdf(path=profile_name,mode='w')
    return profile

wprimebprime=compute_mean_wprime_bprime('20090701',0)
