#!/usr/bin/env python
# coding: utf-8

import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client

c = Client()
c



##imports

import xarray as xr 
import dask 
import numpy as np 
import os 
import time 
import glob
import datetime
import pandas as pd
import sys

today=datetime.date.today()

import sys
sys.path.insert(0,'/home/albert7a/git/xscale')
import xscale


## data location and gridfile

data_dir = '/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/'
gridfile='/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-I/mesh_mask_eNATL60_3.6_lev1.nc4'
dsgrid=xr.open_dataset(gridfile,chunks={'x':1000,'y':1000})



## box indices 
def read_csv(box):
    boxes=pd.read_csv('/home/albert7a/git/formation_ANNA/make_boxes/boxes_'+str(box)+'_1x1_eNATL60.csv',sep = '\t',index_col=0)
    imin=boxes['imin']
    imax=boxes['imax']
    jmin=boxes['jmin']
    jmax=boxes['jmax']
    box_name=boxes.index
    return imin,imax,jmin,jmax,box_name


## functions useful for computations

def dx_var(data,e1):
    dx_var = (data.shift(x=-1) - data)/e1
    return dx_var
def dy_var(data,e2):
    dy_var = (data.shift(y=-1) - data)/e2
    return dy_var
def dz_var(data,e3,dimdep):
    if dimdep == 'deptht':
        dz_var = (data.shift(deptht=-1) - data)/e3
    if dimdep == 'depthu':
        dz_var = (data.shift(depthu=-1) - data)/e3
    if dimdep == 'depthv':
        dz_var = (data.shift(depthv=-1) - data)/e3
    if dimdep == 'depthw':
        dz_var = (data.shift(depthw=-1) - data)/e3
    return dz_var


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



## correspondance of dimensions and grids for each variable
filetyps = {'buoyancy' : 'gridT','votemper' : 'gridT', 'vosaline' : 'gridS','vozocrtx' : 'gridU', 'vomecrty' : 'gridV','vovecrtz' : 'gridW'}
filedeps = {'buoyancy' : 'deptht','votemper' : 'deptht','vosaline' : 'deptht','vozocrtx' : 'depthu', 'vomecrty' : 'depthv','vovecrtz':'depthw'}
filee1 = {'buoyancy' : 'e1t','votemper' : 'e1t','vosaline' : 'e1t','vozocrtx' : 'e1u', 'vomecrty' : 'e1v','vovecrtz':'e1f'}
filee2 = {'buoyancy' : 'e2t','votemper' : 'e2t','vosaline' : 'e2t','vozocrtx' : 'e2u', 'vomecrty' : 'e2v','vovecrtz':'e2f'}
filee3 = {'buoyancy' : 'e3t_0','votemper' : 'e3t_0','vosaline' : 'e3t_0','vozocrtx' : 'e3u_0', 'vomecrty' : 'e3v_0','vovecrtz':'e3w_0'}



## main computation function
def compute_profile(var,date,ibox,imin,imax,jmin,jmax,box_name):
    if var == 'buoyancy':
        filenameT = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_gridT_'+date+'-'+date+'.nc'))
        fileT=filenameT[0]
        dsT=xr.open_dataset(fileT,chunks={'x':1000,'y':1000,'time_counter':1,'deptht':1})
        dataT=dsT['votemper']
        filenameS = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_gridS_'+date+'-'+date+'.nc'))
        fileS=filenameS[0]
        dsS=xr.open_dataset(fileS,chunks={'x':1000,'y':1000,'time_counter':1,'deptht':1})
        dataS=dsS['vosaline']
        data=compute_buoy(dataT,dataS)
        attrs=dataT.attrs
        attrs['standard_name']='Buoyancy'
        attrs['long_name']='Buoyancy'
        attrs['units']='m/s2'
    else:
        filename = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_'+filetyps[var]+'_'+date+'-'+date+'.nc'))
        file=filename[0]
        ds=xr.open_dataset(file,chunks={'x':1000,'y':1000,'time_counter':1,filedeps[var]:1})
        data=ds[str(var)]
        attrs=data.attrs
        
    filt_data=filt(data)
    profile_data=filt_data[:,:,jmin[ibox]:jmax[ibox],imin[ibox]:imax[ibox]].mean(dim={'x','y','time_counter'})
    return profile_data,attrs


def compute_profile_all_var(date,ibox,profile_name,imin,imax,jmin,jmax,box_name):
    list_dataset=[]
    for var in ['votemper']:
        print('compute mean profile of '+var)
        profile_data,attrs=compute_profile(var,'20090701',0,imin,imax,jmin,jmax,box_name)
        dataset=profile_data.to_dataset(name=var)
        dataset[var].attrs=attrs
        dataset[var].attrs['standard_name']=attrs['standard_name']
        dataset[var].attrs['long_name']=attrs['long_name']
        dataset[var].attrs['units']=attrs['units']
        list_dataset.append(dataset)
    print('merging all datasets')
    big_dataset=xr.merge(list_dataset)
    big_dataset.attrs['global_attribute']= 'predictors profiles averaged over 24h and in '+box_name[ibox]+' computed on occigen '+str(today)
    print('writing to netcdf')
    big_dataset.to_netcdf(path=profile_name,mode='w')



box = 'LS'
k = 0
date = '20090714'

imin,imax,jmin,jmax,box_name=read_csv(box)
profile_name='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/'+str(box)+'/eNATL60'+str(box)+box_name[k]+'-BLBT02_y'+date[0:4]+'m'+date[4:6]+'d'+date[6:9]+'_profiles.nc'


compute_profile_all_var(date,k,profile_name,imin,imax,jmin,jmax,box_name)

for k in np.arange(61):
    profile_name='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/'+str(box)+'/eNATL60'+str(box)+box_name[k]+'-BLBT02_y'+date[0:4]+'m'+date[4:6]+'d'+date[6:9]+'_profiles.nc'
    if not os.path.exists(profile_name):
        print("Computing profiles")
        compute_profile_all_var(date,k,profile_name,imin,imax,jmin,jmax,box_name)
    else:
        print("Not computing, profiles already exist")


