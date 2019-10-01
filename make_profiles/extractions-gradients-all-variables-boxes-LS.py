#!/usr/bin/env python
#
"""
This script extracts profiles of temperature, salinity and 3D velocities in 1°x1° boxes in eNATL60-BLBT02 and compute buoyancy and 3D gradients of all quantities. Then it average over the box and over 24h and save it to a single file
"""

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

## data location and gridfile

data_dir = '/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-BLBT02-S/'
gridfile='/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-I/mesh_mask_eNATL60_3.6_lev1.nc4'
dsgrid=xr.open_dataset(gridfile,chunks={'x':1000,'y':1000})

## box indices 
boxes_LS=pd.read_csv('/home/albert7a/git/formation_ANNA/make_boxes/boxes_LS_1x1_eNATL60.csv',sep = '\t',index_col=0)
imin_LS=boxes_LS['imin']
imax_LS=boxes_LS['imax']
jmin_LS=boxes_LS['jmin']
jmax_LS=boxes_LS['jmax']
box_name=boxes_LS.index

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

## correspondance of dimensions and grids for each variable
filetyps = {'buoyancy' : 'gridT','votemper' : 'gridT', 'vosaline' : 'gridS','vozocrtx' : 'gridU', 'vomecrty' : 'gridV','vovecrtz' : 'gridW'}
filedeps = {'buoyancy' : 'deptht','votemper' : 'deptht','vosaline' : 'deptht','vozocrtx' : 'depthu', 'vomecrty' : 'depthv','vovecrtz':'depthw'}
filee1 = {'buoyancy' : 'e1t','votemper' : 'e1t','vosaline' : 'e1t','vozocrtx' : 'e1u', 'vomecrty' : 'e1v','vovecrtz':'e1f'}
filee2 = {'buoyancy' : 'e2t','votemper' : 'e2t','vosaline' : 'e2t','vozocrtx' : 'e2u', 'vomecrty' : 'e2v','vovecrtz':'e2f'}
filee3 = {'buoyancy' : 'e3t_0','votemper' : 'e3t_0','vosaline' : 'e3t_0','vozocrtx' : 'e3u_0', 'vomecrty' : 'e3v_0','vovecrtz':'e3w_0'}

## main computation function

def compute_all_profiles(var,date,ibox):
    if var == 'buoyancy':
        filenameT = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_gridT_'+date+'-'+date+'.nc'))
        fileT=filenameT[0]
        dsT=xr.open_dataset(fileT)
        data_boxT=dsT['votemper'][:,:,jmin_LS[ibox]-10:jmax_LS[ibox]+10,imin_LS[ibox]-10:imax_LS[ibox]+10]
        filenameS = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_gridS_'+date+'-'+date+'.nc'))
        fileS=filenameS[0]
        dsS=xr.open_dataset(fileS)
        data_boxS=dsS['vosaline'][:,:,jmin_LS[ibox]-10:jmax_LS[ibox]+10,imin_LS[ibox]-10:imax_LS[ibox]+10]
        data_box=compute_buoy(data_boxT,data_boxS)
        attrs=data_boxT.attrs
        attrs['standard_name']='Buoyancy'
        attrs['long_name']='Buoyancy'
        attrs['units']='m/s2'
    else:
        filename = sorted(glob.glob(data_dir+'*/eNATL60-BLBT02_1h_*_'+filetyps[var]+'_'+date+'-'+date+'.nc'))
        file=filename[0]
        ds=xr.open_dataset(file)
        data_box=ds[str(var)][:,:,jmin_LS[ibox]-10:jmax_LS[ibox]+10,imin_LS[ibox]-10:imax_LS[ibox]+10]
        attrs=data_box.attrs
        
    e1=dsgrid[str(filee1[var])][0,jmin_LS[ibox]-10:jmax_LS[ibox]+10,imin_LS[ibox]-10:imax_LS[ibox]+10]
    e2=dsgrid[str(filee2[var])][0,jmin_LS[ibox]-10:jmax_LS[ibox]+10,imin_LS[ibox]-10:imax_LS[ibox]+10]
    e3=dsgrid[str(filee3[var])][0,jmin_LS[ibox]-10:jmax_LS[ibox]+10,imin_LS[ibox]-10:imax_LS[ibox]+10]
    data_dx=dx_var(data_box,e1)
    data_dy=dy_var(data_box,e2)
    data_dz=dz_var(data_box,e3,filedeps[var])
    profile_data=data_box[:,:,10:-10,10:-10].mean(dim={'x','y','time_counter'})
    profile_data_dx=data_dx[:,:,10:-10,10:-10].mean(dim={'x','y','time_counter'})
    profile_data_dy=data_dy[:,:,10:-10,10:-10].mean(dim={'x','y','time_counter'})
    profile_data_dz=data_dz[:,:,10:-10,10:-10].mean(dim={'x','y','time_counter'})
    return profile_data,profile_data_dx,profile_data_dy,profile_data_dz,attrs

def compute_all_profiles_all_var(date,ibox):
    list_dataset=[]
    for var in ['votemper','vosaline','vozocrtx','vomecrty','vovecrtz','buoyancy']:
        print('compute profile and dx,dy,dz of '+var)
        profile_data,profile_data_dx,profile_data_dy,profile_data_dz,attrs=compute_all_profiles(var,'20090701',0)
        dataset=profile_data.to_dataset(name=var)
        dataset[var].attrs=attrs
        dataset[var].attrs['standard_name']=attrs['standard_name']
        dataset[var].attrs['long_name']=attrs['long_name']
        dataset[var].attrs['units']=attrs['units']
        list_dataset.append(dataset)
        dataset=profile_data_dx.to_dataset(name='dx'+var)
        dataset['dx'+var].attrs=attrs
        dataset['dx'+var].attrs['standard_name']='dx gradient of '+attrs['standard_name']
        dataset['dx'+var].attrs['long_name']='dx_'+attrs['long_name']
        dataset['dx'+var].attrs['units']=attrs['units']
        list_dataset.append(dataset)
        dataset=profile_data_dy.to_dataset(name='dy'+var)
        dataset['dy'+var].attrs=attrs
        dataset['dy'+var].attrs['standard_name']='dy gradient of '+attrs['standard_name']
        dataset['dy'+var].attrs['long_name']='dy_'+attrs['long_name']
        dataset['dy'+var].attrs['units']=attrs['units']
        list_dataset.append(dataset)
        dataset=profile_data_dz.to_dataset(name='dz'+var)
        dataset['dz'+var].attrs=attrs
        dataset['dz'+var].attrs['standard_name']='dz gradient of '+attrs['standard_name']
        dataset['dz'+var].attrs['long_name']='dz_'+attrs['long_name']
        dataset['dz'+var].attrs['units']=attrs['units']
        list_dataset.append(dataset)
    print('merging all datasets')
    profile_name='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/LS/eNATL60LS'+box_name[ibox]+'-BLBT02_y'+date[0:4]+'m'+date[4:6]+'d'+date[6:9]+'_predictors-profiles.nc'
    big_dataset=xr.merge(list_dataset)
    big_dataset.attrs['global_attribute']= 'predictors profiles averaged over 24h and in '+box_name[ibox]+' computed on occigen '+str(today)
    print('writing to netcdf')
    big_dataset.to_netcdf(path=profile_name,mode='w')


## parser and main
def script_parser():
    """Customized parser.
    """
    from optparse import OptionParser
    usage = "usage: %prog k[which box] date"
    parser = OptionParser(usage=usage)
    return parser

def main():
    parser = script_parser()
    (options, args) = parser.parse_args()
    if len(args) < 2: # print the help message if number of args is not 3.
        parser.print_help()
        sys.exit()
    optdic = vars(options)

    k = int(args[0])
    date = str(args[1])

    now=datetime.datetime.now()
    print("Start at ", now.strftime("%Y-%m-%d %H:%M:%S"))

    profile_name='/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/ANNA/LS/eNATL60LS'+box_name[ibox]+'-BLBT02_y'+date[0:4]+'m'+date[4:6]+'d'+date[6:9]+'_predictors-profiles.nc'
  
    if not os.path.exists(profile_name):
        print("Computing profiles")
        compute_all_profiles_all_var(date,k)
    else:
        print("Not computing, profiles already exist")

    now=datetime.datetime.now()
    print("End at ", now.strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == '__main__':
    sys.exit(main() or 0)

