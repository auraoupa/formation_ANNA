from netCDF4 import Dataset
import numpy as np

# - grid file
gridfile = '/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-I/coordinates_eNATL60.nc'

# - Define read data function
def read_datagrid(gridfile,latmin=None,latmax=None,lonmin=None,lonmax=None):
    """Return navlon,navlat."""
    ncfile = Dataset(gridfile,'r')
    # load navlon and navlat
    _navlon = ncfile.variables['glamt'][:,:]
    _navlat = ncfile.variables['gphit'][:,:]
    #-Define domain
    domain = (lonmin<_navlon) * (_navlon<lonmax) * (latmin<_navlat) * (_navlat<latmax)
    where = np.where(domain)
    vlats = _navlat[where]
    vlons = _navlon[where]
    #get indice
    jmin = where[0][vlats.argmin()]
    jmax = where[0][vlats.argmax()]
    imin = where[1][vlons.argmin()]
    imax = where[1][vlons.argmax()]
    #load arrays
    navlon = _navlon[jmin:jmax+1,imin:imax+1]
    navlat = _navlat[jmin:jmax+1,imin:imax+1]
    return navlon,navlat,jmin,jmax,imin,imax

# - Define box dimensions
Box_01 = ['32.0','30.0','-70.0','-68.0','Box_1','1']
Box_02 = ['32.0','30.0','-68.0','-66.0','Box_2','2']

# - Generate box array
box_arr = []
for ii in np.arange(1,3,1):
    name = eval('Box_'+str(ii).zfill(2))
    box_arr.append(name)

#- defining dictionaries for the boxes
class box: # empty container.
    def __init__(self,name=None):
        self.name = name
        return

dictboxes = {}

for ibox in box_arr:
    
    y2 = eval(ibox[0]) ;y1 = eval(ibox[1]);
    x2 = eval(ibox[2]) ;x1 = eval(ibox[3]);
    box_name = ibox[4]
    print(box_name)

    # - Obtain navlon and Navlat
    navlon,navlat,jmin,jmax,imin,imax = read_datagrid(gridfile,latmin=y1,latmax=y2,lonmin=x2,lonmax=x1)

    # - save box parameter
    abox = box(box_name)
    abox.lonmin = navlon.min()
    abox.lonmax = navlon.max()
    abox.latmin = navlat.min()
    abox.latmax = navlat.max()
    abox.navlon = navlon
    abox.navlat = navlat
    abox.imin = imin
    abox.imax = imax
    abox.jmin = jmin
    abox.jmax = jmax
    abox.nb = ibox[5]
    dictboxes[box_name] = abox

boxes = dictboxes.values()

