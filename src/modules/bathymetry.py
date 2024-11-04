import os
import requests
import numpy as np
import netCDF4 as nc

from tqdm import tqdm

class BathymetryDataSingleton:
    _instance = None
    _bathymetry_data = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(BathymetryDataSingleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def set(self, data):
        self._bathymetry_data = data

    def get(self):
        return self._bathymetry_data
    
def download_large_file(url, local_path):
    chunk_size = 1024  # Size of each chunk in bytes
    response = requests.get(url, stream=True)
    
    # Raise an exception if the request was unsuccessful
    response.raise_for_status()

    total_size = int(response.headers.get('Content-Length', 0))
    progress_bar = tqdm(total=total_size, unit='B', unit_scale=True)

    with open(local_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=chunk_size):
            file.write(chunk)
            progress_bar.update(len(chunk))

    progress_bar.close()
    print("Download complete!")

def get_bathymetry_data():
    """Loads bathymetry data from file or if not existent from URL"""

    bathymetry_data_url = 'https://www.ngdc.noaa.gov/thredds/fileServer/global/ETOPO2022/60s/60s_bed_elev_netcdf/ETOPO_2022_v1_60s_N90W180_bed.nc'
    bathymetry_data_path = 'data/ETOPO_2022_v1_60s_N90W180_bed.nc'

    if not os.path.exists('data'):
        os.makedirs('data')

    # Check if the bathymetry data file already exists locally
    if not os.path.isfile(bathymetry_data_path):
        print("Bathymetry data file is missing. Downloading ...")
        download_large_file(bathymetry_data_url, bathymetry_data_path)
        print("OK, Downloaded.")

    # Load bathymetry data from file
    bathymetry_data = nc.Dataset(bathymetry_data_path, 'r')

    return bathymetry_data

def get_bathymetry_subset_data(dataset, lon_min, lon_max, lat_min, lat_max):
    # Extract longitude, latitude, and depth data
    lon = dataset.variables['lon'][:]
    lat = dataset.variables['lat'][:]
    depth = dataset.variables['z'][:]

    # Find the indices corresponding to the specified region
    lon_indices = np.logical_and(lon >= lon_min, lon <= lon_max)
    lat_indices = np.logical_and(lat >= lat_min, lat <= lat_max)

    # Subset the data based on the specified region
    subset_depth = depth[lat_indices][:, lon_indices]

    # Create an in-memory netCDF4 dataset
    output_dataset = nc.Dataset('subset_dataset', 'w', memory=True)

    # Define dimensions
    output_dataset.createDimension('lon', lon_indices.sum())
    output_dataset.createDimension('lat', lat_indices.sum())

    # Create longitude and latitude variables
    output_lon = output_dataset.createVariable('lon', 'f4', ('lon',))
    output_lat = output_dataset.createVariable('lat', 'f4', ('lat',))

    # Create depth variable
    output_depth = output_dataset.createVariable('z', 'f4', ('lat', 'lon'))

    # Set attribute for longitude and latitude variables
    output_lon.long_name = 'longitude'
    output_lat.long_name = 'latitude'

    # Set data for longitude and latitude variables
    output_lon[:] = lon[lon_indices]
    output_lat[:] = lat[lat_indices]

    # Set data for depth variable
    output_depth[:, :] = subset_depth

    return output_dataset