"""
salvoimport.py

A collection of methods for loading and consistently formatting data from the SALVO campaign, NSA, and GML

@author:
    David Clemens-Sewall, NOAA

"""

import os
import re
import numpy as np
import pandas as pd

def load_salvo_data_str(root_path, sites=None, dates=None, insts=None):
    """
    Loads requested salvo data into an xarray Dataset

    Parameters:
    -----------
    root_path : str
        Filesystem path to the root of the SALVO data directory
    sites : list, optional
        List of site names to load, if None, load all sites. The default is None.
    dates : list, optional
        List of datestrings ('YYYYMMDD') to load, if None, load all dates. The default is None.
    insts : list, optional
        List of instrument names to load, if None, load all instruments. The default is None.

    Returns:
    --------
    Dataset
        xarray dataset containing requested data
    """
    
    # Parse inputs
    if sites is None:
        sites = ['arm', 'beo', 'ice']
    if insts is None:
        raise NotImplementedError

    # Create list containing data arrays for each file
    df_list = []
    dirs = os.listdir(root_path)
    for dir in dirs:
        # Exclude nonselected dates
        if dates is None:
            if not re.search('^20[1,2][0-9]0[4-6][0-3][0-9]', dir):
                continue
        else:
            if not dir[:8] in dates:
                continue
        # Exclude nonselected sites
        site = dir[-3:]
        if not site in sites:
            continue

        # Parse selected sites
        dirs_site = os.listdir(os.path.join(root_path, dir))
        for inst in dirs_site:
            if (inst in insts) and not (inst=='magnaprobe'): # hack for magnaprobe data...
                files_inst = os.listdir(os.path.join(root_path, dir, inst))
                for file in files_inst:
                    if file[-7:]=='.a1.csv' and file[:5]=='salvo':
                        #print(os.path.join(root_path, dir, inst, file))
                        df_list.append(load_salvo_file(os.path.join(root_path, dir, inst, file), inst, site=site))

    # hack for preliminary magnaprobe data...
    if 'magnaprobe' in insts:
        magna_path = os.path.join(root_path, '..', '..', 'magnaprobe200mpreliminary')
        files = os.listdir(magna_path)
        for file in files:
            split_file = re.split(r'_|-|\.', file)
            site = split_file[1]
            if (site in sites) and ((dates is None) or (split_file[5] in dates)):
                df_list.append(load_salvo_file(os.path.join(magna_path, file), 'magnaprobe', site=site))
                        
    return pd.concat(df_list)
        

def load_salvo_file(file_path, inst, site=None):
    """
    Loads requested file into dataframe

    Parameters:
    -----------
    file_path : str
        path to file location
    inst : str
        string specifying type of instrument
    site : str, optional
        string specifying site. The default is None.

    Returns:
    --------
    Dataframe
        pandas dataframe with requested file
    """

    if inst=='kz-mobile':
        df = pd.read_csv(file_path, usecols=['date_akdt', 'timestamp_akdt', 'incident_solar_W_m2', 
                                             'reflected_solar_W_m2', 'site', 'location',
                                             'position', 'repetition', 'albedo'],
                         dtype={'position': 'float64', 'timestamp_akdt':'str'})
        # Convert date and time to timestamp
        # Check whether : is present in timestamp_akdt
        if not df['timestamp_akdt'].str.contains(':[0-9][0-9]').all():
            if df['timestamp_akdt'].str.contains(':[0-9][0-9]').any():
                raise RuntimeError("misformatted timestamp column in: " + file_path)
            else:
                df['timestamp_akdt'] = df['timestamp_akdt'].apply(lambda x: x[:-2]+':'+x[-2:])
        #print(file_path)
        df['timestamp_akdt'] = pd.to_datetime(df['date_akdt']+'T'+df['timestamp_akdt']+'-0800', utc=False)
        df.drop(columns=['date_akdt'], inplace=True)
        df['wavelength'] = np.NaN
        df.set_index(['timestamp_akdt', 'site', 'location', 'position', 'repetition', 'wavelength'], inplace=True)
        df['albedo'] = df['reflected_solar_W_m2'] / df['incident_solar_W_m2']
        df = df.melt(ignore_index=False).set_index('variable', append=True)
    if inst=='magnaprobe':
        df = pd.read_csv(file_path, parse_dates=False, usecols=['Timestamp', 'LineLocation', 'SnowDepth'])
        df['site'] = site
        df['location'] = 'line'
        df['repetition'] = 1
        df['wavelength'] = np.NaN
        df.rename(columns={'Timestamp': 'timestamp_akdt', 'LineLocation':'position', 'SnowDepth':'snow_depth_m'},
                  inplace=True)
        df['timestamp_akdt'] = pd.to_datetime(df['timestamp_akdt']+'-0800', utc=False)
        df['water_depth_m'] = -1*df['snow_depth_m'].clip(upper=0)
        df['snow_depth_m'] = df['snow_depth_m'].clip(lower=0)
        df.set_index(['timestamp_akdt', 'site', 'location', 'position', 'repetition', 'wavelength'], inplace=True)
        df = df.melt(ignore_index=False).set_index('variable', append=True)
    return df

def load_gml_albedo(dir_path, tz='UTC'):
    files = os.listdir(dir_path)
    df_list = []
    for file in files:
        df_list.append(pd.read_csv(os.path.join(dir_path, file), skiprows=2, sep=r'\s+',
                      names=['year', 'jd', 'month', 'day', 'hour', 'minute', 'dt', 'zen', 
                             'global_solar_W_m2', 'global_solar_W_m2_qc', 
                             'reflected_solar_W_m2', 'reflected_solar_W_m2_qc',
                             'direct_solar_W_m2', 'direct_solar_W_m2_qc',
                             'diffuse_solar_W_m2', 'diffuse_solar_W_m2_qc'],
                      usecols=np.arange(16)))
    df_gml = pd.concat(df_list, ignore_index=True)
    df_gml['timestamp_utc'] = pd.to_datetime(df_gml[['year', 'month', 'day', 'hour', 'minute']], utc=True)
    df_gml['incident_solar_W_m2'] = df_gml['direct_solar_W_m2']*np.cos(df_gml['zen']*np.pi/180) + df_gml['diffuse_solar_W_m2']
    df_gml['incident_solar_W_m2_qc'] = df_gml['direct_solar_W_m2_qc'] + df_gml['diffuse_solar_W_m2_qc']

    df_gml.drop(columns=['year', 'month', 'day', 'hour', 'minute', 'jd', 'dt'], inplace=True)
    df_gml.set_index('timestamp_utc', inplace=True)

    if tz=='AKDT':
        df_gml = df_gml.index.tz_convert('-0800').rename('timestamp_akdt')
    elif tz=='UTC': 
        # Do nothing
        pass
    else:
        raise NotImplementedError('tz must be "UTC" or "AKDT"')
    
    return df_gml

def preprocess_rap(ds):
    utq_site = 6
    sel_vars = ['stemp0', 'lflux0', 'sflux0', 'gflux0', 'dswsfc0', 'uswsfc0',
                'dlwsfc0', 'ulwsfc0', 'prate0', 'paccum0', 'csnow0', 'dswdbeam0', 
                'dswdiffuse0', 'tsfc0', 'usfc0', 'vsfc0']
    
    ds_rap_sel = ds.sel(sites=utq_site)[sel_vars]
    
    init_time = ds.model_initialization.sel(forecast_hour=0).values
    time = init_time + ds.forecast_hour.values.astype('timedelta64[h]')
    
    ds_rap_sel['forecast_hour'] = ('forecast_hour', time)
    ds_rap_sel = ds_rap_sel.rename({'forecast_hour':'time'})
    ds_rap_sel = ds_rap_sel.assign_coords({'model_initialization': init_time})
    ds_rap_sel = ds_rap_sel.expand_dims(dim="model_initialization")

    return ds_rap_sel

