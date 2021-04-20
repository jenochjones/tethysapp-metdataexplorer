from django.http import JsonResponse
import json
import os
import urllib
import netCDF4 as nc
import numpy as np
import pandas as pd


def mean_of_timeseries(path, variable, time_dim):
    results = {}
    numlist = []
    time_list = []
    netcdf = nc.Dataset(path)

    units = netcdf[time_dim].units
    times = netcdf[time_dim][:]

    for ti in times:
        t = nc.num2date(ti, units)
        time_list.append(str(t))

    results['timeseries'] = time_list

    array = netcdf[variable][:]
    array[array == -9999] = np.nan

    for dat in array:
        numlist.append(np.nanmean(dat))

    results['mean'] = numlist
    df = pd.DataFrame(data=results)
    return df


def get_box_values(request):
    url_dict = request.GET.dict()
    path_to_netcdf = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'temp', 'temp.nc')
    try:
        urllib.request.urlretrieve(url_dict['subsetURL'], path_to_netcdf)
    except OSError:
        data = False
        return JsonResponse({'data': data})
    try:
        data = mean_of_timeseries(path_to_netcdf, url_dict['var'], url_dict['time'])
        return JsonResponse({'data': data})
    except ValueError:
        data = False
        return JsonResponse({'data': data})
