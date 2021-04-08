from django.http import JsonResponse, HttpResponse
import grids
import tempfile
import os
import json
import csv
import requests
import geopandas as gpd
from geojson import dump

from .timestamp import iterate_files


def get_full_array(request):
    attribute_array = json.loads(request.GET['containerAttributes'])
    data = organize_array(attribute_array)
    #print(data)
    csv_file_path = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'csv_file.csv')
    with open(csv_file_path, 'w') as f:
        f.write(json.dumps(data))
        # for key in data.keys():
        #    f.write("%s,%s\n" % (key, data[key]))
    return JsonResponse({'result': data})


def organize_array(attribute_array):
    access_urls = {}
    variables = ''
    if attribute_array['timestamp'] == 'true':
        access_urls, file_name = iterate_files(attribute_array['url'])
    else:
        access_urls['OPENDAP'] = attribute_array['url'].split(',')[0][4:]
        access_urls['WMS'] = attribute_array['url'].split(',')[1][4:]
        access_urls['NetcdfSubset'] = attribute_array['url'].split(',')[2][4:]

    for variable in attribute_array['attributes']:
        variables += 'var=' + variable + '&'

    epsg = attribute_array['epsg']
    geojson_path = get_geojson_and_data(attribute_array['spatial'], epsg)

    data = {}
    for variable in attribute_array['attributes']:
        dims = attribute_array['attributes'][variable]['dimensions'].split(',')
        dim_order = (dims[0], dims[1], dims[2])
        stats_value = 'mean'
        feature_label = 'id'
        timeseries = get_timeseries_at_geojson([access_urls['OPENDAP']], variable, dim_order, geojson_path, feature_label, stats_value)
        data[variable] = timeseries

    os.remove(geojson_path)
    return data


def get_geojson_and_data(spatial, epsg):
    geojson_path = os.path.join(tempfile.gettempdir(), 'temp.json')
    if type(spatial) == dict:
        spatial['properties']['id'] = 'Shape'
        data = os.path.join(tempfile.gettempdir(), 'new_geo_temp.json')
        with open(data, 'w') as f:
            dump(spatial, f)
        geojson_geometry = gpd.read_file(data)
        os.remove(data)
    elif spatial[:4] == 'http':
        data = requests.Request('GET', spatial).url
        geojson_geometry = gpd.read_file(data)
    else:
        data = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', spatial + '.geojson')
        geojson_geometry = gpd.read_file(data)

    if not str(epsg) == 'false':
        if not str(epsg)[:4] == str(geojson_geometry.crs)[5:]:
            geojson_geometry = geojson_geometry.to_crs('EPSG:' + str(epsg)[:4])
        if len(epsg) > 4:
            shift_lat = int(epsg.split(',')[2][2:])
            shift_lon = int(epsg.split(',')[1][2:])
            geojson_geometry['geometry'] = geojson_geometry.translate(xoff=shift_lon, yoff=shift_lat)

    geojson_geometry.to_file(geojson_path, driver="GeoJSON")

    return geojson_path


def get_timeseries_at_geojson(files, var, dim_order, geojson_path, feature_label, stats):
    series = grids.TimeSeries(files=files, var=var, dim_order=dim_order)
    timeseries_array = series.shape(vector=geojson_path, behavior='features', labelby=feature_label, statistics=stats)
    timeseries_array['datetime'] = timeseries_array['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return timeseries_array


# Test code
def get_time_series(request):
    csv_file_path = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'csv_file.csv')
    with open(csv_file_path, mode='r') as f:
        data = f.read()

    data = json.loads(data)
    print(data)
    return JsonResponse({'result': data})
