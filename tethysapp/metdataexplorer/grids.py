from django.http import JsonResponse, HttpResponse
import grids
import os
import json
import requests
import geopandas as gpd
from geojson import dump
from datetime import datetime

from .timestamp import iterate_files


def loop_through_containers(containers):
    """
    This function iterates through each container in the database. It checks to see if there is a geojson
    associated with the container and if so, it formatts the database object as an array and calls a function to get
    the timeseries.
    """
    updated_containers = {}
    start = datetime.utcnow()
    print('started at ' + start.strftime('%Y-%m-%d %H:%M:%S'))
    for container in containers:
        print(container.spatial)
        print(type(container.spatial))
        if not container.spatial is False:
            formatted_container = {
                'type': container.type,
                'group': container.group,
                'title': container.title,
                'epsg': container.epsg,
                'url': container.url,
                'spatial': container.spatial.replace('"', ''),
                'description': container.description,
                'attributes': json.loads(container.attributes),
                'timestamp': container.timestamp,
            }
            print(formatted_container)
            update_container(formatted_container)
            updated_containers[container.title] = True
        else:
            updated_containers[container.title] = False

    end = datetime.utcnow()
    difference = start - end
    print('ended at ' + end.strftime('%Y-%m-%d %H:%M:%S'))
    print('time elapsed: ' + str(difference.total_seconds()) + 'seconds')


def update_container(container):
    """
    This function does the same thing as loop_through_containers but on a single container. It is called
    when a container is added to the database.
    """
    data = organize_array(container)
    filename = container['title'].replace(' ', '') + '_' + container['group'].replace(' ', '') + '.txt'
    filepath = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'timeseries', filename)
    with open(filepath, 'w') as f:
        f.write(json.dumps(data))
    print('Finished')


def get_full_array(request):
    """
    This function gets the timeseries from the file.
    """
    name = request.GET['containerName']
    group = request.GET['containerGroup']
    filename = name.replace(' ', '') + '_' + group.replace(' ', '') + '.txt'
    print(filename)
    filepath = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'timeseries', filename)
    f = open(filepath, 'r')
    data = json.loads(f.read())
    data.pop('updated')
    print(data)
    return JsonResponse({'result': data})


def update_one_container(request):
    """
    This function updates the timeseries file for one container.
    """
    attribute_array = json.loads(request.GET['containerAttributes'])
    print(attribute_array)
    data = organize_array(attribute_array)
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
    print(epsg)
    geojson_path = get_geojson_and_data(attribute_array['spatial'], epsg)
    print(geojson_path)

    data = {}
    for variable in attribute_array['attributes']:
        dims = attribute_array['attributes'][variable]['dimensions'].split(',')
        dim_order = (dims[0], dims[1], dims[2])
        print(dim_order)
        stats_value = 'mean'
        feature_label = 'id'
        timeseries = get_timeseries_at_geojson([access_urls['OPENDAP']], variable, dim_order, geojson_path,
                                               feature_label, stats_value)
        data[variable] = timeseries

    now = datetime.utcnow()
    data['updated'] = now.strftime('%m/%d/%Y, %H:%M:%S')
    os.remove(geojson_path)
    return data


def get_geojson_and_data(spatial, epsg):
    geojson_path = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'temp', 'temp.json')
    if type(spatial) == dict or spatial[:5] == '{type':
        spatial['properties']['id'] = 'Shape'
        data = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'temp', 'new_geo_temp.json')
        with open(data, 'w') as f:
            dump(spatial, f)
        geojson_geometry = gpd.read_file(data)
        os.remove(data)
    elif spatial[:4] == 'http':
        data = requests.Request('GET', spatial).url
        geojson_geometry = gpd.read_file(data)
    else:
        data = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'geojsons', spatial + '.geojson')
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
