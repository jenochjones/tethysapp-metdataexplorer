from django.http import JsonResponse
import geopandas
import os
import glob
import json

from .geoserver import *


def shp_to_geojson(shp_filepath, filename):
    new_directory = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'geojsons')
    current_geojsons = glob.glob(os.path.join(new_directory, '*.geojson'))
    already_made = False
    for geojson in current_geojsons:
        print(geojson)
        if not already_made:
            if os.path.basename(geojson) == filename + '.geojson':
                already_made = True
            else:
                already_made = False
    print(already_made)
    if not already_made:
        shape_file = geopandas.read_file(shp_filepath)
        shape_file.to_file(os.path.join(new_directory, filename + '.geojson'), driver='GeoJSON')
    return already_made


def upload_shapefile(request):
    files = request.FILES.getlist('files')
    shp_path = os.path.join(os.path.dirname(__file__), 'workspaces', 'user_workspaces')

    # write the new files to the directory
    for n, shp_file in enumerate(files):
        with open(os.path.join(shp_path, shp_file.name), 'wb') as dst:
            for chunk in files[n].chunks():
                dst.write(chunk)

    filepath = glob.glob(os.path.join(shp_path, '*.shp'))[0]
    filename = os.path.splitext(os.path.basename(filepath))[0]
    path_to_shp = os.path.join(shp_path, filename)
    already_made = True
    i = 0
    while already_made:
        already_made = shp_to_geojson(path_to_shp + '.shp', filename)
        if already_made:
            if i == 0:
                filename += str(i)
            else:
                filename = filename[:-1] + str(i)
            i += 1

    files_to_remove = glob.glob(path_to_shp + '*')
    for this_file in files_to_remove:
        os.remove(this_file)

    return JsonResponse({'filename': filename, 'alreadyMade': already_made})


def get_geojson(request):
    file_name = request.GET['name']
    path_to_geojson = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'geojsons', file_name + '.geojson')
    with open(path_to_geojson) as f:
        geojson = json.load(f)
    return JsonResponse({'geojson': geojson})


def upload_shapefile_to_geoserver(request):
    workspace = request.GET['workspace']
    store_name = request.GET['storeName']
    path_to_shp = request.GET['pathToShp']
    filename = request.GET['filename']
    result = geoserver_upload_shapefile(path_to_shp, store_name, workspace)

    if not result:
        raise

    for shp_file in glob.glob(os.path.join(path_to_shp, '*')):
        if os.path.splitext(os.path.basename(shp_file))[0] == filename:
            os.remove(shp_file)
    return JsonResponse({'success': result})


def user_geojsons(request):
    geojson_path = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'geojsons')
    files = glob.glob(os.path.join(geojson_path, '*.geojson'))
    geojson = {}

    if len(files) == 0:
        geojson = False
    else:
        for this_file in files:
            geojson[os.path.basename(this_file)[:-8]] = geopandas.read_file(this_file)

    return JsonResponse({'geojson': geojson})

############################################################################################
