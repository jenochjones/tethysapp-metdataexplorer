from django.http import JsonResponse, HttpResponse
from django.shortcuts import render
from tethys_sdk.permissions import login_required #, has_permission
from siphon.catalog import TDSCatalog
import requests
import netCDF4
import logging
import threading

from .model import Thredds, Groups
from .app import metdataexplorer as app
from .grids import *

log = logging.getLogger('tethys.metdataexplorer')

@login_required()
def home(request):
    """
    This function retrieves all containers from the database and passes it to home.html to render.
    It also starts a thread that calls a function in grids.py that retrieves a new timeseries for every container
    that is configured as a file with a geojson.
    """
    # ToDo fix permissions
    # demo_group = has_permission(request, 'edit_demo_group')

    SessionMaker = app.get_persistent_store_database('thredds_db', as_sessionmaker=True)
    session = SessionMaker()

    groups = session.query(Groups).all()
    thredds = session.query(Thredds).all()

    session.close()

    thread = threading.Thread(target=loop_through_containers, args=(thredds,))
    thread.start()

    context = {
        'groups': groups,
        'thredds': thredds,
        'permission': True, # demo_group,
    }
    return render(request, 'metdataexplorer/home.html', context)


def get_files_and_folders(request):
    """
    This function gets the opendap url from the client side and uses siphon to get all the files and folders
    that are in the catalog. The files and folders are organized into seperate lists in an array and the array is
    returned to the client side.
    """
    url = request.GET['url']
    data_tree = {}
    folders_dict = {}
    files_dict = {}

    try:
        # ToDo error on gfs catalog top folder, server error
        ds = TDSCatalog(url)
    except OSError:
        exception = 'Invalid URL'
        return JsonResponse({'dataTree': exception})

    folders = ds.catalog_refs
    for x in enumerate(folders):
        folders_dict[folders[x[0]].title] = folders[x[0]].href

    files = ds.datasets
    for x in enumerate(files):
        files_dict[files[x[0]].name] = files[x[0]].access_urls

    data_tree['folders'] = folders_dict
    data_tree['files'] = files_dict

    correct_url = ds.catalog_url
    return JsonResponse({'dataTree': data_tree, 'correct_url': correct_url})


def get_variables_and_file_metadata(request):
    """
    Once a file has been selected, this function is called. It uses siphon to get a list of the variables in the
    file along with all dimensions associated with each variable.
    It also uses siphon to get any metadata associated with the file.
    """
    url = request.GET['opendapURL']
    variables = {}
    file_metadata = ''

    try:
        ds = netCDF4.Dataset(url)
    except OSError:
        log.exception('get_variables_and_file_metadata')
        exception = False
        return JsonResponse({'variables_sorted': exception})

    for metadata_string in ds.__dict__:
        # The metadata is formatted into an html string befor returning to the client side.
        file_metadata += '<b>' + str(metadata_string) + '</b><br><p>' + str(ds.__dict__[metadata_string]) + '</p>'

    for variable in ds.variables:
        dimension_list = []
        for dimension in ds[variable].dimensions:
            dimension_list.append(dimension)
        # The metadata associated with each variable includes the variable dimensions, units,
        # and a range of colors for WMS visualization.
        array = {'dimensions': dimension_list, 'units': 'false', 'color': 'false'}
        variables[variable] = array

    return JsonResponse({'variables_sorted': variables, 'file_metadata': file_metadata})


def get_variable_metadata(request):
    """
    Once a variable within a file is specified, netCDF4 is used to retrieve any metadata associated with the variable.
    The metadata is formatted into an html string and returned to the client side.
    """
    url = request.GET['opendapURL']
    variable = request.GET['variable']
    variable_metadata = ''

    try:
        ds = netCDF4.Dataset(url)
    except OSError:
        exception = False
        return JsonResponse({'variables': exception})

    for metadata_string in ds[variable].__dict__:
        variable_metadata += '<b>' + str(metadata_string) + '</b><br><p>' + str(ds[variable].__dict__[metadata_string])\
                             + '</p>'

    return JsonResponse({'variable_metadata': variable_metadata})


def thredds_proxy(request):
    if 'main_url' in request.GET:
        request_url = request.GET['main_url']
        query_params = request.GET.dict()
        query_params.pop('main_url', None)
        r = requests.get(request_url, params=query_params)

        return HttpResponse(r.content, content_type="image/png")
    else:
        return JsonResponse({})
