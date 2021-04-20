from django.http import JsonResponse, HttpResponse

from .app import metdataexplorer as app

#########################################################################################
"""
The only function currently used in the app is list_geoserver_resources. The rest are generic functions
for interacting with the geoserver linked with the app.
"""

def geoserver_create_workspace(request):
    workspace_name = request.GET['workspaceName']
    uri = request.GET['uri']
    engine = app.get_spatial_dataset_service('geoserver', as_engine=True)
    engine.create_workspace(workspace_id=workspace_name, uri=uri)
    return JsonResponse({'result': 'success'})


def geoserver_upload_shapefile(path_to_shp, filename, workspace):
    engine = app.get_spatial_dataset_service('geoserver', as_engine=True)
    result = engine.create_shapefile_resource(store_id=workspace + ':' + filename, shapefile_base=path_to_shp, overwrite=True)
    return result['success']


def delete_geoserver_layer(layer_id):
    engine = app.get_spatial_dataset_service('geoserver', as_engine=True)
    result = engine.delete_layer(layer_id, store_id=None, purge=False, recurse=False, debug=False)
    if not result['success']:
        raise


def list_geoserver_layers(request):
    engine = app.get_spatial_dataset_service('geoserver', as_engine=True)
    result = engine.list_layers(with_properties=False, debug=False)
    print(result)
    if not result['success']:
        raise
    return JsonResponse({'result': result['result']})


def list_geoserver_stores(request):
    engine = app.get_spatial_dataset_service('geoserver', as_engine=True)
    result = engine.list_stores(workspace=None, with_properties=True, debug=False)
    print(result)
    if not result['success']:
        raise
    return JsonResponse({'result': result['result']})


################################################################################


def list_geoserver_resources(request):
    """
    This function retrieves a list of all the layers in the geoserver linked to the app.
    """
    engine = app.get_spatial_dataset_service('geoserver', as_engine=True)
    result = engine.list_resources(with_properties=True, debug=False)
    workspaces = {}
    if not result['success']:
        raise

    for shape in result['result']:
        workspaces[shape['workspace']] = {}

    for shape in result['result']:
        workspaces[shape['workspace']][shape['store']] = {}

    for shape in result['result']:
        if 'wfs' in shape:
            workspaces[shape['workspace']][shape['store']][shape['name']] = shape['wfs']['geojson']
        else:
            workspaces[shape['workspace']][shape['store']][shape['name']] = False

    return JsonResponse({'result': workspaces})
