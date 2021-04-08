from tethys_sdk.base import TethysAppBase, url_map_maker
from tethys_sdk.app_settings import PersistentStoreDatabaseSetting
#from tethys_sdk.permissions import Permission, PermissionGroup
from tethys_sdk.app_settings import SpatialDatasetServiceSetting


class metdataexplorer(TethysAppBase):
    """
    Tethys app class for NetCDF Viewer.
    """

    name = 'Met Data Explorer'
    index = 'metdataexplorer:home'
    icon = 'metdataexplorer/images/nc.png'
    package = 'metdataexplorer'
    root_url = 'metdataexplorer'
    color = '#1600F0'
    description = 'An app for viewing files on a Thredds Data Server'
    tags = 'netCDF4, CHIRPS,'
    enable_feedback = False
    feedback_emails = []

    def url_maps(self):
        """
        Add controllers
        """
        UrlMap = url_map_maker(self.root_url)

        url_maps = (
            UrlMap(
                name='home',
                url='metdataexplorer/',
                controller='metdataexplorer.controllers.home'
            ),
            UrlMap(
                name='getFilesAndFolders',
                url='metdataexplorer/getFilesAndFolders/',
                controller='metdataexplorer.controllers.get_files_and_folders'
            ),
            UrlMap(
                name='uploadShapefile',
                url='metdataexplorer/uploadShapefile/',
                controller='metdataexplorer.shapefile.upload_shapefile'
            ),
            UrlMap(
                name='uploadShapefileToGeoserver',
                url='metdataexplorer/uploadShapefileToGeoserver/',
                controller='metdataexplorer.shapefile.upload_shapefile_to_geoserver'
            ),
            UrlMap(
                name='userGeojson',
                url='metdataexplorer/userGeojsons/',
                controller='metdataexplorer.shapefile.user_geojsons'
            ),
            UrlMap(
                name='getBoxValues',
                url='metdataexplorer/getBoxValues/',
                controller='metdataexplorer.timeseries.get_box_values'
            ),
            UrlMap(
                name='getVariablesAndFileMetadata',
                url='metdataexplorer/getVariablesAndFileMetadata/',
                controller='metdataexplorer.controllers.get_variables_and_file_metadata'
            ),
            UrlMap(
                name='getVariableMetadata',
                url='metdataexplorer/getVariableMetadata/',
                controller='metdataexplorer.controllers.get_variable_metadata'
            ),
            UrlMap(
                name='deleteContainer',
                url='metdataexplorer/deleteContainer/',
                controller='metdataexplorer.database.delete_container'
            ),
            UrlMap(
                name='threddsProxy',
                url='metdataexplorer/threddsProxy/',
                controller='metdataexplorer.controllers.thredds_proxy'
            ),
            UrlMap(
                name='updateDB',
                url='metdataexplorer/updateDB/',
                controller='metdataexplorer.database.update_database'
            ),
            UrlMap(
                name='geoserverListLayers',
                url='metdataexplorer/geoserver/',
                controller='metdataexplorer.geoserver.list_geoserver_resources'
            ),
            UrlMap(
                name='createGeoserverWorkspace',
                url='metdataexplorer/workspace/',
                controller='metdataexplorer.geoserver.geoserver_create_workspace'
            ),
            UrlMap(
                name='getLatestFiles',
                url='metdataexplorer/latest/',
                controller='metdataexplorer.timestamp.url_to_iterate_files'
            ),
            UrlMap(
                name='getFullArray',
                url='metdataexplorer/getFullArray/',
                controller='metdataexplorer.grids.get_full_array'
            ),
            UrlMap(
                name='getGeojson',
                url='metdataexplorer/getGeojson/',
                controller='metdataexplorer.shapefile.get_geojson'
            ),
            # Test
            UrlMap(
                name='GetTimeSeries',
                url='metdataexplorer/GetTimeSeries/',
                controller='metdataexplorer.grids.get_time_series'
            ),
        )

        return url_maps

    def persistent_store_settings(self):
        ps_settings = (
            PersistentStoreDatabaseSetting(
                name='thredds_db',
                description='Database to store thredds URLs.',
                initializer='metdataexplorer.init_stores.init_thredds_db',
                required=True
            ),
        )

        return ps_settings

    def spatial_dataset_service_settings(self):
        sds_settings = (
            SpatialDatasetServiceSetting(
                name='geoserver',
                description='Specify a geoserver to load shapefiles into the Met Data Explorer.',
                engine=SpatialDatasetServiceSetting.GEOSERVER,
                required=False,
            ),
        )

        return sds_settings

    #def permissions(self):
    #    edit_demo_group = Permission(
    #        name='edit_demo_group',
    #        description='Allows the user to edit the demo group'
    #    )
    #    admin = PermissionGroup(
    #        name='admin',
    #        permissions=(edit_demo_group,)
    #    )
    #    permissions = (admin,)
    #    return permissions
