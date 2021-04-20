from sqlalchemy.orm import sessionmaker
from .model import Base, Thredds, Groups


def init_thredds_db(engine, first_time):
    """
    An example persistent store initializer function
    """
    # Create tables
    Base.metadata.create_all(engine)

    # Initial data
    if first_time:
        # Make session
        SessionMaker = sessionmaker(bind=engine)
        session = SessionMaker()
        group_one = Groups(
            name='Demo Group',
            description='This group contains pre-configured containers of commonly used data. '
                        'You can configure your own containers in User Groups.',
        )
        group_two = Groups(
            name='User Group',
            description='You can configure custom groups to make your data accessible',
        )
        session.add(group_one)
        session.add(group_two)

        container_one = Thredds(
            type='file',
            group='Demo Group',
            title='GFS Quarter Degree File',
            url='opd:https://thredds.ucar.edu/thredds/dodsC/grib/NCEP/GFS/Global_0p25deg/Best,wms:https://thredds.ucar.edu/thredds/wms/grib/NCEP/GFS/Global_0p25deg/Best,sub:https://thredds.ucar.edu/thredds/ncss/grib/NCEP/GFS/Global_0p25deg/Best,ful:https://thredds.ucar.edu/thredds/catalog/grib/NCEP/GFS/Global_0p25deg/Best',
            epsg='4326,x:360,y:0',
            spatial='"https://geoserver.hydroshare.org/geoserver/HS-c8427d7a30f0440b9773d17b7369fb69/wfs?service=wfs&version=2.0.0&request=GetFeature&typeNames=HS-c8427d7a30f0440b9773d17b7369fb69:watersheds&outputFormat=application/json"',
            description='This is the GFS One Quarter Degree Forecast File.\n\nGeoreferance:\nLat: 0 to 360 degrees\nLon: -90 to 90 degrees',
            attributes='{"Precipitation_rate_surface":{"dimensions":"time,lat,lon","units":"mm","color":"0,0.001"}}',
            timestamp='false',
        )
        container_two = Thredds(
            type='file',
            group='Demo Group',
            title='AROME Latest File',
            url='http://186.149.199.244:7000/thredds/catalog/modeldata/Salva/AROME/#YYYY#/#mm#/#DD#/arome_#YYYYmmDDHH#.nc',
            epsg='4326,x:360,y:0',
            spatial='"watersheds"',
            description='This is the latest AROME file for the Dominican Republic.',
            attributes='{"PREC":{"dimensions":"time,lat,lon","units":"mm","color":"0,5"}}',
            timestamp='true',
        )
        session.add(container_one)
        session.add(container_two)
        session.commit()
        session.close()
