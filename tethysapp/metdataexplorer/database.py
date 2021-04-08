from django.http import JsonResponse
import json
import os

from crontab import CronTab
from .model import Thredds
from .app import metdataexplorer as app


def update_database(request):
    database_info = json.loads(request.GET["data"])
    SessionMaker = app.get_persistent_store_database('thredds_db', as_sessionmaker=True)
    session = SessionMaker()
    # print(database_info)
    db = Thredds(
        server_type=database_info['type'],
        group=database_info['group'],
        title=database_info['title'],
        url=database_info['url'],
        epsg=database_info['epsg'],
        spatial=json.dumps(database_info['spatial']),
        description=database_info['description'],
        attributes=json.dumps(database_info['attributes']),
        timestamp=database_info['timestamp'],
    )

    session.add(db)
    session.commit()

    session.close()

    print(database_info['spatial'])
    print(type(database_info['spatial']))
    if database_info['spatial'] != False:
        file_to_run = os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', 'cron.py')
        cron = CronTab(user='jonjones')
        job = cron.new(command='cd /Users/jonjones/tethys/notebooks/ && python test.py')
        job.minute.every(1)

        cron.write()

    success = 'Dababase Updated'
    return JsonResponse({'success': success})


def delete_container(request):
    array = request.GET.dict()
    print(array)

    SessionMaker = app.get_persistent_store_database('thredds_db', as_sessionmaker=True)
    session = SessionMaker()

    if array['all'] == 'true':
        session.query(Thredds).delete(synchronize_session=False)
    else:
        delete_url = session.query(Thredds).filter(Thredds.group == array['group']).filter(
            Thredds.title == array['title']).first()
        session.delete(delete_url)

    session.commit()
    print(array['spatial'])
    if not array['spatial'] == 'false':
        os.remove(os.path.join(os.path.dirname(__file__), 'workspaces', 'app_workspace', array['spatial'] + '.geojson'))
    success = True
    return JsonResponse({'success': success})

#TODO fix edit db timestamp -- database.py", line 23, in update_database, KeyError: 'timestamp'