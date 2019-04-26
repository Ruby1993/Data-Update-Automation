import functools as reduce
import pandas as pd
import os
print (os.environ)

from oauth2client.client import GoogleCredentials

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/rzhang3/Downloads/0/WeWork/Team/GoogleAPI_wemodule/wemodule-46efcc7035da.json'
print (os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
#credentials = GoogleCredentials.get_application_default()
#print (credentials)

from we_module import configure_computer
from we_module.we import We

def config():
    config =\
    {'email_me': ['cynthia.wong@wework.com'],
    'text_me': ["9173310000"],
    'end_user': 'Data Push Test',
    'description': 'This script pushes open locations data that will be implemented into a marketing tool into google sheet. ',
    'schedule':
    [{
    'type': 'normal',
    'table_name': 'test',
    'enabled':True,
    'start_at': '2019-04-27',
    'timezone': 'America/New_York',
    'cron_schedule': '30 17 * * *',
    'input_json': { 'key':"1DaGjCaYpD8lqagmsuwTkODZlvgD1353u--Q0BWgpqQM"
    },
    'incremental_field': None,
    'keys':{
    'primarykey': 'id',
    'sortkey': 'stargate_market'
    }
    }
    ]}

    return config



def main(we, **kwargs):

        ss_sync = we.get_tbl_query("""
        select market.name,territory.name,region.name
        from stargate_bi_tables.bi_market as market
        left join stargate_bi_tables.bi_territory as territory on market.territory_uuid=territory.uuid
        left join stargate_bi_tables.bi_region as region on territory.region_id=region.id
        LIMIT 10;
        """
        )

        data=we.get_google_sheet(GS_KEY, 'test')
        #print(ss_sync)
        print (data)
        #ss_sync.to_csv('test.csv')
        we.put_google_data(ss_sync,GS_KEY,'First-try', delete_existing=False)


GS_KEY = '1DaGjCaYpD8lqagmsuwTkODZlvgD1353u--Q0BWgpqQM'
we = We(True)
config=config()
print (config['schedule'][0]['input_json']['key'])
main(we,kwargs=config)
