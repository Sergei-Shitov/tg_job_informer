import os
import requests
import pathlib
import pandas as pd
import json
from datetime import date, datetime

from sqlalchemy import create_engine as eng, Table, MetaData, text

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')


def extract_data_job_list():
    '''Getting list of jobs for parsing'''
    # setting connection to DB with list of lobs
    engine = eng(f'postgresql://{DB_USER}:{DB_PASSWORD}@bot_db:5432/{DB_NAME}')
    meta_obj = MetaData(schema='dw')
    requests_table = Table('request',
                           meta_obj,
                           autoload_with=engine)

    # geting data from DB
    with engine.connect() as conn:
        db_req = (requests_table
                  .select()
                  .with_only_columns(requests_table.c.text)
                  )
        res = conn.execute(db_req)

    # transform to list for parsing
    job_list = [x[0] for x in res.fetchall()]
    return job_list


def extract_job_from_hh():
    '''Extract information about jobs in list using API HH'''
    # getting job list from database
    job_list = extract_data_job_list()

    # getting information with HH API
    for job in job_list:
        r = requests.get(
            f'https://api.hh.ru/vacancies?text={job}&search_field=name&per_page=100&page=0&no_magic=true'
        )

        # save first json file
        job_text = job.replace(' ', '_').replace('/', '_')

        print(job_text)

        path = pathlib.Path('temp_storage',
                            f'{job_text}_0.json'
                            )

        with open(path, 'w', encoding='utf-8') as file:
            file.write(r.text)

        # quantity of pages for searching request
        pages = r.json()['pages']

        # collect data from other pages
        for page in range(1, pages):
            res = requests.get(
                f'https://api.hh.ru/vacancies?text={job}&search_field=name&per_page=100&page={str(page)}&no_magic=true'
            )

            print(f'got {job} {page}')

            sub_path = pathlib.Path('temp_storage',
                                    f'{job_text}_{page}.json'
                                    )

            # save JSONs from other pages
            with open(sub_path, 'w', encoding='utf-8') as file:
                file.write(res.text)

            print(f'{job} {page} wrote succesfully')

        print(f'all pages for {job} wrote to files')

    print('all data was recived')


def combine_to_csv():
    '''Assemble data to one csv file'''

    # set basic variables
    result = pd.DataFrame(columns=[
        'vac_id',
        'vac_name',
        'vac_url',
        'area_id',
        'area_name',
        'salary_from',
        'salary_to',
        'salary_currency',
        'published_at',
        'created_at',
        'employer_name',
        'employer_url',
        'schedule_id',
        'request_text',
        'load_date'
    ])

    # set directory with jsons
    dir = pathlib.Path('temp_storage'
                       )
    counter = 0

    # getting list of files in temp_stg folder
    file_list = [file.name for file in dir.iterdir()]

    if file_list:

        vac_list_temp = []

        for file in file_list:
            if '.json' in file:
                vac_list_temp.append(file)
        print(vac_list_temp)

        # for every file
        for file_name in vac_list_temp:

            print(f'working with file {file_name}')

            path = pathlib.Path('temp_storage',
                                file_name
                                )
            # open file
            with open(path, encoding='utf-8') as src:
                vacancies = json.load(src)

            for item in vacancies['items']:

                vac_string = []

                vac_string.append(item['id'])
                vac_string.append(item['name'])
                vac_string.append(item['alternate_url'])
                vac_string.append(item['area']['id'])
                vac_string.append(item['area']['name'])

                if item['salary'] == None:
                    vac_string.append('')
                    vac_string.append('')
                    vac_string.append('')
                else:
                    vac_string.append(item['salary']['from'])
                    vac_string.append(item['salary']['to'])
                    vac_string.append(item['salary']['currency'])

                vac_string.append(
                    datetime
                    .strptime(item['published_at'], '%Y-%m-%dT%H:%M:%S%z')
                    .date())
                vac_string.append(
                    datetime
                    .strptime(item['created_at'], '%Y-%m-%dT%H:%M:%S%z')
                    .date())
                vac_string.append(item['employer']['name'])

                if 'alternate_url' in item['employer']:
                    vac_string.append(item['employer']['alternate_url'])
                else:
                    vac_string.append('')

                if item['schedule'] == None:
                    vac_string.append('')
                else:
                    vac_string.append(item['schedule']['id'])

                name = file_name.split('.')[0]
                clear_name = '_'.join(name.split('_')[:-1])

                vac_string.append(clear_name)
                vac_string.append(date.today())

                result.loc[max(result.index) + 1
                           if len(result.index) != 0
                           else 0] = vac_string
                counter += 1

    print(f'succesefuly loaded {counter} lines')

    # save assembled table to csv
    csv_path = pathlib.Path('/temp_storage',
                            'csv',
                            'result.csv'
                            )
    result.to_csv(csv_path, index=False, encoding='utf-8')


def job_stg_filling():
    '''Filling stage table in datatable'''
    csv_path = pathlib.Path('/temp_storage',
                            'csv',
                            'result.csv'
                            )
    # read result file
    df = pd.read_csv(csv_path, encoding='utf-8')

    # connect to DB
    engine = eng(f'postgresql://{DB_USER}:{DB_PASSWORD}@bot_db:5432/{DB_NAME}')

    # load to stg DB
    df.to_sql('main', engine, schema='job_stg', index=False,
              if_exists='append', method='multi')
    print('load succesful')


def get_new_jobs_list():
    '''Get ids of new today's vacancies'''

    # connection to db
    engine = eng(f'postgresql://{DB_USER}:{DB_PASSWORD}@bot_db:5432/{DB_NAME}')
    query = text('''
        select *
        from job_stg.main m 
        left join 
            (select m2.vac_id as filt
            from job_stg.main m2
            where m2.load_date != date(now())) ids
        on m.vac_id = ids.filt	
        where filt isnull 
    ''')
    new_job_list = pd.read_sql(query, engine)

    return new_job_list


def add_to_working_table():
    '''Add new vacancies to main table'''
    engine = eng(f'postgresql://{DB_USER}:{DB_PASSWORD}@bot_db:5432/{DB_NAME}')

    # get ids of new vacancies
    new_job_list = get_new_jobs_list()

    vac_req_list = new_job_list[['vac_id', 'request_text']]
    new_job_list = (new_job_list
                    .drop(['request_text', 'load_date', 'filt'], axis=1)
                    .rename(columns={'created_at': 'closed_at'})
                    )
    new_job_list['closed_at'] = None
    new_job_list.drop_duplicates('vac_id', inplace=True)

    new_job_list.to_sql('vac_list', engine, schema='dw', index=False,
                        if_exists='append', method='multi')

    req_ids_query = text('''
            select *
            from dw.request
            ''')
    req_ids = (pd
               .read_sql(req_ids_query, engine)
               .rename(columns={'text': 'request_text'})
               )

    req_ids['request_text'] = req_ids['request_text'].apply(
        lambda x: x.replace('/', '_').replace(' ', '_'))

    vac_req_table = vac_req_list.join(req_ids.set_index(
        'request_text'), on='request_text', validate='m:1')
    vac_req_table.drop(columns='request_text', axis=1, inplace=True)

    vac_req_table.to_sql('vacs_requests', engine, schema='dw', index=False,
                         if_exists='append', method='multi')


def create_send_data_mart():
    query = '''
        select 
            u.user_id,
            vl.vac_name,
            vl.vac_url,
            vl.employer_name 
        from
            dw.users u 
        join dw.users_requests ur on u.user_id = ur.user_id 
        join dw.request r on ur.request_id = r.request_id 
        join dw.vacs_requests vr on r.request_id = vr.request_id 
        join dw.vac_list vl on vr.vac_id = vl.vac_id
        join (select *
                from job_stg.main m 
                left join 
                    (select m2.vac_id as filt
                    from job_stg.main m2
                    where m2.load_date != date(now())) ids
                on m.vac_id = ids.filt	
                where filt isnull ) m on vl.vac_id = m.vac_id
        where m.load_date = date(now());
        '''
    engine = eng(f'postgresql://{DB_USER}:{DB_PASSWORD}@bot_db:5432/{DB_NAME}')

    send_mart = pd.read_sql(query, engine)
    send_mart.drop_duplicates(inplace=True)
    send_mart.to_sql('users_vacs', index=False, con=engine, schema='marts',
                     if_exists='replace', method='multi')


def update_closed_vacs():
    '''Request for updating closed vacancies weekly'''
    engine = eng(f'postgresql://{DB_USER}:{DB_PASSWORD}@bot_db:5432/{DB_NAME}')
    query = '''
    update dw.vac_list
    set closed_at = sub.closed_at 
    from (
        select distinct
            m.vac_id,
            m.request_text,
            max(m.load_date) over(partition by m.vac_id) as closed_at
        from 
            job_stg.main m
        ) as sub
    where
        dw.vac_list.vac_id = sub.vac_id and 
        sub.closed_at != date(now()) and 
        dw.vac_list.closed_at isnull;
    '''
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()


if __name__ == '__main__':
    #
    # used for developing and testing
    #
    # extract_job_from_hh()
    # combine_to_csv()
    # job_stg_filling()
    # add_to_working_table()
    # create_send_data_mart()
    pass
