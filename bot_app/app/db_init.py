import os
from sqlalchemy import create_engine as eng
from sqlalchemy import text, MetaData, Table
from sources.job_dict import JOB_DICT

# Parameters
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

init_query = text('''
    create schema if not exists job_stg;
    create schema if not exists dw;
    create schema if not exists marts;
    
    create table if not exists dw.users (
        user_id int,
        name varchar(45) not null,
        sub_date date,
        primary key(user_id)
    );

    create table if not exists dw.request (
        request_id int generated always as identity,
        text varchar(45) not null,
        primary key(request_id)
    );

    create table if not exists dw.users_requests (
        user_id int,
        request_id int,
        primary key(user_id, request_id),
        constraint fk_user
            foreign key(user_id)
                references dw.users(user_id),
        constraint fk_request
            foreign key(request_id)
                references dw.request(request_id)
    );

    create table if not exists dw.vac_list (
        vac_id int,
        vac_name text,
        vac_url text,
        area_id int,
        area_name text,
        salary_from float,
        salary_to float,
        salary_currency text,
        published_at date,
        closed_at date,
        employer_name text,
        employer_url text,
        schedule_id text,
        primary key(vac_id)
    );

    create table if not exists dw.vacs_requests (
        vac_id int,
        request_id int,
        primary key(vac_id, request_id),
        constraint fk_vac
            foreign key(vac_id)
                references dw.vac_list(vac_id),
        constraint fk_request
            foreign key(request_id)
                references dw.request(request_id)
    );

    create table if not exists marts.users_vacs (
        user_id int,
        vac_name text,
        vac_url text,
        employer_name text 
    );

    create table if not exists job_stg.main (
        vac_id int8,
        vac_name text,
        vac_url text,
        area_id int8,
        area_name text,
        salary_from float8,
        salary_to float8,
        salary_currency text,
        published_at date,
        created_at date,
        employer_name text,
        employer_url text,
        schedule_id float8,
        request_text text,
        load_date date
    );
'''
                  )


class DB_init():
    def __init__(self, job_list, init_query=init_query):
        self.engine = eng(
            f'postgresql://{DB_USER}:{DB_PASSWORD}@bot_db:5432/{DB_NAME}')
        self.init_query = init_query
        self.job_list = job_list

    # create schemad and tables if not exists
    def create_structure(self):
        with self.engine.connect() as conn:
            conn.execute(self.init_query)
            conn.commit()

    # check existing requests
    def filter_input_requests(self, req_list: list):
        with self.engine.connect() as conn:
            db_req = (self
                      .request_table
                      .select()
                      .where(self.request_table.c.text.in_(req_list))
                      )
            res = [name for _, name in conn.execute(db_req)]

            filtered = [req for req in req_list if req not in res]
        return filtered

    # add requests to table
    def add_requests(self):
        self.dw_metadata_obj = MetaData(schema='dw')
        self.request_table = Table('request',
                                   self.dw_metadata_obj,
                                   autoload_with=self.engine)
        req = self.job_list
        lower_req = [item.lower() for item in req]
        filtered = self.filter_input_requests(lower_req)
        if filtered:
            val_list = [{'text': item} for item in filtered]
            with self.engine.connect() as conn:
                db_req = (self
                          .request_table
                          .insert()
                          .values(val_list)
                          )
                conn.execute(db_req)
                conn.commit()


if __name__ == '__main__':
    job_list = []
    for key in JOB_DICT:
        for job in JOB_DICT[key]:
            job_list.append(job)

    test = DB_init(job_list, init_query)
    test.create_structure()
    test.add_requests()
