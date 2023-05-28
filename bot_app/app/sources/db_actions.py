import os
from sqlalchemy import create_engine as eng, Table, MetaData
from datetime import datetime

# Parameters
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')


class DB_reqs():
    ''' Class for adding and checking users and requests'''

    def __init__(self):
        # set main parameters for worcing with database
        self.engine = eng(
            f'postgresql://{DB_USER}:{DB_PASSWORD}@localhost:5432/{DB_NAME}')
        self.dw_metadata_obj = MetaData(schema='dw')
        self.marts_metadata_obj = MetaData(schema='marts')
        self.user_table = Table('users',
                                self.dw_metadata_obj,
                                autoload_with=self.engine)
        self.request_table = Table('request',
                                   self.dw_metadata_obj,
                                   autoload_with=self.engine)
        self.user_requests_table = Table('users_requests',
                                         self.dw_metadata_obj,
                                         autoload_with=self.engine)
        self.users_vacs_table = Table('users_vacs',
                                      self.marts_metadata_obj,
                                      autoload_with=self.engine)

    # -- CHECKING USER --

    def checking_users(self, user_id: int):
        '''checking user exists in the database'''
        with self.engine.connect() as conn:
            db_req = (self
                      .user_table
                      .select()
                      .where(self.user_table.c.user_id == user_id)
                      )
            res = conn.execute(db_req)
        return res.fetchall()

    # -- FILTER REQUESTS -- (moved to db_init)

    # -- filter USER-REQEST --

    def filter_user_req(self, user_id: int, ids: list):
        '''Checking if exists user - requests '''
        # getting what alreaidy exsits for user
        with self.engine.connect() as conn:
            db_req = (self
                      .user_requests_table
                      .select()
                      .where(self
                             .user_requests_table
                             .c
                             .user_id == user_id)
                      )
            exists_ids = [name for _, name in conn.execute(db_req)]
        filtered = [id for id in ids if id not in exists_ids]
        return filtered

    # -- ADDING USER --

    def add_user(self, user_id: int, name: str):
        ''' Adding user to DataBase '''
        # create dictionary with user information
        user_detail = [{
            'user_id': user_id,
            'name': name,
            'sub_date': datetime.now().date()
        }]
        # check if user not exists in db
        if not self.checking_users(user_id):
            # then add new user to database
            with self.engine.connect() as conn:
                db_req = (self
                          .user_table
                          .insert()
                          .values(user_detail)
                          )
                conn.execute(db_req)
                conn.commit()
            return True
        # else return False
        return False

    # -- ADDING REQUESTS -- (moved to db_init)

    # -- GETTING LIST OF REQUESTS FOR MANAGING

    def get_reqs_list(self):
        '''Getting list of requests from DB for managing users subscribes'''
        # get ids for user's requests
        with self.engine.connect() as conn:
            db_req = (self
                      .request_table
                      .select()
                      )
            req_dict = {}
            for id, req in conn.execute(db_req):
                req_dict[req] = id
        return req_dict

    # -- ADDING "USER - REQUESTS" LINKS --

    def add_user_req_links(self, user_id: int, req: list):
        '''Add links between the user and requests'''
        lower_req = [item.lower() for item in req]
        # get ids for user's requests
        with self.engine.connect() as conn:
            db_req = (self
                      .request_table
                      .select()
                      .where(self.request_table.c.text.in_(lower_req))
                      )
            ids = [id for id, _ in conn.execute(db_req)]
        filtered_ids = self.filter_user_req(user_id, ids)
        if filtered_ids:
            # prepare dict of values for load to database
            u_r_dict = [{'user_id': user_id,
                        'request_id': _id}
                        for _id in filtered_ids]

            # add rows into the table
            with self.engine.connect() as conn:
                db_req = (self
                          .user_requests_table
                          .insert()
                          .values(u_r_dict)
                          )
                conn.execute(db_req)
                conn.commit()

    # -- GENERATE DAILY REPORT --

    def generate_report(self):
        '''Get information about new jobs and generate messages'''

        # Get list with user's ids and new jobs for them
        with self.engine.connect() as conn:
            db_req = (self
                      .users_vacs_table
                      .select()
                      )
            vac_data = conn.execute(db_req).fetchall()

        result = {}

        # Generate messages for users
        for id, vac, url, company in vac_data:
            if id not in result:
                result[id] = ''
            result[id] += f'Компания: "{company}"\n{vac}\n{url}\n\n'

        return result

    # -- GET INFORMATION ABOUT SUBSCRIPTIONS --

    def get_list_of_subscribes(self, user_id: int):
        '''Get information about subscriptions for user'''
        with self.engine.connect() as conn:
            db_req = (self
                      .user_requests_table
                      .select()
                      .where(self.user_requests_table.c.user_id == user_id)
                      )
            ids = [id for _, id in conn.execute(db_req)]
        res_dict = {}
        with self.engine.connect() as conn:
            db_req = (self
                      .request_table
                      .select()
                      .where(self.request_table.c.request_id.in_(ids))
                      )
            for id, req in conn.execute(db_req):
                res_dict[req] = id
        return res_dict

    # -- UNSUBSCRIBE USER FROM CHOSEN JOB --

    def delete_user_req_links(self, user_id: int, req_id: int):
        '''Delete link between the user and request'''
        with self.engine.connect() as conn:
            db_req = (self
                      .user_requests_table
                      .delete()
                      .where(self.user_requests_table.c.user_id == user_id)
                      .where(self.user_requests_table.c.request_id == req_id)
                      )
            req_list = conn.execute(db_req)
        # return req_list
        pass


if __name__ == '__main__':
    test = DB_reqs()
    print(test.get_list_of_subscribes(472730224))
    pass
