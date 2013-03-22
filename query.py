#!/usr/bin/env python

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker, scoped_session, create_session, mapper
from sqlalchemy.sql import exists
 
 
class SetUp(object):
    """ This class sets up the database connection, creates the tables etc"""
    
    def __init__(self, username, password, host, port, database):
        self.user = username
        self.passwd = password
        self.db = database
        self.host = host
        self.port = port

    def run(self):
        conn_string = 'postgresql://'+str(self.user)+':'+str(self.passwd)+'@'+str(self.host)+':'+str(self.port)+'/'+str(self.db)
        db = create_engine(conn_string)
        return db

class UrlRecords(object):
    pass

def main():

    user = 'vjain'
    passwd = 'vj@in123'
    host = 'localhost'
    port = '5432'
    database = 'twitter_urls'


    db = SetUp(user, passwd, host, port, database)
    db_engine = db.run()

    metadata = MetaData(db_engine)
    DBSession = scoped_session(
        sessionmaker(
            autoflush=True,
            autocommit=False,
            bind=db_engine
            )
        )     

    # accounts = Table('accounts', metadata, autoload = True)
    url_records = Table('url_recs_upd', metadata, autoload = True)


    print dir(url_records)
    url_rec_mapper = mapper(UrlRecords, url_records)
    print dir(url_records)

    session = DBSession()
    res = session.query(UrlRecords).filter(url_records.c.num_redir >= 5)
    print type(res)
    # session.commit()
    # session.flush()
    DBSession.remove()
    
if __name__ == '__main__' :
    main()
