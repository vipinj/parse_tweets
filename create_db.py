#!/usr/bin/env python

import sys
import json
import time
import datetime
import calendar
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine, types, DECIMAL
Base = declarative_base()    

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

class CreateDb(object):
    """ Creates database from the connection engine"""
    
    def __init__(self, db):
        self.db = db

    def run(self):
        with self.db.connect() as db_conn:
            metadata = MetaData()
            url_tab = Table('url_records', metadata,
                            Column('exp_url', String, primary_key=True),
                            Column('tco_url', String),
                            Column('num_redir',Integer),
                            Column('creation_tstamp', types.DECIMAL),
                            # Column('access_tstamp', Varchar)
                            Column('redir_list', types.LargeBinary),
                            Column('coordinates', String(30)),
                            )
            metadata.create_all(self.db)
            return True            

def convert_timestr(date_str):
    """ Twitter's date rep is like this
    Tue Feb 19 04:05:44 +0000 2013, this functions converts it to unix
    timestamps"""
    
    # python %z support is broken, it should be supported by the underlying OS
    # so, for now, we just remove the field entirely
    l = date_str.split(' ')
    l.__delitem__(len(l)-2)
    tp = ' '.join(l)
    t_stamp = time.strptime(tp, '%a %b %d %H:%M:%S %Y')
    return calendar.timegm(t_stamp)
    
# class Base():
# why doesn't this work ? 
# Base = declarative_base()    

class UrlRecords(Base):
    """ creates a class to represent the url records table """

    __tablename__ = 'url_records'
    
    exp_url = Column(String, primary_key=True) # exp url
    tco_url = Column(String)
    creation_tstamp = Column(DECIMAL)
    coordinates = Column(String)

    def __init__(self, exp_url, tco_url, creation_tstamp,  num_redir, coordinates):
        self.exp_url = exp_url
        self.tco_url = tco_url
        self.creation_tstamp = creation_tstamp
        self.coordinates = coordinates
        self.num_redir = num_redir
                             
class UpdateData(object):
    """ Updates data into the database"""

    def __init__(self,conn,datafile):
        self.datafile = datafile
        self.conn = conn
        
    def publish(self):
        Session = sessionmaker(bind=self.conn)
        session = Session()
        exp_set = set()

        # session.bind_table(self, tab, bind)
        count = 0
        with open(self.datafile,'r') as f:
            for line in f:
                try:
                    l = json.loads(line)
                except:
                    print line
                    continue

                if l['expanded_url'] in exp_set:
                    continue
                else:
                    exp_set.add(l['expanded_url'])
                    
                if count % 100000 == 0:
                    session.commit()
                    print count

                rec = Session.query(UrlRecords).filter(
                    UrlRecords.exp_url == l['expanded_url']).update(
                    {'num_redir': l['num_redirects']}, 
                    'redir_list':json.dumps(l['redirect_list']))
                                                                                                        
                # session.add(rec)
                count += 1
        session.commit()

class PublishData(object):
    """ Publishes data into the database"""

    def __init__(self,conn,datafile):
        self.datafile = datafile
        self.conn = conn
        
    def publish(self):
        Session = sessionmaker(bind=self.conn)
        session = Session()
        exp_setq = set()

        count = 0
        with open(self.datafile,'r') as f:
            for line in f:
                l = json.loads(line)
                if l['expanded_url'] in exp_set:
                    continue
                else:
                    exp_set.add(l['expanded_url'])
                    
                # print l
                if count % 100000 == 0:
                    session.commit()
                    print count
                tstamp = convert_timestr(l['created_at'])
                if 'geo' in l:
                    geo = l['geo']
                    coods = str(geo['coordinates'])
                #     print coods
                    # res = session.execute(
                    #     url_records.insert(), [
                    #         {'exp_url':l['expanded_url'], 'tco_url':l['twit_url'],
                    #          'creation_tstamp':tstamp, 'num_redir':0,
                    #          'coordinates':coods}
                    #         ])
                    session.query(UrlRecords).filter_by(exp_url = l['expanded_url']).all()
                    
                    rec = UrlRecords(exp_url = l['expanded_url'], 
                                     tco_url = l['twit_url'],
                                     creation_tstamp = tstamp, num_redir = 0,
                                     coordinates = coods)
                    session.add(rec)
                    count += 1
                else:
                    # res = session.execute(
                    #     url_records.insert(), [
                    #         {'exp_url':l['expanded_url'], 'tco_url':l['twit_url'],
                    #          'creation_tstamp':tstamp, 'num_redir':0}
                    #         ])
                    rec = UrlRecords(exp_url = l['expanded_url'], tco_url = l['twit_url'],
                                     creation_tstamp = tstamp, num_redir = 0, coordinates = "")
                    session.add(rec)                    
                    count += 1
        session.commit()
    
            
def main():
    print 'started'
    user = 'vjain'
    passwd = 'vj@in123'
    host = 'localhost'
    port = '5432'
    database = 'twitter_urls'
    
    base = Base()
    
    # url_records = UrlRecords(Base)

    db = SetUp(user, passwd, host, port, database)
    db_conn = db.run()
    
    create_db = CreateDb(db_conn)
    
    if create_db.run():
        print "True"
        p_data = PublishData(db_conn, sys.argv[1])
        p_data.publish()

    # db = SetUp(user, passwd, host, port, database)
    # db.run():
            
if __name__ == "__main__":
    sys.exit(main())
