#!/usr/bin/env python

import sys
import json
import time
import datetime
import urllib
import calendar
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, Boolean, String, MetaData, ForeignKey, create_engine, types, DECIMAL
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
    
class UrlRecords(Base):
    """ creates a class to represent the url records table """

    __tablename__ = 'url_recs_upd'
    
    id = Column(String, primary_key=True) # exp url
    num_redir = Column(Integer)
    exp_url = Column(String)
    tco_url = Column(String)
    creation_tstamp = Column(DECIMAL)
    coordinates = Column(String)
    redir_list =  Column(String)


class Domains(Base):
    """ creates a class to reprsent the original url table"""
    __tablename__ = 'domains'
    id = Column(Integer, primary_key=True)
    domain_name = Column(String)
    resolved = Column(Boolean)

class ExpUrls(Base):
    """ class creation to represent exp_url table """
    __tablename__ = 'exp_urls'
    exp_url = Column(String, primary_key=True)
    tco_url = Column(String)
    resolved = Column(Boolean)

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
                    {
                        'num_redir': l['num_redirects'], 
                        'redir_list':json.dumps(l['redirect_list'])
                    }                                        
                    )
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
                    session.query(UrlRecords).filter_by(exp_url = l['expanded_url']).all()
                    
                    rec = UrlRecords(exp_url = l['expanded_url'], 
                                     tco_url = l['twit_url'],
                                     creation_tstamp = tstamp, num_redir = 0,
                                     coordinates = coods)
                    session.add(rec)
                    count += 1
                else:
                    rec = UrlRecords(exp_url = l['expanded_url'], tco_url = l['twit_url'],
                                     creation_tstamp = tstamp, num_redir = 0, coordinates = "")
                    session.add(rec)                    
                    count += 1
        session.commit()

class QueryData(object):
    ''' This class serve as a custom querier to the 
    postgres database using the sqlalchemy API'''

    def __init__(self, conn):
        self.conn = conn
        Session = sessionmaker(bind=self.conn)
        self.session = Session()

    def execute(self):
        res = self.session.query(UrlRecords).filter(UrlRecords.num_redir>= '5').all()
        return res

    def new_urls(self):
        count = 0
        count2 = 0
        # unused code, DO NOT USE this
        """ Misc code to create new urls table with only the domain name
        instead of the full url's, source url's are taken from the url_recs_upd table"""
        # res = self.session.query(UrlRecords).filter_by().yield_per(100000)
        # 
        # res = self.session.query(UrlRecords.exp_url).all()
        # for i in res:
        #     count += 1
        #     if count % 100000 == 0:
        #         print "-------------------count", count, "---------------------------"
        #     if count2 % 100000 == 0:
        #         print "-------------------co", count2, "---------------------------"

        #     exp_url = i.exp_url

        #     exp_url = urllib.unquote_plus(exp_url)

        #     # some url's don't have http://
        #     if exp_url.find('http://') == -1:
        #         exp_url = 'http://'+exp_url

        #     exp_url = exp_url[exp_url.find(':')+1:]

        #     try:
        #         host,path = urllib.splithost(exp_url)
        #         host, port = urllib.splitport(host)
        #     except:
        #         print "Error in splithost,port:", exp_url
                
        #     if self.session.query(BaseUrls).filter(BaseUrls.exp_url == host).all():
        #         # print "Already present", host
        #         continue

        #     # for j in l:
        #     #     print j.exp_url
        #     #     continue
        #     count2 += 1
        #     if i.num_redir:
        #         entry = BaseUrls(exp_url=host,resolved=True)
        #         # print "###############################Entered in DB:", host
        #     else:
        #         entry = BaseUrls(exp_url=host,resolved=False)
        #         # print "################################Entered in DB:", host
        #     self.session.add(entry)
        self.session.commit()

def connect_db():
    user = 'vjain'
    passwd = 'vj@in123'
    host = 'localhost'
    port = '5432'
    database = 'twitter_urls'

    db = SetUp(user, passwd, host, port, database)
    db_conn = db.run()
    return db_conn
    
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
    qi = QueryData(db_conn)
    l = qi.new_urls()
    # l = qi.execute()
    
    # for i in l:
    #     print i.exp_url,i.num_redir

    # create_db = CreateDb(db_conn)
    
    # if create_db.run():
    #     print "True"
    #     p_data = PublishData(db_conn, sys.argv[1])
    #     p_data.publish()

    # db = SetUp(user, passwd, host, port, database)
    # db.run():

            
if __name__ == "__main__":
    sys.exit(main())
