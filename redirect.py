#!/usr/bin/env python

# import urllib.request
import urllib2
import sys
import time
import json
import codecs
import urllib
import multiprocessing
import threading
import sqlalc_utilities as sqa
from threading import Thread
from collections import deque
from multiprocessing import  Process
from multiprocessing import JoinableQueue as Queue
from sqlalchemy.orm import Session, sessionmaker
# from sqlalchem.orm import scoped_session
import logging



lock1 = threading.Lock() # total queue
lock2 = threading.Lock() # indb queue
lock3 = threading.Lock() # todb queue



logging.basicConfig(level=logging.ERROR, filename='debug.log',
                    format='%(asctime)s %(levelname)s %(thread)d: %(process)d %(lineno)d, %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
# class RedirectionHandlerPy3(urllib.request.HTTPRedirectHandler):
#     """ Overrides RedirectionHandler to output 
#     the number of redirects"""
#     """ script started as py3 script, but was edited for py2 for sqlalchemy """
    
#     def __init__(self, redirect_list):
#         self.redirect_list = redirect_list

#     def redirect_request(self, req, fp, code, msg, headers, newurl):
#         redirect = (code, newurl)
#         self.redirect_list.append(redirect)
#         return urllib.request.Request(newurl)


class RedirectionHandler(urllib2.HTTPRedirectHandler):
    """ Overrides RedirectionHandler to output 
    the number of redirects"""

    def __init__(self, redirect_list):
        self.redirect_list = redirect_list

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirect = (code, newurl)
        self.redirect_list.append(redirect)
        return urllib2.Request(newurl)


def redirect_urls(inputq, total_q, indb_q, todb_q):
    """ Updated: input remains the same, however the output gets
    written to the database """
    """ Takes the list of Url's as input and writes
    the input record and the number of redirects to a file"""
    
    db_conn = sqa.connect_db()
    Session = sessionmaker(bind=db_conn) 
    session = Session()

    # print "rurls", inputq.qsize()
    # while not inputq.empty():
    while True:
        try:
            item = inputq.get_nowait()
        except Exception, e:
            print "Ye", e
            session.close()
            return

        # print "qsize", inputq.qsize()
        with lock1:
        # if not total_q.empty():
            total_q.put(total_q.get() + 1)
        # print "sent"
        try:
            rec = json.loads(item)
        except:
            print "json error"
        tc_url = rec['twit_url']
        ex_url = rec['expanded_url']
        print "item", ex_url
        # print "queue", ex_url
        # with open("log",'a') as f:
        #     f.write("%s\n" %ex_url)

        try:
            ex_url = ex_url.lower()  # Expect the encoding errors here, unicode string
        except Exception, e:
            logging.debug("terminating %s" %exp_url)
            # print "Terminating: ", exp_url, e
            inputq.task_done()
            continue
            # self.terminate()

        # url processing 
        if ex_url.find('http://') == -1:
            dom = ex_url
        else:
            dom = ex_url
            #exp_url =  http://via.me/-9u5qho0" # normal urls
            dom = dom.split('/')[2]
            # exp_url = http://biebervideo55.tk?=irgvhrxr, no / at the end, abnormal urls
            if dom.find('?') != -1:
                dom = dom.split('?')[0]
        print "lexical passed"
        # Check whether it exists in the database
        # logging.debug('Querying  %s' %dom)        
        print "lexical passed2"
        in_domains = session.query(sqa.Domains).filter(sqa.Domains.domain_name == dom).all()
        logging.debug('Query Done %s' %dom)
        logging.debug('Querying %s' %dom)
        in_exp_urls = session.query(sqa.ExpUrls).filter(sqa.ExpUrls.exp_url == ex_url).all()
        logging.debug('Query Done %s' %dom)
        if not in_domains:
            domain = sqa.Domains(domain_name=dom)
            logging.debug('Adding %s' %dom)
            session.add(domain)
            session.commit()
            print "Added in Domains"

        if not in_exp_urls:
            url_rec = sqa.ExpUrls(exp_url=ex_url,tco_url=tc_url)
            logging.debug('Adding %s' %dom)
            session.add(url_rec)
            session.commit()
            print "Added in exp_urls"
        else:
            logging.debug('Querying %s' %dom)
            db_record = session.query(sqa.UrlRecords.num_redir).filter(sqa.UrlRecords.exp_url == ex_url).all()
            logging.debug('Query Done %s' %dom)
            # db_record = session.query(sqa.ExpUrls).filter(sqa.ExpUrls.exp_url == ex_url).all()
            # print ex_url, db_record
            # sys.exit()
            if db_record:
                print "Already Present ", ex_url
                # if not indb_q.empty():
                with lock2:
                    indb_q.put(indb_q.get() + 1)

                inputq.task_done()
                continue
                
                # self.terminate()

        # request = urllib.request.Request(ex_url)
        # print "Processing ", ex_url
        request = urllib2.Request(ex_url)
        redir_list = []
        redirect_list = []
        opener = urllib2.build_opener(RedirectionHandler(redir_list))
        tstamp = sqa.convert_timestr(rec['created_at'])
        try:
            s = opener.open(ex_url)
        except Exception, e:
            print "opener_exception", e
        finally:
            # rec['redirect_list'] = redir_list
            num_redirects = len(redir_list)
            for i in range(0,len(redir_list)):
                redirect_list.append(urllib.quote_plus(redir_list[i][1].encode('iso-8859-1')))

            redir_str = '#'.join(redirect_list)
            # print ex_url, redirect_list
            # if not todb_q.empty():
            with lock3:
                todb_q.put(todb_q.get() + 1)
            # Have the entry, fill the database and the corresponding tables
            if 'geo' in rec:
                geo = rec['geo']
                coods = str(geo['coordinates'])
                db_rec = sqa.UrlRecords(exp_url = ex_url,
                                        tco_url = tc_url, 
                                        creation_tstamp = tstamp, 
                                        num_redir = num_redirects,
                                        coordinates = coods,
                                        redir_list = redir_str)
                logging.debug('Adding %s' %dom)
                session.add(db_rec)
                session.commit()

            else:
                db_rec = sqa.UrlRecords(exp_url = ex_url,
                                        tco_url = tc_url, 
                                        creation_tstamp = tstamp, 
                                        num_redir = num_redirects,
                                        redir_list = redir_str)

                # print "########## Written to db##############", ex_url
                logging.debug('Adding %s' %dom)
                session.add(db_rec)
                session.commit()
                
        inputq.task_done()

    session.commit()
    session.close()

    
class ParseTwitter(object):
    """ Takes a json file, and outputs the date, url, and geolocation if any"""
    
    def __init__(self, jsonf, errfile, outq):
        # print "call1"
        self.json = open(jsonf, 'r')
        self.errf = open(errfile,'w')
        self.outq = outq
        self.count = 0

    def __del__(self):
        self.json.close()
        self.errf.close()
        
    def run(self):
        print "beign coa"
        while True:
            # try:
            #     line = self.json.readline()
            #     if not len(line):
            #         break
            # except:
            #     continue
            self.count += 1
            line = self.json.readline()
            if not len(line):
                break
            
            if self.count % 10 == 0:
                pass
                # pass
                # print self.count
            try:
                rec = json.loads(line)
            except :
                continue
            # print (rec)
            if len(rec) > 1:
                # print  rec["created_at"], rec["text"], rec["geo"]
                if rec['entities']:
                    if rec['entities']['urls']:
                        json_rec = {}
                        json_rec['created_at'] = rec['created_at']
                        json_rec['twit_url'] = rec['entities']['urls'][0]['url']
                        json_rec['expanded_url'] = rec['entities']['urls'][0]['expanded_url']
                        # print ( rec['created_at'],rec['entities']['urls'][0] 
                        #         end="\n",file = self.op)
                        if rec['geo']:
                            json_rec['geo'] = rec['geo']
                            # print ( rec['geo'], file = self.op)
                        json_dump = json.dumps(json_rec)
                        if not self.outq.full():
                            # print self.outq.qsize()
                            self.outq.put(json_dump)                     
                        # print "Entering ", json_rec['expanded_url']
                        # print (json_dump, end="\n", file = self.op)
                    else:
                        if 'media' in rec['entities']:
                            json_rec = {}
                            if rec['entities']['media']:
                                media = rec['entities']['media'][0]
                                json_rec['created_at'] = rec['created_at']
                                json_rec['twit_url'] = media['url']
                                json_rec['expanded_url'] = media['expanded_url']
                                # print (rec['created_at'],media['display_url'],
                                #        media['expanded_url'],media['url'],
                                #        end = "\n", file = self.op)
                                if rec['geo']:
                                    json_rec['geo'] = rec['geo']
                            json_dump = json.dumps(json_rec)
                            # print "Entering ", json_rec['expanded_url']
                            if not self.outq.full():
                                # print self.outq.qsize()
                                self.outq.put(json_dump)
                            # print "en2:", json_rec['expanded_url']
                # print (rec['geo'], end="\n", file = self.op)
                else:
                    self.errf.write('%s' %(rec))
                    # pass
                    
                    # print (rec, file = sys.stdout)
        # except Exception e :
        #     print (line, e)
        #     pass
                                   

class Statistics(threading.Thread):

    def __init__(self, inputq, total_q, indb_q, todb_q):
        # print "init called"
        logging.debug(" stats init called")
        self.total_q = total_q
        self.inputq = inputq
        self.indb_q = indb_q
        self.todb_q = todb_q
        threading.Thread.__init__(self)

    def run(self):
        logging.debug(" stats run called")
        with open("stats.log",'a') as f:
            # while not self.inputq.empty():
            while True:
                print  >>f, "URLS:\tTotal\tIN DB\t TO DB\n"
                with lock1:
                    tq = self.total_q.get()
                    self.total_q.put(tq)

                    # if not indb_q.empty():
                with lock2:
                    inq = self.indb_q.get()
                    self.indb_q.put(inq)

                # if not todb_q.empty():
                with lock3:
                    toq = self.todb_q.get()
                    self.todb_q.put(toq)

                print >>f, '\t'+str(tq)+'\t'+str(inq)+'\t'+str(toq)


class FileIoUpd(object):
    """ Makes an input queue for input url records
    """

    def __init__(self, jsonf, errf, inputq):
        self.jsonf = jsonf
        self.errf = errf
        self.count = 0
        self.inputq = inputq

    # def __del__(self):
    #     self.jsonf.close()
    #     self.errf.close()

    def run(self):

        total_q = multiprocessing.Queue(1)
        indb_q = multiprocessing.Queue(1)
        todb_q = multiprocessing.Queue(1)

        total_q.put(0)
        indb_q.put(0)
        todb_q.put(0)

        procs_out = []
        
        # producer_thr = threading.Thread(
        #     target=ParseTwitter, args = 
        #     (self.jsonf, self.errf, self.inputq))
        
        # producer_thr.start()
        
        p_t = ParseTwitter(self.jsonf, self.errf, self.inputq)
        p_t.run()

        print self.inputq.qsize()

        stats = Statistics(self.inputq, total_q, indb_q, todb_q)
        stats.daemon = True
        stats.start()
          
        procs_out = [multiprocessing.Process(target = 
                                             redirect_urls,
                                             args = (self.inputq,
                                                     total_q, indb_q, todb_q,))
                     for i in range(0,8)]

        [i.start() for i in procs_out]
        self.inputq.join()
        print "Joined Queue"
        [i.join() for i in procs_out]
        
        
class FileIo(object):
    """ Makes two(input,output) queues for input url records
    and output url entries"""
    """ Old class not used anymore """
    def __init__(self, inputf, outputf):
        self.inputf = open(inputf, 'r', encoding = 'UTF-8')
        self.outputf = open(outputf, 'w', encoding = 'UTF-8')
    
    def __del__(self):
        self.inputf.close()
        self.outputf.close()

    def run(self):
        inputq = multiprocessing.Queue(1000)
        outputq = multiprocessing.Queue(1000)
        
        for line in self.inputf:
            inputq.put(line)
    
        procs = []
        for i in range(1,8):
            p = multiprocessing.Process(target = RedirectUrls,
                                        args = (inputq,outputq))
            proces.append(p)
            p.start()

        while not outputq.empty():
            item = outputq.get()
            # print (item, end = "\n", file=self.outputf)

        for p in procs:
            p.join()


class TestRedirect(object):
    """ Class for user testing"""

    def __init__(self, url):
        self.url = url

    def run(self):
        request = urllib2.Request(self.url)
        redirect_list = []
        opener = urllib2.build_opener(RedirectionHandler(redirect_list))
        try:
            s = opener.open(self.url)
        except:
            pass
        finally:
            print redirect_list
        
def main():
    logging.debug("main called")

    db_conn = sqa.connect_db()
    Session = sessionmaker(bind=db_conn) 
    session = Session()
    
    count1 = session.query(sqa.UrlRecords).count()
    session.close()
    # in_queue = multiprocessing.Queue()
    in_queue = Queue()
    # pt = ParseTwitter(sys.argv[1], sys.argv[2], in_queue)
    # pt.parse()
    url_dump = FileIoUpd(sys.argv[1], sys.argv[2], in_queue)
    url_dump.run()

    count2 = session.query(sqa.UrlRecords).count()

    print "Records Added: ", count2-count1
    # redirects = RedirectUrls(sys.argv[1],sys.argv[2])
    # redirects.run()
    # redirects = TestRedirect(sys.argv[1])
    # redirects.run()


if __name__ == "__main__":
    main()

