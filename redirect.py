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
from multiprocessing import Queue, Process
from sqlalchemy.orm import Session, sessionmaker
lock = threading.Lock()

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

    if not inputq.empty():
        item = inputq.get()

        with lock:
        # if not total_q.empty():
            total_q.put(total_q.get() + 1)
            
        rec = json.loads(item)
        tc_url = rec['twit_url']
        ex_url = rec['expanded_url']
        with open("log",'a') as f:
            f.write("%s\n" %ex_url)

        try:
            ex_url = ex_url.lower()  # Expect the encoding errors here, unicode string
        except Exception, e:
            print "Terminating: ", exp_url, e
            sys.exit()
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

        # Check whether it exists in the database
        in_domains = session.query(sqa.Domains).filter(sqa.Domains.domain_name == dom).all()
        in_exp_urls = session.query(sqa.ExpUrls).filter(sqa.ExpUrls.exp_url == ex_url).all()

        if not in_domains:
            domain = sqa.Domains(domain_name=dom)
            session.add(domain)
            # print "Added in Domains"

        if not in_exp_urls:
            url_rec = sqa.ExpUrls(exp_url=ex_url,tco_url=tc_url)
            session.add(url_rec)
            # print "Added in exp_urls"
        else:
            db_record = session.query(sqa.UrlRecords.num_redir).filter(sqa.UrlRecords.exp_url == ex_url).all()
            # db_record = session.query(sqa.ExpUrls).filter(sqa.ExpUrls.exp_url == ex_url).all()
            # print ex_url, db_record
            # sys.exit()
            if db_record:
                print "Already Present ", ex_url
                if not indb_q.empty():
                    indb_q.put(indb_q.get() + 1)
                sys.exit()
                # self.terminate()

        # request = urllib.request.Request(ex_url)
        print "Processing ", ex_url
        request = urllib2.Request(ex_url)
        redir_list = []
        redirect_list = []
        opener = urllib2.build_opener(RedirectionHandler(redir_list))
        tstamp = sqa.convert_timestr(rec['created_at'])
        try:
            s = opener.open(ex_url)
        except:
            pass
        finally:
            # rec['redirect_list'] = redir_list
            num_redirects = len(redir_list)
            for i in range(0,len(redir_list)):
                redirect_list.append(urllib.quote_plus(redir_list[i][1].encode('iso-8859-1')))
                
            redir_str = '#'.join(redirect_list)
            # print ex_url, redirect_list
            if not todb_q.empty():
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
                session.add(db_rec)
            else:
                db_rec = sqa.UrlRecords(exp_url = ex_url,
                                        tco_url = tc_url, 
                                        creation_tstamp = tstamp, 
                                        num_redir = num_redirects,
                                        redir_list = redir_str)
                
                # print "########## Written to db##############", ex_url
                session.add(db_rec)
    
    session.commit()
    sys.exit()
        # outputq.put(rec)

class ParseTwitter(object):
    """ Takes a json file, and outputs the date, url, and geolocation if any"""
    
    def __init__(self, jsonf, errfile, outq):
        self.json = open(jsonf, 'r')
        self.errf = open(errfile,'w')
        self.outq = outq
        self.count = 0

    def __del__(self):
        self.json.close()
        self.errf.close()
        
    def parse(self):
        #try:
        while True:
            try:
                line = self.json.readline()
                if not len(line):
                    break
            except:
                continue
        # for line in self.json:
            self.count += 1
            if self.count % 10 == 0:
                print self.count
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
                            self.outq.put(json_dump)
                            # print (rec['geo'], end="\n", file = self.op)
                else:
                    self.errf.write('%s' %(rec))
                    # pass
                    
                    # print (rec, file = sys.stdout)
        # except Exception e :
        #     print (line, e)
        #     pass
                                   

def statistics(total_q, indb_q, todb_q):
    print "URLS:\tTotal\tIN DB\t TO DB\n"
    while True:
        # if not total_q.empty():
        with lock:
            tq = total_q.get()
            total_q.put(tq)

        if not indb_q.empty():
            inq = indb_q.get()
            indb_q.put(inq)

        if not todb_q.empty():
            toq = todb_q.get()
            todb_q.put(toq)

        print '\t'+str(tq)+'\t'+str(inq)+'\t'+str(toq)

        time.sleep(10)

class FileIoUpd(object):
    """ Makes an input queue for input url records
    """

    def __init__(self, inputq):
        # self.inputf = codecs.open(inputf, 'r', encoding = 'UTF-8')
        self.inputq = inputq

    def run(self):

        total_q = multiprocessing.Queue(1)
        indb_q = multiprocessing.Queue(1)
        todb_q = multiprocessing.Queue(1)

        total_q.put(0)
        indb_q.put(0)
        todb_q.put(0)

        procs = []
        while not self.inputq.empty():
            
            # stats = threading.Thread(target = statistics, 
            #                          args = (total_q, indb_q, todb_q))
            stats = multiprocessing.Process(target = statistics, 
                                     args = (total_q, indb_q, todb_q,))
            stats.start()
            
            for i in range(1,8):
                p = multiprocessing.Process(target = redirect_urls,
                                            args = (self.inputq, total_q, indb_q, todb_q,))
                procs.append(p)
                p.start()

            for p in procs:
                p.join()
        

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
    
    in_queue = multiprocessing.Queue(1000)
    pt = ParseTwitter(sys.argv[1], sys.argv[2], in_queue)
    pt.parse()
    url_dump = FileIoUpd(in_queue)
    url_dump.run()

    # redirects = RedirectUrls(sys.argv[1],sys.argv[2])
    # redirects.run()
    # redirects = TestRedirect(sys.argv[1])
    # redirects.run()


if __name__ == "__main__":
    main()

