#!/usr/bin/env python

import urllib.request
import sys
import json
from multiprocessing import Queue, Process
from sqlalchemy.orm import Session, sessionmaker
import sqlalc_utilities as sqa

class RedirectionHandler(urllib.request.HTTPRedirectHandler):
    """ Overrides RedirectionHandler to output 
    the number of redirects"""

    def __init__(self, redirect_list):
        self.redirect_list = redirect_list

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirect = (code, newurl)
        self.redirect_list.append(redirect)
        return urllib.request.Request(newurl)

def RedirectUrls(inputq, outputq):
    """ Takes the list of Url's as input and writes
    the input record and the number of redirects to a file"""
    
    db_conn = sqa.connect_db()
    Session = sessionmaker(bind=db_conn) 
    session = Session()

    item = inputq.get()
    rec = json.loads(item)
    tco_url = rec['twit_url']
    exp_url = rec['expanded_url']
    print "Original: ", tco_url, exp_url

    try:
        # exp_url = str.lower(urllib.unquote_plus(exp_url)) 
        exp_url = str.lower(exp_url)  # Expect the encoding errors here
    except:
        print "Terminating: ", exp_url
        self.terminate()
    
    # URL processing 
    if exp_url.find('http://') == -1:
        dom = exp_url
    else:
        #exp_url =  http://via.me/-9u5qho0" # normal urls
        dom = dom.split('/')[2]
        # exp_url = http://biebervideo55.tk?=irgvhrxr, no / at the end, abnormal urls
        if dom.find('?') != -1:
            dom = dom.split('?')[0]
            
    # Check whether it exists in the database
    in_domains = session.query(sqa.Domains).filter(sqa.Domains.exp_url == dom).all()
    in_exp_urls = session.query(sqa.ExpUrls).filter(sqa.ExpUrls.exp_url == exp_url).all()

    if not in_domains:
        domain = sqa.Domains(domain_name=dom)
        session.add(domain)
        print "Added in Domains"
        
    if not in_exp_urls:
        url_rec = sql.ExpUrls(exp_url=exp_url,tco_url=url)
        session.add(url_rec)
        print "Added in exp_urls"
    else:
        db_record = session.query(sqa.UrlRecs).filter(sqa.UrlRecs.exp_url = exp_url).all()
        if db_record.num_redir > 0:
            self.terminate()
    
    request = urllib.request.Request(exp_url)
    redirect_list = []
    opener = urllib.request.build_opener(RedirectionHandler(redirect_list))
    try:
        s = opener.open(exp_url)
    except:
        pass
    finally:
        rec['redirect_list'] = redirect_list
        rec['num_redirects'] = len(redirect_list)
        outputq.put(rec)

class FileIo(object):
    """ Makes two(input,output) queues for input url records
    and output url entries"""

    def __init__(self, inputf, outputf):
        self.inputf = open(inputf, 'r', encoding = 'UTF-8')
        self.outputf = open(outputf, 'w', encoding = 'UTF-8')
    
    def __del__(self):
        self.inputf.close()
        self.outputf.close()

    def run(self):
        inputq = Queue(1000)
        outputq = Queue(1000)
        
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
            print (item, end = "\n", file=self.outputf)

        for p in procs:
            p.join()

# class RedirectUrls(object):
#     """ Takes the list of Url's as input and writes
#     the input record and the number of redirects to a file"""
    
#     def __init__(self, inputf, outputf):
#         self.inputf = open(inputf, 'r', encoding='UTF-8')
#         self.outputf = open(outputf, 'w', encoding='UTF-8')
#         self.count = 0

#     def __del__(self):
#         self.inputf.close()
#         self.outputf.close()

#     def run(self):
#         for line in self.inputf:
#             rec = json.loads(line)
#             url = rec['twit_url']
#             request = urllib.request.Request(url)
#             redirect_list = []
#             opener = urllib.request.build_opener(RedirectionHandler(redirect_list))
#             try:
#                 s = opener.open(url)
#             except:
#                 pass
#             finally:
#                 rec['redirect_list'] = redirect_list
#                 rec['num_redirects'] = len(redirect_list)
#             print (rec, end = '\n', file = self.outputf)

class TestRedirect(object):
    """ Class for user testing"""

    def __init__(self, url):
        self.url = url

    def run(self):
        request = urllib.request.Request(self.url)
        redirect_list = []
        opener = urllib.request.build_opener(RedirectionHandler(redirect_list))
        try:
            s = opener.open(self.url)
        except:
            pass
        finally:
            print (redirect_list)
        
def main():
    
    url_dump = FileIo(sys.argv[1],sys.argv[2])
    url_dump.run()

    # redirects = RedirectUrls(sys.argv[1],sys.argv[2])
    # redirects.run()
    # redirects = TestRedirect(sys.argv[1])
    # redirects.run()


if __name__ == "__main__":
    main()

