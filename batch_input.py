#!/usr/bin/env python

import sys
import json
import time
import codecs
import calendar
import psycopg2
import urllib

class BatchInput(object):
   ''' batch input for psql from flatfiles'''
   
   def __init__(self, datafile):
      self.dfile = open(datafile, 'r')
      
   def setup(self, db, user, passwd):
      """ connects to the db"""
      return psycopg2.connect(database=db, user=user, password=passwd)

   def run(self, conn):
      cur = conn.cursor()
      cur.copy_from(self.dfile, 'url_recs_upd', sep='\v',
                    columns=('creation_tstamp', 'exp_url', 'tco_url',
                             'num_redir', 'coordinates', 'redir_list' ))
      conn.commit()
      
   def run_urls(self, conn):
      cur = conn.cursor()
      cur.copy_from(self.dfile, 'urls', sep=' ',
                    columns=('exp_url', 'resolved', ))
      conn.commit()
         
            
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


class CovertInput(object):
   ''' converts the json object file to \t delimited file with the following
   changes
   1. concatenates the list with # as the separator
   2. removes the error codes
   Everything else remains the same '''

   def __init__(self, infile, outfile, errfile):
      self.infile = infile
      self.outfile = outfile
      self.errfile = errfile

   def run(self):
      count = 0
      count2 = 0
      with codecs.open(self.errfile, 'w') as h:
         with codecs.open(self.outfile, 'w', encoding='UTF-8') as g:
            with codecs.open(self.infile,'r') as f:
               for line in f:
                  count += 1
                  if count % 100000 == 0:
                     print count
                  try:
                     l = json.loads(line)
                  except Exception, e:
                     print >>h, line, e
                     count2 += 1
                     if count2 % 100 == 0:
                        print count2
                     continue
                  tstamp = convert_timestr(l['created_at'])
                  redir_list = l['redirect_list']
                  r_list = []
                  for i in range(0,len(redir_list)):
                     r_list.append(urllib.quote_plus(redir_list[i][1].encode('iso-8859-1')))

                  redir_str = '#'.join(r_list)

                  if 'geo' in l:
                     geo = l['geo']
                     coods = str(geo['coordinates'])
                     # url's contains all kinds of chars, including space and \t's
                     # so, we choose a custom delim of \v
                     g.write('%s\v%s\v%s\v%d\v%s\v%s\n' 
                             %(str(tstamp), str(l['expanded_url']), 
                               str(l['twit_url']), int(l['num_redirects']), 
                               coods, redir_str ))
                  else:
                     # url's contains all kinds of chars, including space and \t's
                     # so, we choose a custom delim of \v
                     g.write('%s\v%s\v%s\v%d\v%s\v%s\n' 
                             %(str(tstamp), str(l['expanded_url']), 
                               str(l['twit_url']), int(l['num_redirects']),0,
                               redir_str ))

def main():
   
   # ci = ConvertInput(sys.argv[1], sys.argv[2], sys.argv[3])
   # ci.run()

   user = 'vjain'
   passwd = 'vj@in123'
   # host = 'localhost'
    # port = '5432'
   database = 'twitter_urls'

   bi = BatchInput(sys.argv[1])
   conn = bi.setup(database, user, passwd)
   bi.run_urls(conn)

if __name__ == "__main__":
   sys.exit(main())
