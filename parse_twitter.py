#!/usr/bin/env python 
import json
import sys

class ParseTwitter(object):
    """ Takes a json file, and outputs the date, url, and geolocation if any"""
    
    def __init__(self, jsonf, output):
        self.json = open(jsonf, 'r')
        self.op = open(output, 'w')
        self.count = 0

    def __del__(self):
        self.json.close()
        self.op.close()
        
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
            if not self.count % 100000:
                print (self.count)
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
                        print (json_dump, end="\n", file = self.op)
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
                            print (json_dump, end="\n", file = self.op)
                            # print (rec['geo'], end="\n", file = self.op)
                else:
                    # pass
                    print (rec, file = sys.stdout)
        # except Exception e :
        #     print (line, e)
        #     pass
                                   
def main():
    twitter_data = ParseTwitter(sys.argv[1],sys.argv[2])
    twitter_data.parse()

if __name__ == "__main__":
    main()
