#!/usr/bin/env python

import sys
import json
# import unicode

def main():
    tweet_file = open(sys.argv[1],'r')
    op_file = open(sys.argv[2],'w')

    count = 0
    for line in tweet_file:
        count += 1 
        temp =  json.loads(line)
        if "text" in temp:
            # print temp["text"]
            op_file.write("%s\n" %temp["text"].encode('utf8'))
        else:
            pass
        if count % 1000 == 0:
            print count
        # for c in temp:
        #     print ord(c)
        
    tweet_file.close()
    op_file.close()

if __name__ == "__main__":
    main()
