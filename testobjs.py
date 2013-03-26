#!/usr/bin/env python

import sqlalc_utilities as sqa
from sqlalchemy.orm import session, sessionmaker

def main():
    db_conn = sqa.connect_db()
    Session = sessionmaker(bind=db_conn)
    session = Session()
    
    res = session.query(sqa.UrlRecords).filter(sqa.UrlRecords.num_redir>='5').all()
    for i in res:
        print i.exp_url
        


if __name__ == "__main__":
    main()
