#coding=utf-8
#!/usr/bin/env python
# define source file encoding, see: http://www.python.org/dev/peps/pep-0263
# -*- coding: utf-8 -*-

from urllib2 import urlopen
from bs4 import BeautifulSoup
import re,sys
import requests
import time
import MySQLdb as mdb

con = mdb.connect('localhost',"root","104064",'QandA')
cur = con.cursor()

time.strftime("%Y-%m-%d")
# update automatically a week later / every Tuesday

pro_soup = BeautifulSoup(requests.get('http://www.abc.net.au/tv/qanda/past-programs-by-date.htm').text)

hentries = pro_soup.find_all('div', class_ = 'hentry')

cur.execute('DROP TABLE IF EXISTS hentry')
cur.execute(
    "CREATE TABLE `hentry` ("
    "   `hentryDate` VARCHAR(30) NOT NULL,"
    "   `entryTitle` VARCHAR(60) NOT NULL,"
    "   `bookmark` VARCHAR(300) NOT NULL,"
    "   `videoLink` varchar(100),"
    "   `questions` TEXT NOT NULL,"
    "   `transcript` MEDIUMTEXT NOT NULL,"
    "   PRIMARY KEY (`hentryDate`)"
    ") engine=InnoDB")

cur.execute('DROP TABLE IF EXISTS henPan')
cur.execute("""CREATE TABLE henPan(hentryDate VARCHAR(30) NOT NULL, \
                                 panellID VARCHAR(10) NOT NULL)""")

cur.execute('DROP TABLE IF EXISTS panellist')
cur.execute('CREATE TABLE panellist(panellID VARCHAR(10) NOT NULL PRIMARY KEY, panelName VARCHAR(40) NOT NULL, panellProfile VARCHAR(5000) NOT NULL)')
#                                    panellIdentity VARCHAR(40), \

fakeID = 1000000

try:
    for hentry in hentries:
        date = hentry.find('span', class_ = 'date').string.encode('UTF-8')
        print date
        epi_link = hentry.find('a', class_ = 'details')['href'].encode('UTF-8')
        bookmark = hentry.find('a', class_ = 'entry-title').string.encode('UTF-8')
        #panelIden = find('div', class_ = 'entry-summary').string.encode('UTF-8')

        epi_soup = BeautifulSoup(requests.get(epi_link).text)

        videoLink = epi_soup.find('li', class_ = 'download')
        if videoLink:
            videoLink = videoLink.find('a')['href'].encode('UTF-8')
        else:
            fakeID += 100
            videoLink = str(fakeID)
            print fakeID

        questions = epi_soup.find('div', id = 'questions').text.encode('UTF-8')
        transcript = epi_soup.find('div', id = 'transcript').text.encode('UTF-8')
        sql = 'INSERT INTO hentry VALUES(%s,%s,%s,%s,%s,%s)'
        cur.execute(sql,(date,epi_link,bookmark,videoLink,questions,transcript,))

        presenters = epi_soup.find_all('div', class_ = 'presenter')
        for presenter in presenters:

            ID = presenter.find('img')
            if ID:
                ID = ID['src'][-11:-4]
            else:
                ID = str(fakeID)
                fakeID += 1
                print ID

            name = presenter.find('a').text.encode('UTF-8')

            profile = presenter.find('p').text.encode('UTF-8')

            sql = 'INSERT INTO henPan VALUES(%s,%s)'
            cur.execute(sql,(date,ID,))

            sql = 'SELECT * FROM panellist WHERE PanellID = %s'
            cur.execute(sql,(ID,))
            if not cur.rowcount:
                sql = 'INSERT INTO panellist VALUES(%s,%s,%s)'
                cur.execute(sql,(ID,name,profile,))
except:
    con.commit()
    cur.close()
    con.close()
    print 'error occurs', date
    sys.exit(0)

con.commit()
cur.close()
con.close()

# cannot fetch data in 2008
