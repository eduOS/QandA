#coding=utf-8
#!/usr/bin/env python
# define source file encoding, see: http://www.python.org/dev/peps/pep-0263
# -*- coding: utf-8 -*-

from urllib2 import urlopen
from bs4 import BeautifulSoup
import re,sys
import requests
import time
from datetime import datetime
import MySQLdb as mdb

con = mdb.connect('localhost',"root","104064",'QandA')
cur = con.cursor()

fakeID = 1000000
time.strftime("%Y-%m-%d")
# update automatically a week later / every Tuesday

def create_tables():
    cur.execute('DROP TABLE IF EXISTS hentry')
    cur.execute(
        "CREATE TABLE `hentry` ("
        "   `epiShortNumber` VARCHAR(10) NOT NULL,"
        "   `hentryDate` VARCHAR(30) NOT NULL,"
        "   `entryTitle` VARCHAR(60) NOT NULL,"
        "   `bookmark` VARCHAR(300) NOT NULL,"
        "   `videoLink` varchar(100),"
        "   `questions` TEXT NOT NULL,"
        "   `transcript` MEDIUMTEXT NOT NULL,"
        "   PRIMARY KEY (`shortNumber`)"
        ") engine=InnoDB")
    
    cur.execute('DROP TABLE IF EXISTS henPan')
    cur.execute("""CREATE TABLE henPan(epiShortNumber VARCHAR(10) NOT NULL, \
                                     panelNameTag VARCHAR(50) NOT NULL)""")
    
    cur.execute('DROP TABLE IF EXISTS panellist')
    cur.execute('CREATE TABLE panellist(panelNameTag VARCHAR(50) NOT NULL PRIMARY KEY, panelName VARCHAR(40) NOT NULL, panelPhotoNumber VARCHAR(10), panelProfile VARCHAR(5000) NOT NULL)')
    #                                    panellIdentity VARCHAR(40), \

def retrieve_entries():
    pro_soup = BeautifulSoup(requests.get('http://www.abc.net.au/tv/qanda/past-programs-by-date.htm').text)
    entries = pro_soup.find_all('div', class_ = 'hentry')
    # pass one date and get all new entries only
    return entries

def dump_the_hentry(hentry):
    'insert all into hentry table'
    date = hentry.find('span', class_ = 'date').string.encode('UTF-8')
    date = datetime.strptime(date,'%A %d $B, %Y')
    epi_link = hentry.find('a', class_ = 'details')['href'].encode('UTF-8')
    epiShortNumber = epi_link[-11:-4]
    bookmark = hentry.find('a', class_ = 'entry-title').string.encode('UTF-8')
    epi_soup = BeautifulSoup(requests.get(epi_link).text)
    videoLink = epi_soup.find('li', class_ = 'download')

    if videoLink:
        videoLink = videoLink.find('a')['href'].encode('UTF-8')
    else:
        fakeID += 100
        videoLink = str(fakeID)

    questions = epi_soup.find('div', id = 'questions').text.encode('UTF-8')
    transcript = epi_soup.find('div', id = 'transcript').text.encode('UTF-8')

    sql = 'INSERT INTO hentry VALUES(%s,%s,%s,%s,%s,%s,%s)'
    cur.execute(sql,(epiShortNumber,date,epi_link,bookmark,videoLink,questions,transcript,))

    return epi_soup

def dump_panellist(epi_soup):

    presenters = epi_soup.find_all('div', class_ = 'presenter')

    for presenter in presenters:
        panel_panel_ID = presenter.find('img')
        if panel_ID:
            panel_ID = ID['src'][-11:-4]
        else:
            panel_ID = str(fakeID)
            fakepanel_ID += 1
        name = presenter.find('a').text.encode('UTF-8')
        profile = presenter.find('p').text.encode('UTF-8')

        sql = 'INSERT INTO henPan VALUES(%s,%s)'
        cur.execute(sql,(epi_ID,panel_ID,))
        # the table henpan shoulbe be modified: making episode number and panellist's panel_ID as foreign key

        sql = 'SELECT * FROM panellist WHERE panellID = %s'
        cur.execute(sql,(panel_ID,))
        if not cur.rowcount:
            sql = 'INSERT INTO panellist VALUES(%s,%s,%s)'
            cur.execute(sql,(panel_ID,name,profile,))


class QandA:
    @staticmethod
    def init_database():
        # create table
        create_tables()
        # retrieve all entries from the home page
        hentries = retrieve_entries()
        
        # insert all data
        
        try:
            # fetch details of each episode via the link from each entry
            for hentry in hentries:
                #panelIden = find('div', class_ = 'entry-summary').string.encode('UTF-8')
                entry_info = fetch_info_from_entry(hentry)

                epi_info = fetch_from_epi(entry_info['epi_link'])

        
        
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
