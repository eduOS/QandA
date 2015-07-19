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
import argparse

HOMEPAGE = 'http://www.abc.net.au/tv/qanda/past-programs-by-date.htm'

parser = argparse.ArgumentParser(description = 'dump data from QandA to database.')
parser.add_argument('dbserver', nargs='?', default='localhost',help='input your database server.')
parser.add_argument('dbuser', nargs='?', default='root',help='input your database username.')
parser.add_argument('dbpwd', nargs='?', default='0123456789',help='input your database password.')
parser.add_argument('dbname', nargs='?', default='qanda',help='input your database database name.')
parser.add_argument('soup_dir', nargs='?', default=default='./soupfiles',help='set the soup directory')
parser.add_argument('-d','--delay',default=1,type=float,help='time to sleep between downloads, in seconds')

global args
args = parser.parse_args()

con = mdb.connect(args.dbserver,args.dbuser,args.dbpwd,args.dbname)
cur = con.cursor()

fakeID = 1000000
global today
today = time.strftime("%Y-%m-%d")
# update automatically a week later / every Tuesday

def init_database():
    cur.execute('DROP TABLE IF EXISTS hentry')
    cur.execute(
        "CREATE TABLE `hentry` ("
        "   `epiShortNumber` VARCHAR(10) NOT NULL,"
        "   `hentryDate` VARCHAR(30) NOT NULL,"
        "   `entryTitle` VARCHAR(60) NOT NULL,"
        "   `bookmark` VARCHAR(300) NOT NULL,"
        "   `videoLink` varchar(100),"
        "   PRIMARY KEY (`epiShortNumber`)"
        ") ENGINE=INNODB")
    
    cur.execute(
        "CREATE TABLE `qanda` ("
        "   `questionNumber` VARCHAR(12),"
        "   `questions` VARCHAR(15000) NOT NULL,"
        "   `transcript` TEXT NOT NULL,"
        "   PRIMARY KEY (`questionNumber`)"
        ") ENGINE=INNODB")

    cur.execute('DROP TABLE IF EXISTS henPan')
    cur.execute("""CREATE TABLE henPan(epiShortNumber VARCHAR(10) NOT NULL, \
                                     panelNameTag VARCHAR(50) NOT NULL)""")
    
    cur.execute('DROP TABLE IF EXISTS panellist')
    cur.execute('CREATE TABLE panellist(panelNameTag VARCHAR(50) NOT NULL PRIMARY KEY, panelName VARCHAR(40) NOT NULL, panelPhotoNumber VARCHAR(10), panelProfile VARCHAR(6000) NOT NULL)')
    #                                    panellIdentity VARCHAR(40), \

def local_dump(text,fname):
    with open(fname) as f:
        f.write(text)

def isoutdated(filename):
    # 7*24*3600 should be replaced by the latest time in database
    should_time = time.time() - 7*24*3600

    try:
        file_mod_time = os.path.getatime('programs-by-date')
    except OSError as e:
        if e.errno == 2:
            text = requests.get(HOMEPAGE).text
            local_dump(text,'programs-by-date')
            init_database()
            #if this file doesn't exist then initiate the database, it's too dogmatic
            file_mod_time = os.path.getatime('programs-by-date')
        else:
            raise

    # if it's outdated then refresh. That is, reload the homepage and update the database
    if should_time > file_mod_time:
        text = requests.get(HOMEPAGE).text
        local_dump(text,'programs-by-date')
        pro_soup = BeautifulSoup(text)
        entries = pro_soup.find_all('div', class_ = 'hentry')
        date = entry.find('span', class_ = 'date').string.encode('UTF-8')
        return entries
    else:
        return False

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
#    @staticmethod
#    def init_database():
#        # create table
#        create_tables()
#        # retrieve all entries from the home page
#        hentries = retrieve_entries()
        
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
