#coding=utf-8
#!/usr/bin/env python
# define source file encoding, see: http://www.python.org/dev/peps/pep-0263
# -*- coding: utf-8 -*-

from urllib2 import urlopen
from bs4 import BeautifulSoup as BS
import re,sys,os.path,os,errno
import requests
import time
#from datetime import datetime
from dateutil import parser
import MySQLdb as mdb
import argparse

HOMEPAGE = 'http://www.abc.net.au/tv/qanda/past-programs-by-date.htm'
EPISPAGE = 'http://www.abc.net.au/tv/qanda/txt/s{num}.htm'

parser = argparse.ArgumentParser(description = 'dump data from QandA to database.')
parser.add_argument('dbserver', nargs='?', default='localhost',help='input your database server.')
parser.add_argument('dbuser', nargs='?', default='root',help='input your database username.')
parser.add_argument('dbpwd', nargs='?', default='0123456789',help='input your database password.')
parser.add_argument('dbname', nargs='?', default='qanda',help='input your database database name.')
parser.add_argument('soup_dir', nargs='?', default=default='./soupfiles',help='set the soup directory')
parser.add_argument('-d','--delay',default=1,type=float,help='time to sleep between downloads, in seconds')

global args
args = parser.parse_args()

HFILENAME = args.soup_dir+'/programs-by-date'
EFILENAME = args.soup_dir+'/{num}'

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
        "   `epiLink` VARCHAR(60) NOT NULL,"
        "   `bookmark` VARCHAR(300) NOT NULL,"
        "   `videoLink` varchar(100),"
        "   PRIMARY KEY (`epiShortNumber`)"
        ") ENGINE=INNODB")
    
    cur.execute('DROP TABLE IF EXISTS qanda')
    cur.execute(
        "CREATE TABLE `qanda` ("
        "   `questionNumber` VARCHAR(12) NOT NULL,"
        "   `topic` VARCHAR(30),"
        # questionNumber is the episodenumber appending the question number
        "   `question` VARCHAR(15000) NOT NULL,"
        "   `answers` TEXT NOT NULL,"
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

def get_new_soup():
    pass

def dump_epi(epiShortNumber):
    epi_soup = ''
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        file_handle = os.open(EFILENAME.format(num=epiShortNumber),flags)
    except OSError as e:
        # that episode already exists
        if e.errno == errno.EEXIST:
            with os.fdopen(file_handle,'r') as file_obj:
                epi_soup = BS(f)
        else:
        # something unexpected happened
            raise
    else:
        # file doesn't exist and open the file successfully
        text = requests.get(EPISPAGE.format(num=epiShortNumber)).text
        with os.fdopen(file_handle,'w') as file_obj:
            file_obj.write(text)
        epi_soup = BS(text)

    videoLink = epi_soup.find('li', class_ = 'download')

    if videoLink:
        videoLink = videoLink.find('a')['href'].encode('UTF-8')
        sql = 'UPDATE hentry SET videoLink=%s WHERE epiShortNumber=%s'
        cur.execute(sql,(videoLink,epiShortNumber))

    transcript_soup = epi_soup.find('div', id = 'transcript')
    qandas = re.split('<span id=\"',str(transcript_soup))

    for qanda in qandas[1:]:
        t,a = re.split('</span>',qanda)
        qnumber = t[0:2]
        questionNumber = epiShortNumber+'-'+qnumber
        question = a.split('<br/>')[1].encode('UTF-8')
        topic = t[4:].encode('UTF-8')
        answers = BS(''.join(a.split('<br/>')[2:])).text.encode('UTF-8')
        sql = 'INSERT INTO qanda (questionNumber, topic, question, answers) VALUES(%s,%s,%s,%s)'
        cur.execute(sql,(questionNumber,topic,question,answers))

    dump_panellists()

def dump_entries(entries):
    for entry in entries:
        'insert all into hentry table'
        date = hentry.find('span', class_ = 'date').string.encode('UTF-8')
        date = parser.parse(date).strftime('%Y-%m-%d')
        
        epi_link = hentry.find('a', class_ = 'details')['href'].encode('UTF-8')
        epiShortNumber = epi_link[-11:-4]
        bookmark = hentry.find('a', class_ = 'entry-title').string.encode('UTF-8')
        #all the above are available
        sql = 'INSERT INTO hentry (epiShortNumber, hentryDate, epiLink, bookmark) VALUES(%s,%s,%s,%s)'
        cur.execute(sql,(epiShortNumber,date,epi_link,bookmark))
        dump_epi(epiShortNumber)

def initiate():
    text = requests.get(HOMEPAGE).text
    local_dump(text,HFILENAME)
    init_database()
    #if this file doesn't exist then initiate the database, it's too dogmatic
    remote_soup = BS(remote_text)
    remote_latest_entries = pro_soup.find_all('div', class_ = 'hentry')
    # not return but dump the database
    dump_entries(remote_latest_entries)


def refresh():
    # if the home page is updated
        # 7*24*3600 should be replaced by the latest time in database

    try:
        file_mod_time = os.path.getatime(HFILENAME)
        print 'You updated the database %s ago. Continue?[y/n] ' % str(int((time.time()-file_mod_time)/86400))
        if sys.stdin.read(1) == 'n':
            break
        else:
            # should be detailed further
            pass
        # if the home page is outdated
        with open(HFILENAME) as f:
            local_soup = BS(f)
            local_latest_entry = pro_soup.find('div', class_ = 'hentry')

            local_latest_date = local_latest_entry.find('span', class_ = 'date').string
#                local_latest_date = parser.parse(local_latest_date).strftime('%Y-%m-%d')
            remote_text = requests.get(HOMEPAGE).text
            remote_soup = BS(remote_text)
            remote_latest_entry = pro_soup.find('div', class_ = 'hentry')
            remote_latest_entries = pro_soup.find_all('div', class_ = 'hentry')
            remote_latest_date = local_latest_entry.find('span', class_ = 'date').string

            nu_new = int((parser.parse(remote_latest_date)-parser.parse(local_latest_date))/604800)
            if nu_new > 0:
                dump_entries(remote_latest_entries[:nu_new])
            else:
                print 'Nothing new.'
                break

    except OSError as e:
    # if no home page
        if e.errno == 2:
            print 'initiate the database'
            initiate()
        else:
            raise

    # if it's outdated then refresh. That is, reload the homepage and update the database
    else:
        print 'unexpected error'

def dump_panellists(epiShortNumber):
    try:
        with open(EFILENAME.format(num=epiShortNumber),'r') as f:
            epi_soup = BS(f)
    except:
        print epiShortNumber + 'does not exist locally'

    presenters = epi_soup.find_all('div', class_ = 'presenter')

    for presenter in presenters:
        panel_NAME = presenter.find('a')['name'].encode('UTF-8')
        panel_name = presenter.find('a').text.encode('UTF-8')

        panel_pic_ID = presenter.find('img')
        if panel_pic_ID:
            panel_pic_ID = panel_pic_ID['src'][-11:-4]

        panel_profile = presenter.find('p').text.encode('UTF-8')

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
