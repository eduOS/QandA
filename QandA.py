#coding=utf-8
#!/usr/bin/env python
# define source file encoding, see: http://www.python.org/dev/peps/pep-0263
# -*- coding: utf-8 -*-

from urllib2 import urlopen
from bs4 import BeautifulSoup as BS
import re,sys,os.path,os,errno
import requests
import time
from dateutil import parser
import MySQLdb as mdb
import argparse

HOMEPAGE = 'http://www.abc.net.au/tv/qanda/past-programs-by-date.htm'
EPISPAGE = 'http://www.abc.net.au/tv/qanda/txt/s{num}.htm'

argparser = argparse.ArgumentParser(description = 'dump data from QandA to database.')
argparser.add_argument('dbpwd', nargs='?', default='0123456789',help='input your database password.')
argparser.add_argument('dbserver', nargs='?', default='localhost',help='input your database server.')
argparser.add_argument('dbuser', nargs='?', default='root',help='input your database username.')
argparser.add_argument('dbname', nargs='?', default='QandA',help='input your database database name.')
argparser.add_argument('soup_dir', nargs='?', default='./soupfiles',help='set the soup directory')
argparser.add_argument('-d','--delay',default=1,type=float,help='time to sleep between downloads, in seconds')

global args
args = argparser.parse_args()

HFILENAME = args.soup_dir+'/programs-by-date'
EFILENAME = args.soup_dir+'/{num}'

con = mdb.connect(args.dbserver,args.dbuser,args.dbpwd,args.dbname)
cur = con.cursor()

def init_database():
    cur.execute('DROP TABLE IF EXISTS hentry')
    cur.execute(
        "CREATE TABLE `hentry` ("
        "   `id` SMALLINT NOT NULL AUTO_INCREMENT,"
        "   `epiShortNumber` VARCHAR(12) NOT NULL,"
        "   `hentryDate` VARCHAR(20) NOT NULL,"
        "   `epiLink` VARCHAR(60) NOT NULL,"
        "   `bookmark` VARCHAR(300) NOT NULL,"
        "   `videoLink` varchar(100),"
        "   PRIMARY KEY (`id`)"
        ") ENGINE=INNODB")
    
    cur.execute('DROP TABLE IF EXISTS qanda')
    cur.execute(
        "CREATE TABLE `qanda` ("
        "   `id` SMALLINT NOT NULL AUTO_INCREMENT,"
        "   `epiShortNumber` VARCHAR(12) NOT NULL,"
        "   `questionNumber` VARCHAR(12) NOT NULL,"
        "   `topic` VARCHAR(50),"
        # questionNumber is the episodenumber appending the question number
        "   `question` VARCHAR(15000) NOT NULL,"
        "   `answers` TEXT NOT NULL,"
        "   PRIMARY KEY (`id`)"
        ") ENGINE=INNODB")

    cur.execute('DROP TABLE IF EXISTS henPan')
    cur.execute("""CREATE TABLE henPan(id SMALLINT NOT NULL AUTO_INCREMENT PRIMARY KEY\
                                       epiShortNumber VARCHAR(12) NOT NULL, \
                                       panelName VARCHAR(50) NOT NULL)""")
    
    cur.execute('DROP TABLE IF EXISTS panellist')
    cur.execute('CREATE TABLE panellist(id SMALLINT NOT NULL AUTO_INCREMENT, panelName VARCHAR(50) NOT NULL, panelPicID VARCHAR(10), panelProfile VARCHAR(8000) NOT NULL,PRIMARY KEY (id))')
    #                                    panellIdentity VARCHAR(40), \

def local_dump(text,fname):
    with open(fname,'w') as f:
        f.write(text.encode('UTF-8'))

def get_new_soup():
    pass

def dump_panellists(epiShortNumber):
    try:
        with open(EFILENAME.format(num=epiShortNumber),'r') as f:
            epi_soup = BS(f)
    except:
        print epiShortNumber + 'does not exist locally'

    presenters = epi_soup.find_all('div', class_ = 'presenter')

    for presenter in presenters:
        panel_name = presenter.find('a').text.encode('UTF-8')

        panel_pic_ID = presenter.find('img')
        if panel_pic_ID:
            panel_pic_ID = panel_pic_ID['src'][-11:-4]
        else:
            panel_pic_ID = 0
        # if panle pic id doesn't exist then id should be none rather than 0, so this should be bolished

        panel_profile = presenter.find('p').text.encode('UTF-8')

        sql = 'INSERT INTO panellist VALUES(%s,%s,%s)'
        cur.execute(sql,(panel_name,panel_pic_ID,panel_profile,))

        sql = 'INSERT INTO henPan VALUES(%s,%s)'
        cur.execute(sql,(epiShortNumber,panel_name))
        # the table henpan shoulbe be modified: making episode number and panellist's panel_ID as foreign key

def dump_epi(epiShortNumber):
    epi_soup = None
    file_handle = None
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        file_handle = os.open(EFILENAME.format(num=epiShortNumber),flags)
    except OSError as e:
        # that episode already exists
        if e.errno == errno.EEXIST:
            with open(EFILENAME.format(num=epiShortNumber),'r') as file_obj:
                epi_soup = BS(file_obj)
        else:
        # something unexpected happened
            raise
    else:
        # file doesn't exist and open the file successfully
        time.sleep(2)
        text = requests.get(EPISPAGE.format(num=epiShortNumber)).text
        print 'episode page request'
        with os.fdopen(file_handle,'w') as file_obj:
            file_obj.write(text.encode('UTF-8'))
        epi_soup = BS(text)

    videoLink = epi_soup.find('li', class_ = 'download')

    if videoLink:
        videoLink = videoLink.find('a')['href'].encode('UTF-8')
        sql = 'UPDATE hentry SET videoLink=%s WHERE epiShortNumber=%s'
        cur.execute(sql,(videoLink,epiShortNumber,))
    else:
        videoLink = 0

    transcript_soup = epi_soup.find('div', id = 'transcript')
    qandas = re.split('<span id=\"',str(transcript_soup))

    for qanda in qandas[1:]:
        t,a = re.split('</span>',qanda)
        qNumber,topic = t.split('">')
        question = a.split('<br/>')[1]
        answers = BS(''.join(a.split('<br/>')[2:])).text.encode('UTF-8')
        sql = 'INSERT INTO qanda (epiShortNumber, questionNumber, topic, question, answers) VALUES(%s,%s,%s,%s,%s)'
        cur.execute(sql,(epiShortNumber,qNumber,topic,question,answers,))

    dump_panellists(epiShortNumber)

def dump_entries(entries):
    for entry in entries:
        'insert all into hentry table'
        date = entry.find('span', class_ = 'date').string.encode('UTF-8')
        date = parser.parse(date).strftime('%Y-%m-%d')
        
        epi_link = entry.find('a', class_ = 'details')['href'].encode('UTF-8')
        epiShortNumber = epi_link[-11:-4]
        bookmark = entry.find('a', class_ = 'entry-title').string.encode('UTF-8')
        #all the above are available
        sql = 'INSERT INTO hentry (epiShortNumber, hentryDate, epiLink, bookmark) VALUES(%s,%s,%s,%s)'
        cur.execute(sql,(epiShortNumber,date,epi_link,bookmark,))
        dump_epi(epiShortNumber)
        print 'finish ' + date + bookmark

def initiate():
    time.sleep(2)
    text = requests.get(HOMEPAGE).text
    print 'init request'
    local_dump(text,HFILENAME)
    init_database()
    #if this file doesn't exist then initiate the database, it's too dogmatic
    remote_soup = BS(text)
    remote_latest_entries = remote_soup.find_all('div', class_ = 'hentry')
    # not return but dump the database
    dump_entries(remote_latest_entries)


def refresh():
    # if the home page is updated
        # 7*24*3600 should be replaced by the latest time in database

    try:
        file_mod_time = os.path.getatime(HFILENAME)
        print 'You updated the database %s ago. Continue?[y/n] ' % str(int((time.time()-file_mod_time)/86400))
        if sys.stdin.read(1) == 'n':
            sys.exit(0)
        else:
            # should be detailed further
            pass
        # if the home page is outdated
        with open(HFILENAME) as f:
            local_soup = BS(f)
            local_latest_entry = local_soup.find('div', class_ = 'hentry')

            local_latest_date = local_latest_entry.find('span', class_ = 'date').string
#                local_latest_date = parser.parse(local_latest_date).strftime('%Y-%m-%d')
            time.sleep(2)
            remote_text = requests.get(HOMEPAGE).text
            print 'home page request'
            remote_soup = BS(remote_text)
            remote_latest_entry = pro_soup.find('div', class_ = 'hentry')
            remote_latest_entries = pro_soup.find_all('div', class_ = 'hentry')
            remote_latest_date = local_latest_entry.find('span', class_ = 'date').string

            nu_new = int((parser.parse(remote_latest_date)-parser.parse(local_latest_date))/604800)
            if nu_new > 0:
                local_dump(remote_soup,HFILENAME)
                dump_entries(remote_latest_entries[:nu_new])
            else:
                print 'Nothing new.'
                sys.exit(0)

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

class QandA:
    try:
        refresh()
    except:
        con.commit()
        cur.close()
        con.close()
        
    con.commit()
    cur.close()
    con.close()
if __name__ == "__main__":
    QandA()
# cannot fetch data in 2008
