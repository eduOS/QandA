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
TOCOMPLECATEDTOMATCH = 'Too complecated to match the question.'

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

def write2sql(sql, agms):
    agms = tuple([agm.encode('utf-8', errors='replace') for agm in agms])
    cur.execute(sql,agms)

def init_database():
    cur.execute('DROP TABLE IF EXISTS hentry')
    cur.execute(
        "CREATE TABLE `hentry` ("
        "   `id` SMALLINT NOT NULL AUTO_INCREMENT,"
        "   `epiShortNumber` VARCHAR(12) NOT NULL,"
        "   `hentryDate` VARCHAR(20),"
        "   `epiLink` VARCHAR(60),"
        "   `bookmark` VARCHAR(300),"
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
    cur.execute("""CREATE TABLE henPan(id SMALLINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\
                                       epiShortNumber VARCHAR(12) NOT NULL, \
                                       panelName VARCHAR(50) NOT NULL)""")
    
    cur.execute('DROP TABLE IF EXISTS panellist')
    cur.execute('CREATE TABLE panellist(id SMALLINT NOT NULL AUTO_INCREMENT, \
                                        panelName VARCHAR(50) NOT NULL, \
                                        panelPicID VARCHAR(10), \
                                        panelProfile VARCHAR(8000), \
                                        PRIMARY KEY (id))')
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
        panel_name = presenter.find('a').text

        panel_pic_ID = presenter.find('img')
        if panel_pic_ID:
            panel_pic_ID = panel_pic_ID['src'][-11:-4]
        else:
            panel_pic_ID = 0
        # if panle pic id doesn't exist then id should be none rather than 0, so this should be bolished

        panel_profile = presenter.find('p').text

        sql = 'INSERT INTO panellist (panelName, panelPicID, panelProfile) VALUES(%s,%s,%s)'
        write2sql(sql,(panel_name,panel_pic_ID,panel_profile))

        sql = 'INSERT INTO henPan (epiShortNumber, panelName) VALUES(%s,%s)'
        write2sql(sql,(epiShortNumber,panel_name,))
        # the table henpan shoulbe be modified: making episode number and panellist's panel_ID as foreign key
    con.commit()
    print epiShortNumber, 'panel committed'

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
                print 'that episode already exists'
        #elif e.errno == errno.ENOENT:
        #    time.sleep(0.2)
        #    text = requests.get(EPISPAGE.format(num=epiShortNumber)).text
        #    print epiShortNumber, ' episode page request'
        #    with os.fdopen(file_handle,'w') as file_obj:
        #        file_obj.write(text.encode('UTF-8'))
        #    epi_soup = BS(text)
        else:
        # something unexpected happened
            print 'something unexpected happened'
            raise
    else:
        # another command but don't trace the error
        time.sleep(0.2)
        text = requests.get(EPISPAGE.format(num=epiShortNumber)).text
        print epiShortNumber, ' episode page request'
        with os.fdopen(file_handle,'w') as file_obj:
            file_obj.write(text.encode('UTF-8'))
        epi_soup = BS(text)
    finally:
        print 'soup loaded'

    videoLink = epi_soup.find('li', class_ = 'download')
    if videoLink:
        videoLink = videoLink.find('a')['href'].encode('UTF-8')
        sql = 'UPDATE hentry SET videoLink=%s WHERE epiShortNumber=%s'
        write2sql(sql,(videoLink,epiShortNumber,))
    else:
        print epiShortNumber, ' no videoLink'
        videoLink = 0

    transcript = str(epi_soup.find('div', id = 'transcript')).replace('<br/><br/>','\n').replace('<br/>','\n')
    qandas = transcript.split('<span id=')
    # don't know where to put this 
    #greetings = qandas[0]

    for qanda in qandas[1:]:
        # don't use match too much, manage the transcript line by line
        lines = qanda.replace('</span>','\n').replace('\n\n','\n').split('\n')
        try:
            match = re.match(r'"(q\d{1,2})">(.*)\n{,1}',lines[0]).groups()
            qNumber = match[0]
            topic = match[1]
        except:
            print 'rules of the changed for ', epiShortNumber
            print qanda
            raise
        question = lines[1]
        answers = '\n'.join(lines[2:])
        # later each line of the answer can be dumpted to database seperately
        sql = 'INSERT INTO qanda (epiShortNumber, questionNumber, topic, question, answers) VALUES(%s,%s,%s,%s,%s)'
        write2sql(sql,(epiShortNumber,qNumber,topic,question,answers))
    con.commit()
    print epiShortNumber, 'scripts committed'
    dump_panellists(epiShortNumber)

def dump_entries(entries):
    'insert all into hentry table'
    for entry in entries:
        date = entry.find('span', class_ = 'date').string.encode('UTF-8')
        date = parser.parse(date).strftime('%Y-%m-%d')
        
        epi_link = entry.find('a', class_ = 'details')['href'].encode('UTF-8')
        epiShortNumber = epi_link[-11:-4]
        bookmark = entry.find('a', class_ = 'entry-title').string.encode('UTF-8')
        #all the above are available
        sql = 'INSERT INTO hentry (epiShortNumber, hentryDate, epiLink, bookmark) VALUES(%s,%s,%s,%s)'
        write2sql(sql,(epiShortNumber,date,epi_link,bookmark,))
        dump_epi(epiShortNumber)
        print 'finish ' + date + bookmark

def initiate():
    # the function in dump_epi should be divided for the file date check function
    text = requests.get(HOMEPAGE).text
    remote_soup = BS(text)
    #with open(HFILENAME) as f:
    #    remote_soup = BS(f)
    #init_database()
    #if this file doesn't exist then initiate the database, it's too dogmatic
    remote_latest_entries = remote_soup.find_all('div', class_ = 'hentry')
    # not return but dump the database
    dump_entries(remote_latest_entries)


def refresh():
    # if the home page is updated
        # 7*24*3600 should be replaced by the latest time in database

    try:
        file_mod_time = os.path.getatime(HFILENAME)
        print 'You updated the database %s days ago. Continue?[y/n] ' % str(int((time.time()-file_mod_time)/86400))
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
            time.sleep(0.2)
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
#    try:
    refresh()
#    except:
#        con.commit()
#        cur.close()
#        con.close()
        
    con.commit()
    cur.close()
    con.close()

if __name__ == "__main__":
    QandA()
# cannot fetch data in 2008
