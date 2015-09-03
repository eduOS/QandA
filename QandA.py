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

if not os.path.exists(args.soup_dir):
    os.makedirs(args.soup_dir)

FILEPATH = args.soup_dir+'/{name}'
HPNAME = 'programs-by-date'
HFPATH = FILEPATH.format(name=HPNAME)

con = mdb.connect(args.dbserver,args.dbuser,args.dbpwd)
cur = con.cursor()

if not cur.execute("SHOW DATABASES LIKE '" + args.dbname + "'"):
    cur.execute('CREATE DATABASE ' + args.dbname)
cur.execute('USE ' + args.dbname)

def executesql(sql, agms):
    try:
        agms = tuple([agm.encode('utf-8', errors='replace') for agm in agms])
        return cur.execute(sql,agms)
    except:
        return cur.execute(sql,agms)

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
    with open(FILEPATH.format(name=epiShortNumber),'r') as f:
        epi_soup = BS(f)

    presenters = epi_soup.find_all('div', class_ = 'presenter')

    for presenter in presenters:
        try:
            panel_name = presenter.find('a').text
        except:
            panel_name = presenter.find('h4').text

        panel_pic_ID = presenter.find('img')
        if panel_pic_ID:
            panel_pic_ID = panel_pic_ID['src'][-11:-4]
        else:
            panel_pic_ID = 0
        # if panle pic id doesn't exist then id should be none rather than 0, so this should be bolished

        panel_profile = presenter.find('p').text

        sql = 'INSERT INTO panellist (panelName, panelPicID, panelProfile) VALUES(%s,%s,%s)'
        executesql(sql,(panel_name,panel_pic_ID,panel_profile))

        sql = 'INSERT INTO henPan (epiShortNumber, panelName) VALUES(%s,%s)'
        executesql(sql,(epiShortNumber,panel_name,))
        # the table henpan shoulbe be modified: making episode number and panellist's panel_ID as foreign key
    con.commit()
    print 'succeed: ', epiShortNumber

def dump_epi(epiShortNumber):
    with open(FILEPATH.format(name=epiShortNumber),'r') as epif:
        epi_soup = BS(epif)

    videoLink = epi_soup.find('li', class_ = 'download')
    if videoLink:
        videoLink = videoLink.find('a')['href'].encode('UTF-8')
        sql = 'UPDATE hentry SET videoLink=%s WHERE epiShortNumber=%s'
        executesql(sql,(videoLink,epiShortNumber,))
    else:
        print epiShortNumber, ' no videoLink'
        videoLink = 0

    transcript = str(epi_soup.find('div', id = 'transcript')).replace('<br/><br/>','\n').replace('<br/>','\n')
    qandas = transcript.split('<span id=')
    # don't know where to put this 
    #greetings = qandas[0]

    qnu=1
    for qanda in qandas[1:]:
        # don't use match too much, manage the transcript line by line
        lines = qanda.replace('</span>','\n').replace('\n\n','\n').split('\n')
        try:
            match = re.match(r'.+(q\d{1,2}).+>(.*)\n{,1}',lines[0]).groups()
            qNumber = match[0]
            topic = match[1]
        except:
            try: 
                match = re.match(r'.+(q\d{,2}).+>(.*)\n{,1}',lines[0]).groups()
                qNumber = match[0] + str(qnu)
                qnu += 1
                topic = match[1]
            except:
                print 'rules of the changed for ', epiShortNumber
                print qanda
                raise
        question = lines[1]
        answers = '\n'.join(lines[2:])
        # later each line of the answer can be dumpted to database seperately
        sql = 'INSERT INTO qanda (epiShortNumber, questionNumber, topic, question, answers) VALUES(%s,%s,%s,%s,%s)'
        executesql(sql,(epiShortNumber,qNumber,topic,question,answers))
    dump_panellists(epiShortNumber)



def haveFile(filename):
    file_handle = None
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    filepath = FILEPATH.format(name=filename)
    try:
        file_handle = os.open(filepath,flags)
    except OSError as e:
        # that episode already exists
        if e.errno == errno.EEXIST:
            return True
        else:
        # something unexpected happened
            print 'something unexpected happened'
            raise
    else:
        # if not, request remotely and dump to file
        time.sleep(0.2)
        if filename == HPNAME:
            text = requests.get(HOMEPAGE).text
        else:
            text = requests.get(EPISPAGE.format(name=filename)).text
        local_dump(text, filepath)
        return False

def dumpEntryDetail(entryNum):
    """
    scan if all entries listed on the homepage are dumped into local files and database
    if not, dump them
    """
    # if in file, then check if is't in database(if the epiShortNumber is in hentry table) if not dump it
    if haveFile(entryNum):
        sql = "select * from hentry where epiShortNumber=%s"
        sqllast = "select * from hentry where epiShortNumber=%s"
        if not executesql(sql,(entryNum,)):
            dump_epi(entryNum)
        elif haveFile('2246870') and executesql(sqllast,('2246870',)):
            # if the last entry has been dumped then exit
            print 'dumped all entries'
            cur.close()
            con.close()
            sys.exit(0)

    else:
        dump_epi(entryNum)

def dumpEntry(entry):
    """
    extract short numbers
    insert into database
    return entry number
    """
    date = entry.find('span', class_ = 'date').string
    date = parser.parse(date).strftime('%Y-%m-%d')
    
    epi_link = entry.find('a', class_ = 'details')['href']
    epiShortNumber = epi_link[-11:-4]
    bookmark = entry.find('a', class_ = 'entry-title').string
    #all the above are available
    sql = 'INSERT INTO hentry (epiShortNumber, hentryDate, epiLink, bookmark) VALUES(%s,%s,%s,%s)'
    executesql(sql,(epiShortNumber,date,epi_link,bookmark,))

    print date,' ', epiShortNumber
    dumpEntryDetail(epiShortNumber)

def refresh():
    """
    read entries from local homepage
    get entries short numbers
    have entries dumped
    """
    try:
        with open(HFPATH) as f:
            home_soup = BS(f)
            entries = home_soup.find_all('div', class_ = 'hentry')
            for entry in entries:
                dumpEntry(entry)
    except OSError as e:
        print e
        raise


class QandA:
    try:
        file_mod = os.path.getatime(HFPATH)
        #print 'You updated the database %s days ago. y for update homepage and n for using old homepage?[y/n] ' % str(int((time.time()-file_mod)/86400))
        #if sys.stdin.read(1) == 'y':
        nput = raw_input('You updated the database %s days ago. y for update homepage and n for using old homepage?[y/n] ' % str(int((time.time()-file_mod)/86400))) 
        if nput == 'y':
            os.remove(HFPATH)
            haveFile(HPNAME)
            refresh()
        elif nput == 'n':
            refresh()
        else:
            sys.exit(0)
    except OSError as e:
        if e.errno == 2:
            nput = raw_input('Seems that you should initiate database?[y/n] ') 
            if nput == 'y':
                haveFile(HPNAME)
                init_database()
                refresh()
        else:
            cur.close()
            con.close()
            sys.exit(0)
        
    cur.close()
    con.close()

if __name__ == "__main__":
    QandA()
# cannot fetch data in 2008

