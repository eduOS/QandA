from urllib2 import urlopen
from bs4 import BeautifulSoup
import re
import requests
import time
import MySQLdb as mdb

con = mdb.connect('localhost',"root","104064",'QandA')
cur = con.cursor()

time.strtime("%Y-%m-%d")
# update automatically a week later / every Tuesday

pro_soup = BeaufitulSoup(requests.get('http://www.abc.net.au/tv/qanda/past-programs-by-date.htm').text)

hentries = pro_soup.find_all('div', class_ = 'hentry')

cur.execute('DROP TABLE IF EXIST Hentry')
cur.execute('CREATE TABLE hentry(hentryDate DATE NOT NULL PRIMARY KEY, \
                                 entryTitle VARCHAR(60) NOT NULL, \
                                 bookmark VARCHAR(300), \
                                 videoLink varchar(60) NOT NULL, \
                                 questions TEXT NOT NULL, \
                                 transcript MEDIUMTEXT NOT NULL)')

cur.execute('DROP TABLE IF EXIST HenPan')
cur.execute('CREATE TABLE henPan(hentryDate DATE NOT NULL, \
                                 panellID VARCHAR(10) NOT NULL)')

cur.execute('DROP TABLE IF EXIST Panellist')
cur.execute('CREATE TABLE panellist(panellID VARCHAR(10) NOT NULL PRIMARY KEY, \
                                    panelName VARCHAR(10) NOT NULL, \ 
                                    panellProfile VARCHAR(5000)) NOT NULL')
#                                    panellIdentity VARCHAR(40), \

for hentry in hentries:
    date = hentry.find('span', class_ = 'date').string
    epi_link = hentry.find('a', class_ = 'details')['href']
    bookmark = hentry.find('a', class_ = 'entry-title').string
    #panelIden = find('div', class_ = 'entry-summary').string

    epi_soup = BeautifulSoup(requests.get(epi_link).text)
    videoLink = epi_soup.find('li', class_ = 'download').find('a')['href']
    questions = epi_soup.find('div', id = 'questions').text
    transcript = epi_soup.find('div', id = 'transcript').text
    sql = 'INSERT INTO hentry VALUES(%s,%s,%s,%s,%s,%s)'
    cur.execute(sql,(date,epi_link,bookmark,vedioLink,questions,transcript,))

    presenters = epi_soup.find_all('div', class_ = 'presenter')
    for presenter in presenters:
        ID = presenter.find('img')['src'][-11:-4]
        name = presenter.find('a', name = re.compile('[A-Z0-9_]+').text)
        profile = presenter.find('p').text

        sql = 'INSERT INTO henPan VALUES(%s,%s)'
        cur.execute(sql,(date,ID,))

        sql = 'SELECT * FROM panellist WHERE PanellID = %s'
        cur.execute(sql,(ID,))
        if not cur.rowcount:
            sql = 'INSERT INTO panellist VALUES(%s,%s,%s)'
            cur.execute(sql,(ID,name,profile,))
