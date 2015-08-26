from bs4 import BeautifulSoup
import requests,sys

with open("past-programs-by-date.soup",'wb') as f:
    response = requests.get('http://www.abc.net.au/tv/qanda/past-programs-by-date.htm',stream=True)
    if not response.ok:
        print 'Oops! Something went wrong...'
    else:
        for block in response.iter_content(1024):
            f.write(block)
