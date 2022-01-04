import requests 
from bs4 import BeautifulSoup
import pandas as pd
import math
from tqdm import tqdm

def github_topics_scraper(detailed=False, records=True):
    page_content =''
    start = 1   # start of number ofGitHub Topics pages
    end = 6

    print('Scrapping GitHub topics...')
    # to download all GitHub Topics webpages
    while start<=end:
        url = 'https://github.com/topics?page={}.format(i)'
        r = requests.get(url)
        if r.status_code == 200:    #successful request
            page_content+= '\n'
        else:                       #if page failed to load, reload
            start-=1
        start+=1

    # creating BeautifulSoup object
    soup = BeautifulSoup(page_content, 'html.parser')

    # extracting topic titles
    topic_ptags = soup.find_all('p', {'class': 'f3 lh-condensed mb-0 mt-1 Link--primary'}, limit=records)
    topics = []
    for tag in topic_ptags:
        topics.append(tag.text)

    # extracting topic descriptions
    topic_pdesc = soup.find_all('p', {'class': 'f5 color-fg-muted mb-0 mt-1'}, limit=records)
    topic_desc = []
    for desc in topic_pdesc:
        topic_desc.append(desc.text.strip())

    # extracting topic URLs
    topic_purls = soup.find_all('p', {'class':'d-flex no-underline'}, limit=records)
    topic_urls = []
    for url in topic_purls:
        topic_urls.append('https://github.com' + url['href'])


    


    
