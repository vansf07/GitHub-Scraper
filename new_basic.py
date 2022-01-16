import json
import logging
from pprint import pprint
from time import sleep

import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

def parse(u):
    title = '-'
    description = '-'
    rec = {}


    try:
        r = requests.get(u)


        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            # title
            title_section = soup.select('.f3 lh-condensed mb-0 mt-1 Link--primary')
            # description
            description_section = soup.select('.f5 color-fg-muted mb-0 mt-1')
            # URLS
            url_section = soup.select('.d-flex no-underline')
            if url_section:
                url = 'https://github.com' + url_section[0].text


            if title_section:
                for title in title_section:
                    title_text = title.text.strip()
                    if 'Add all titles to list' not in title_text and title_text != '':
                        title.append({'step': title.text.strip()})


            if description_section:
                description = description_section[0].text.strip()

            rec = {'title': title, 'description': description, 'url': url}

    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
    finally:
    
        return json.dumps(rec)

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Yay Connected')
    else:
        print('Could not connect!') 
    return 


def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "topics": {
                "dynamic": "strict",
                "properties": {
                    "title": {
                        "type": "text"
                    },
                    "description": {
                        "type": "text"
                    },
                    "url": {
                        "type": "text"
                    },
                }
            }
        }
    }


    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created

def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, doc_type='topics', body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)


    url = 'https://www.github.com/topics'
    r = requests.get(url)
    if r.status_code == 200:
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        start = 1
        end = 6
        es = connect_elasticsearch()

        while start<=end:
            sleep(2)
            result = parse(url + '?page={}.format(start)')
            if es is not None:
                if create_index(es, 'topics'):
                    out = store_record(es, 'topics', result)
                    print('Data indexed successfully')
                    start+=1


    es = connect_elasticsearch()

