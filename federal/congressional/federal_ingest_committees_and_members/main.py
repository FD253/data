import logging

import requests
import pandas as pd
import xmltodict

from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, helpers
from google.cloud import secretmanager


# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_host/versions/1"}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_username_data/versions/1"}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_password_data/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)

index = 'federal_congressional_members'


def camel_case_split(string):
    words = [[string[0]]]

    for c in string[1:]:
        if words[-1][-1].islower() and c.isupper():
            words.append(list(c))
        else:
            words[-1].append(c)

    return [''.join(word) for word in words]


def federal_ingest_committees_and_members(message, context):
    actions = []

    # house
    resp = requests.get('https://www.house.gov/representatives')
    soup = BeautifulSoup(resp.text, 'html.parser')

    places = [i.text.strip() for i in soup.find_all('caption')[:56]]
    tables = pd.read_html(resp.text)[:56]

    for place, table in zip(places, tables):
        for row in table.values.tolist():
            key = 'R-' + place + '-' + row[0] + '-' + row[1] + '-' + row[2]
            obj = {
                'Type': 'R',
                'District': place + ' ' + row[0],
                'Name': row[1],
                'Party': row[2],
                'Office Room': row[3],
                'Phone': row[4],
                'Committee Assignment': camel_case_split(row[5]) if type(row[5]) == str else [],
            }
            actions.append({
                '_op_type': 'index',
                '_index': index,
                '_id': key,
                '_source': {
                    'obj': obj,
                }
            })
    helpers.bulk(es, actions)

    # senate
    actions = []
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    senators_resp = requests.get('https://www.senate.gov/general/contact_information/senators_cfm.xml', headers=headers)
    committees_resp = requests.get('https://www.senate.gov/general/committee_assignments/assignments.htm', headers=headers)
    soup = BeautifulSoup(committees_resp.text, 'html.parser')
    committees_senators = soup.find_all('div', {'style': 'float:left; width:25%; font-weight:bold; min-width:200px;'})
    committess_senators_memberships = soup.find_all('div', {'style': 'float:left; width:72%; min-width:200px;'})

    memberships = {}
    for committees_senator, committees_senator_memberships in zip(committees_senators, committess_senators_memberships):
        committees = []
        name = committees_senator.text.strip().split('(')[0].strip()
        for membership in committees_senator_memberships.find_all('a'):
            committees.append(membership.text)
        memberships[name] = committees

    senators_cfm_dict = xmltodict.parse(BeautifulSoup(senators_resp.text, 'html.parser').prettify('latin-1').decode())
    for member in senators_cfm_dict['contact_information']['member']:
        name = member['last_name'].encode('latin-1').decode('latin-1') + ', ' + member['first_name']
        key = 'S-' + member['state'] + '-' + name + '-' + member['party']
        obj = {
            'Type': 'S',
            'District': member['state'],
            'Name': name,
            'Party': member['party'],
            'Office Room': member['address'].replace('\n     ', ''),  # replace() fixes one broken address
            'Phone': member['phone'],
            'Committee Assignment': memberships[name]
        }
        actions.append({
            '_op_type': 'index',
            '_index': index,
            '_id': key,
            '_source': {
                'obj': obj,
            }
        })
    helpers.bulk(es, actions)
    return True
