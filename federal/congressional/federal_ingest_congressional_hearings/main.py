import logging
import time

from datetime import date

import requests

from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, helpers
from furl import urljoin
from google.cloud import firestore
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
db = firestore.Client()

firestore_index = 'congress-number'
elasticsearch_index = 'federal_congressional_hearings'


def get_congress_number_from_year(year):
    return int((year - 1787) / 2)


def federal_ingest_congressional_hearings(message, context):
    start_time = time.time()
    _exit = False
    current_year = date.today().year
    ref = db.collection('federal').document('congressional')
    settings = ref.get().to_dict()
    firestore_idx_value = settings[firestore_index]
    congress_numbers = range(get_congress_number_from_year(current_year), 84, -1)
    #congress_numbers = [get_congress_number_from_year(current_year)]  # uncomment this line to make it work for only the current_year

    base_url = 'https://www.govinfo.gov/'
    all_committees_url = '/browse/committee'

    all_committees_resp = requests.get(urljoin(base_url, all_committees_url))
    soup = BeautifulSoup(all_committees_resp.text, 'html.parser')

    senate_col = soup.find('div', {'id': 'senate-col'})
    senate_committees = []
    for committee_row in senate_col.find_all('div', {'class': 'field_items'}):
        committee_section_url = committee_row.find('a').get('href')
        senate_committees.append(committee_section_url.split('/')[-1].split('-')[-1])

    house_col = soup.find('div', {'id': 'house-col'})
    house_committees = []
    for committee_row in house_col.find_all('div', {'class': 'field_items'}):
        committee_section_url = committee_row.find('a').get('href')
        house_committees.append(committee_section_url.split('/')[-1].split('-')[-1])

    joint_col = soup.find('div', {'id': 'joint-col'})
    joint_committees = []
    for committee_row in joint_col.find_all('div', {'class': 'field_items'}):
        committee_section_url = committee_row.find('a').get('href')
        joint_committees.append(committee_section_url.split('/')[-1].split('-')[-1])

    chambers = ['senate', 'house', 'joint']
    all_committees = [senate_committees, house_committees, joint_committees]
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    session = requests.Session()

    for congress_number in congress_numbers:
        if congress_number > settings[firestore_index]:
            continue
        actions = []
        for chamber, committees in zip(chambers, all_committees):
            for committee in committees:
                api_url = f'/wssearch/browsecommittee/chamber/{chamber}/committee/{committee}/collection/CHRG/congress/{congress_number}?fetchChildrenOnly=1'
                api_resp = session.get(urljoin(base_url, api_url), headers=headers)
                try:
                    api_resp.raise_for_status()
                except requests.exceptions.HTTPError as ex:
                    logger.info(f'Failed to get hearings for: {congress_number} {chamber} {committee}')
                    continue
                children = api_resp.json().get('childNodes')
                if not children:
                    continue
                for child in children:
                    key = child['nodeValue']['granuleid'] or child['nodeValue']['packageid']
                    actions.append({
                        '_op_type': 'index',
                        '_index': elasticsearch_index,
                        '_id': key,
                        '_source': {
                            'obj': child['nodeValue'],
                        }
                    })
        helpers.bulk(es, actions)
        if time.time() - start_time > 300:
            _exit = True
            break
    if _exit:
        settings[firestore_index] = congress_number
        ref.set(settings)
        logger.info(f'FIRESTORE UPDATED | {firestore_index}: {congress_number}')
    else:
        settings[firestore_index] = get_congress_number_from_year(current_year)
    return True
