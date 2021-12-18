import datetime
from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine
import pandas as pd
import urllib

DATABASE = "data.sqlite"
DATA_TABLE = "data"
engine = create_engine(f'sqlite:///{DATABASE}', echo=False)

da_set = []
today = datetime.datetime.strftime(datetime.datetime.now(),"%m-%d-%Y")

raw = requests.get("https://www.canning.wa.gov.au/residents/building-here/development-assessment-panel", verify=False)
soup = BeautifulSoup(raw.content, 'html.parser')

da_rows = soup.find('table').find_all("tr")
for row in da_rows:
        if row.find("td"):
                description, address, council_reference, info_url = row.find_all("td")
                description = description.text
                address = address.text.replace("\r"," ").replace("\n"," ").replace("\t","")
                if "Ref" in council_reference.text:
                        council_reference = council_reference.text.split("Ref: ")[-1]
                else:
                        council_reference = council_reference.text.split("ref: ")[-1]
                if info_url.find("a"):
                        info_url = urllib.parse.quote(info_url.find_all('a')[-1].attrs['href'])
                else:
                        info_url = 'https://www.canning.wa.gov.au/residents/building-here/development-assessment-panel'
                da = {}
                da['council_reference'] = council_reference
                da['description'] = description
                da['address'] = address
                da['info_url'] = info_url
                da['date_scraped'] = today
                da_set.append(da)

data = pd.DataFrame(da_set)
data.to_sql(DATA_TABLE, con=engine, if_exists='append',index=False)
