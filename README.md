# climate_data_exchange
python script to push aggregated data from Nepali Calendar to ISO Calendar 

Python Script to Auto sync Climet data from nepalhmis to dhis2 instance

# add flask for create web-app for DHIS2
## install

sudo apt install python3-pip

pip install flask requests python-dotenv

pip install --upgrade certifi

pip install --upgrade requests certifi urllib3

pip install flask-cors

pip install python-dotenv

pip install psycopg2-binary

pip install clickhouse-connect

pip install nepali-date_converter

pip install npdatetime

pip install datetime

#https://pypi.org/project/nepali-calendar-utils/

pip install nepali-calendar-utils

pip install nepali

pip install nepali-datetime


-- 
sudo apt update

sudo apt install python3-full python3-venv -y

-- Create virtual environment

cd /home/mithilesh/climet_data_exchange

python3 -m venv venv

-- Activate it

source venv/bin/activate

then

pip install nepali-datetime

pip install --upgrade requests certifi urllib3

pip install python-dotenv


-- now add cron inside that

0 2 * * * /home/mithilesh/climet_data_exchange/venv/bin/python /home/mithilesh/climet_data_exchange/run_exchange.py >> /home/mithilesh/climet_data_exchange/cron.log 2>&1

this application run through Power Automate 
Request come from DHIS2 to Power Automate Cloud Flow  --> Power Automate Desktop Flow --> get response from desktop flow -- send to Cloud Flow --> then send to DHIS2 then --> push/put to DHIS2 datastore -->
for add/update TEI attribute value and eventDataValue through DHIS2 APIs

DHIS2 (Event Created / Trigger)
        │
        ▼
Power Automate Cloud Flow
        │
        ├── Get Event Data (DHIS2 API)
        │
        ├── Call Desktop Flow (RPA)
        │        │
        │        ▼
        │   Power Automate Desktop
        │        │
        │        ├── Open Accuity Website
        │        ├── Enter Name / ID
        │        ├── Scrape Result (OCR / Web Scraping)
        │        └── Return JSON Response
        │
        ▼
Process Accuity Response (Cloud Flow)
        │
        ├── Parse JSON
        ├── Store in DHIS2 DataStore (Optional)
        └── Update Event DataValues (DHIS2 API)
