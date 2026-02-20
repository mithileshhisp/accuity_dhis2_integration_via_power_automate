# utils.py

import requests
import logging

import certifi  ## for post data in hmis production certificate issue


import json
import smtplib
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders
from urllib.parse import quote

## for nepali date
#import nepali_datetime
from datetime import datetime, timedelta, date

#from datetime import timedelta

from dotenv import load_dotenv
import os
import glob
load_dotenv()

FROM_EMAIL_ADDR = os.getenv("FROM_EMAIL_ADDR")
FROM_EMAIL_PASSWORD = os.getenv("FROM_EMAIL_PASSWORD")

from constants import LOG_FILE
#from app import QueueLogHandler

DHIS2_API_URL = os.getenv("DHIS2_API_URL")


# ADD THIS PART (UI streaming) for print in HTML Page in response
#Add a global log queue
import queue
log_queue = queue.Queue()
#Add a Queue logging handler
#import logging

'''
class QueueLogHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(self.format(record))
'''

import logging
import queue

log_queue = queue.Queue()

class QueueHandler(logging.Handler):
    def emit(self, record):
        log_queue.put(self.format(record))


def configure_logging():

    #Optional (Advanced, but useful)
    '''
    import sys
    sys.stdout.write = lambda msg: logging.info(msg)
    logging.info(f"[job:{job_id}] step 1")
    '''

    LOG_DIR = "logs"
    #os.makedirs(LOG_DIR, exist_ok=True)

    os.makedirs(LOG_DIR, exist_ok=True)
    assert LOG_DIR != "/" and LOG_DIR != "" #### Never delete outside log folder.

    # Create unique log filename
    #log_filename = f"log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_filename = LOG_FILE
    #log_filename = f"{LOG_FILE}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    log_path = os.path.join(LOG_DIR, log_filename)

    #logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    '''
    logging.basicConfig(filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            QueueLogHandler()   # 👈 THIS is the key
        ]
    )
    '''
    # ✅ ADD THIS (UI streaming)
    '''
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not any(isinstance(h, QueueLogHandler) for h in root_logger.handlers):
        queue_handler = QueueLogHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        queue_handler.setFormatter(formatter)
        root_logger.addHandler(queue_handler)
    '''

def log_info(message):
    logging.info(message)

def log_error(message):
    logging.error(message)

#################################
## for Accuity DHIS2 Integration ######

def get_accuity_response(FLOW_URL, eventUid, orgUnit_uid, program_uid, accuity_search_text ):
    
    print(f"Send to Accuity")
    logging.info(f"Send to Accuity")
    #
    #FLOW_URL_updated = "https://default56af9532501a404c995d80633a35c0.ac.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/5e4d6b6437544b798b93c2035d0a66bd/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=qCVnbA5MH8um9u9Qwux6_xB0qlBxHgmHnNJ--kwPHOE"
    
    #FLOW_URL_updated = "https://default56af9532501a404c995d80633a35c0.ac.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/659d9a7a7b404fbfa426dfa84e486992/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=5VaBmHuGhyAYYnAumUf0eqdXPwOpue0aPICvxPgfthQ"
    
    ## new cloud flow  Cloud flow Keyword Search  link destop flow -- Accuity_RPA_Data_Entry - KeyWord Search
    FLOW_URL_updated = "https://default56af9532501a404c995d80633a35c0.ac.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/659d9a7a7b404fbfa426dfa84e486992/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=5VaBmHuGhyAYYnAumUf0eqdXPwOpue0aPICvxPgfthQ"

    try:
        headers = {
            "Content-Type": "application/json"
        }

        response = requests.post(
            FLOW_URL,
            headers=headers,
            json={
                "eventUid": eventUid,
                "action": "complete",
                "orgUnit": orgUnit_uid,
                "program": program_uid,
                "PresidentName": accuity_search_text
            }
        )
        #print(data["status"])
        #print(data["eventUid"])
        #print(data["PresidentName"])
        #print(data["rawPageText"])
        
        # If HTTP error like 500, 502 etc.
        response.raise_for_status()

        data = json.loads(response.text)

        # ==========================
        # 1️⃣ HANDLE ERROR RESPONSE
        # ==========================
        if "error" in data:
            error_code = data["error"].get("code")
            error_message = data["error"].get("message")

            print(f"❌ Accuity Error: {error_code}")
            logging.error(f"Accuity Error: {error_message}")

            return ""   # return empty so loop continues
        
        #print("data -- ", data)
        # ==========================
        # 2️⃣ HANDLE SUCCESS RESPONSE
        # ==========================
        if data["status"] == "SUCCESS":
            
            print("✅ Accuity Response received")
            logging.info(f"Accuity Response received")
            
            temp_accuity_response_raw_text = data["rawPageText"]   
            
            #raw_text = data["rawPageText"]
            lines = temp_accuity_response_raw_text.splitlines()
            
            start_index = None
            for i, line in enumerate(lines):
                if "Names" in line and "Country/Region" in line and "Class" in line:
                    start_index = i + 1
                break

            finalRecords = []

            for line in lines[start_index:]:
                clean = " ".join(line.split())

                tokens = clean.split()

                if len(tokens) < 6:
                    continue  # too short to be a valid row

                # STEP 1: Extract Class (last token)
                class_value = tokens[-1]

                # STEP 2: Remaining tokens
                body = tokens[:-1]

                # STEP 3: Heuristic split
                # Name is usually shortest (1–3 tokens)
                # Country is next (1–3 tokens)
                # Position is longest (rest)

                for name_len in range(1, 4):
                    for country_len in range(1, 4):
                        if name_len + country_len >= len(body):
                            continue

                        name = " ".join(body[:name_len])
                        country = " ".join(body[name_len:name_len + country_len])
                        position = " ".join(body[name_len + country_len:])

                        # Position must be meaningful
                        if len(position.split()) < 3:
                            continue

                        finalRecords.append({
                            "Names": name,
                            "Country/Region": country,
                            "Position": position,
                            "Class": class_value
                        })

                        break
                    else:
                        continue
                    break

            if not finalRecords:
                
                accuity_response_raw_text = "No Records Found"
                print( f"2 --   No Records Found" )         
            else:
                accuity_response_raw_text = temp_accuity_response_raw_text

            return accuity_response_raw_text
        
        
        # ==========================
        # 3️⃣ UNKNOWN FORMAT
        # ==========================
        else:
            print("⚠ Unknown Accuity response format")
            logging.warning(f"Unknown response: {data}")
            return ""
            
    # ==========================
    # 4️⃣ NETWORK ERROR
    # ==========================
    except requests.exceptions.RequestException as e:
        print(f"🌐 Network error: {e}")
        logging.error(f"Network error: {e}")
        return ""

    # ==========================
    # 5️⃣ JSON ERROR
    # ==========================
    except json.JSONDecodeError:
        print("⚠ Invalid JSON response")
        logging.error("Invalid JSON response")
        return ""

    # ==========================
    # 6️⃣ ANY OTHER ERROR
    # ==========================
    except Exception as e:
        print(f"⚠ Unexpected error: {e}")
        logging.exception("Unexpected error")
        return ""    


def get_accuity_response_for_error(FLOW_URL, eventUid, orgUnit_uid, program_uid, accuity_search_text):

   #
    #FLOW_URL_updated = "https://default56af9532501a404c995d80633a35c0.ac.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/5e4d6b6437544b798b93c2035d0a66bd/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=qCVnbA5MH8um9u9Qwux6_xB0qlBxHgmHnNJ--kwPHOE"
    
    #FLOW_URL_updated = "https://default56af9532501a404c995d80633a35c0.ac.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/659d9a7a7b404fbfa426dfa84e486992/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=5VaBmHuGhyAYYnAumUf0eqdXPwOpue0aPICvxPgfthQ"
    
    ## new cloud flow  Cloud flow Keyword Search  link destop flow -- Accuity_RPA_Data_Entry - KeyWord Search
    FLOW_URL_updated = "https://default56af9532501a404c995d80633a35c0.ac.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/659d9a7a7b404fbfa426dfa84e486992/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=5VaBmHuGhyAYYnAumUf0eqdXPwOpue0aPICvxPgfthQ"


    try:
        print("Send to Accuity")
        logging.info("Send to Accuity")

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.post(
            FLOW_URL,
            headers=headers,
            json={
                "eventUid": eventUid,
                "action": "complete",
                "orgUnit": orgUnit_uid,
                "program": program_uid,
                "PresidentName": accuity_search_text
            },
            timeout=300   # prevent hanging forever
        )

        # If HTTP error like 500, 502 etc.
        response.raise_for_status()

        data = response.json()

        # ==========================
        # 1️⃣ HANDLE ERROR RESPONSE
        # ==========================
        if "error" in data:
            error_code = data["error"].get("code")
            error_message = data["error"].get("message")

            print(f"❌ Accuity Error: {error_code}")
            logging.error(f"Accuity Error: {error_message}")

            return ""   # return empty so loop continues

        # ==========================
        # 2️⃣ HANDLE SUCCESS RESPONSE
        # ==========================
        if data.get("status") == "SUCCESS":

            print("✅ Accuity Response received")
            logging.info("Accuity Response received")

            temp_accuity_response_raw_text = data.get("rawPageText", "")

            '''
            if not temp_accuity_response_raw_text:
                return "No Records Found"
            '''
            lines = temp_accuity_response_raw_text.splitlines()

            start_index = None
            for i, line in enumerate(lines):
                if "Names" in line and "Country/Region" in line and "Class" in line:
                    start_index = i + 1
                    break

            if start_index is None:
                return "No Records Found"

            finalRecords = []

            for line in lines[start_index:]:
                clean = " ".join(line.split())
                tokens = clean.split()

                if len(tokens) < 6:
                    continue

                class_value = tokens[-1]
                body = tokens[:-1]

                for name_len in range(1, 4):
                    for country_len in range(1, 4):

                        if name_len + country_len >= len(body):
                            continue

                        name = " ".join(body[:name_len])
                        country = " ".join(body[name_len:name_len + country_len])
                        position = " ".join(body[name_len + country_len:])

                        if len(position.split()) < 3:
                            continue

                        finalRecords.append({
                            "Names": name,
                            "Country/Region": country,
                            "Position": position,
                            "Class": class_value
                        })
                        break
                    else:
                        continue
                    break

            if not finalRecords:
                print("⚠ No Records Found")
                return "No Records Found"

            return temp_accuity_response_raw_text


        # ==========================
        # 3️⃣ UNKNOWN FORMAT
        # ==========================
        else:
            print("⚠ Unknown Accuity response format")
            logging.warning(f"Unknown response: {data}")
            return ""

    # ==========================
    # 4️⃣ NETWORK ERROR
    # ==========================
    except requests.exceptions.RequestException as e:
        print(f"🌐 Network error: {e}")
        logging.error(f"Network error: {e}")
        return ""

    # ==========================
    # 5️⃣ JSON ERROR
    # ==========================
    except json.JSONDecodeError:
        print("⚠ Invalid JSON response")
        logging.error("Invalid JSON response")
        return ""

    # ==========================
    # 6️⃣ ANY OTHER ERROR
    # ==========================
    except Exception as e:
        print(f"⚠ Unexpected error: {e}")
        logging.exception("Unexpected error")
        return ""




def get_tei_details(tei_get_url, session_get, ORGUNIT_UID,PROGRAM_UID, SEARCH_TEI_ATTRIBUTE_UID, SEARCH_VALUE ):
    
    #UIN code search
    #https://links.hispindia.org/ippf_uin/api/trackedEntityInstances.json?ou=iR2btIxN87s&ouMode=DESCENDANTS&program=GJbgrJjzCrr&filter=pkLdNynZWat:neq:%27%27
    #https://links.hispindia.org/ippf_uin/api/trackedEntityInstances.json?ou=iR2btIxN87s&ouMode=DESCENDANTS&program=GJbgrJjzCrr&filter=IzbdGgEgQ3T:eq:In%20Progress
    #tei_search_url = f"{tei_get_url}?ou={ORGUNIT_UID}&ouMode=DESCENDANTS&program={PROGRAM_UID}&filter=HKw3ToP2354:eq:{beneficiary_mapping_reg_id}"

    tei_search_url = (
        f"{tei_get_url}.json"
        f"?ou={ORGUNIT_UID}&ouMode=DESCENDANTS"
        f"&program={PROGRAM_UID}"
        f"&filter={SEARCH_TEI_ATTRIBUTE_UID}:eq:{SEARCH_VALUE}"
    )

    #print(tei_search_url)
    #print(f" event_search_url : {event_get_url}" )
    #response = requests.get(event_search_url, auth=HTTPBasicAuth(dhis2_username, dhis2_password))
    response = session_get.get(tei_search_url)
    
    if response.status_code == 200:
        tei_response_data = response.json()
        #print(response)
        #print(tei_response_data)
       
        #print(f"tei_response_data trackedEntityInstance : {tei_response_data.get('trackedEntityInstance')}" )
        teiattributesValue = tei_response_data.get('attributes',[])
        teis = tei_response_data.get('trackedEntityInstances', [])
        #print(f"teiattributesValue : {teiattributesValue}" )
        return teis 
    else:
        return []


def get_tei_event_details(tei_get_url, session_get, tei_uid, PROGRAM_STAGE_UID):

  #https://links.hispindia.org/ippf_uin/api/trackedEntityInstances/g2e5lEB62la.json?fields=enrollments[events[event,program,programStage,orgUnit,dataValues[dataElement,value]]]
    
    tei_events_url = (
        f"{tei_get_url}/{tei_uid}.json"
        f"?fields=enrollments[events[event,program,programStage,orgUnit,dataValues[dataElement,value]]]"
    )

    #print(tei_events_url)
    response = session_get.get(tei_events_url)

    if response.status_code != 200:
        return None

    data = response.json()

    # Loop through all enrollments
    for enrollment in data.get("enrollments", []):
        for event in enrollment.get("events", []):
            #print("tei_event:", event)
            #print("tei_event_programstage:", event.get("programStage"))
            if event.get("programStage") == PROGRAM_STAGE_UID:
                return event   # return first matching event

    return None   # if no matching event found

def push_dataStore_tei_in_dhis2( session_get, namespace_url, tei_uid , combined_key, dataStore_payload ):
    #print(f"dataValueSet_payload : {json.dumps(dataValueSet_payload)}")
    #logging.info(f"dataValueSet_payload : {json.dumps(dataValueSet_payload)}")

    dataStore_namespace_url = f"{namespace_url}{tei_uid}"
    #print(f"dataStore_namespace_url : {dataStore_namespace_url}")
    # Step 1: Get existing data
    response = session_get.get(dataStore_namespace_url)

    if response.status_code == 200:
        data_list = response.json()
    else:
        data_list = []

    # Step 2: Check if same event_uid + combined_key exists
    record_found = False

    for record in data_list:
        if (
            record.get("tei_uid") == tei_uid and
            combined_key in record
        ):
            record.update(dataStore_payload)   # update existing record
            record_found = True
            break

    # Step 3: Append if not found
    if not record_found:
        data_list.append(dataStore_payload)

    # Step 4: Save back
    if response.status_code == 200:
        save_response = session_get.put(dataStore_namespace_url, json=data_list)
    else:
        save_response = session_get.post(dataStore_namespace_url, json=data_list)

    print(f"DataStore created/Updated successfully for tei { tei_uid}. {save_response.text}")
    logging.info(f"DataStore created/Updated successfully for tei { tei_uid }. {save_response.text}")
    
 
def push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key, dataStore_payload ):
    #print(f"dataValueSet_payload : {json.dumps(dataValueSet_payload)}")
    #logging.info(f"dataValueSet_payload : {json.dumps(dataValueSet_payload)}")

    dataStore_namespace_url = f"{namespace_url}{tei_uid}"
    #print(f"dataStore_namespace_url : {dataStore_namespace_url}")
    # Step 1: Get existing data
    response = session_get.get(dataStore_namespace_url)

    if response.status_code == 200:
        data_list = response.json()
    else:
        data_list = []

    # Step 2: Check if same event_uid + combined_key exists
    record_found = False

    for record in data_list:
        if (
            record.get("event_uid") == event_uid and
            combined_key in record
        ):
            record.update(dataStore_payload)   # update existing record
            record_found = True
            break

    # Step 3: Append if not found
    if not record_found:
        data_list.append(dataStore_payload)

    # Step 4: Save back
    if response.status_code == 200:
        save_response = session_get.put(dataStore_namespace_url, json=data_list)
    else:
        save_response = session_get.post(dataStore_namespace_url, json=data_list)

    print(f"DataStore created/Updated successfully for event { event_uid}. {save_response.text}")
    logging.info(f"DataStore created/Updated successfully for event { event_uid }. {save_response.text}")
    


def get_dataStore_value(session_get, namespace_url, tei_uid, combined_key):

    dataStore_namespace_url = f"{namespace_url}{tei_uid}"

    response = session_get.get(dataStore_namespace_url)

    if response.status_code != 200:
        return False

    data_list = response.json()

    # If no records
    if not isinstance(data_list, list) or len(data_list) == 0:
        return False

    for record in data_list:

        # Check if this record matches your combined key
        if record.get("id") == combined_key:

            accuity_value = record.get(combined_key, "")

            if accuity_value and accuity_value.strip() != "":
                #print(f"Skipping Accuity push because value already exists: {accuity_value}")
                print(f"Skipping Accuity push because value already exists")
                return True      # Value exists

            else:
                print("Value is empty, calling Accuity API")
                return False     # Empty value → call API

    # If combined_key not found in any record
    return False

        


   

    



import requests
import json
import time
import logging

def get_accuity_response_multiple_call(FLOW_URL, eventUid, orgUnit_uid, program_uid, accuity_search_text):

    print("Send to Accuity")
    logging.info("Send to Accuity")

    headers = {
        "Content-Type": "application/json"
    }

    payload = {
        "eventUid": eventUid,
        "action": "complete",
        "orgUnit": orgUnit_uid,
        "program": program_uid,
        "PresidentName": accuity_search_text
    }

    MAX_RETRIES = 3
    RETRY_DELAY = 5   # seconds

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            print(f"🔁 Attempt {attempt}...")
            
            response = requests.post(
                FLOW_URL,
                headers=headers,
                json=payload,
                timeout=300   # prevent hanging forever
            )

            response.raise_for_status()

            data = response.json()

            # ======================
            # SUCCESS RESPONSE
            # ======================
            if data.get("status") == "SUCCESS":
                
                print("✅ Accuity Response received")
                logging.info("Accuity Response received")

                temp_accuity_response_raw_text = data.get("rawPageText", "")

                '''
                if not temp_accuity_response_raw_text:
                    return "No Records Found"
                '''
                lines = temp_accuity_response_raw_text.splitlines()

                start_index = None
                for i, line in enumerate(lines):
                    if "Names" in line and "Country/Region" in line and "Class" in line:
                        start_index = i + 1
                        break

                if start_index is None:
                    return "No Records Found"

                finalRecords = []

                for line in lines[start_index:]:
                    clean = " ".join(line.split())
                    tokens = clean.split()

                    if len(tokens) < 6:
                        continue

                    class_value = tokens[-1]
                    body = tokens[:-1]

                    for name_len in range(1, 4):
                        for country_len in range(1, 4):

                            if name_len + country_len >= len(body):
                                continue

                            name = " ".join(body[:name_len])
                            country = " ".join(body[name_len:name_len + country_len])
                            position = " ".join(body[name_len + country_len:])

                            if len(position.split()) < 3:
                                continue

                            finalRecords.append({
                                "Names": name,
                                "Country/Region": country,
                                "Position": position,
                                "Class": class_value
                            })
                            break
                        else:
                            continue
                        break
                '''
                if not finalRecords:
                    print("⚠ No Records Found")
                    #return "No Records Found"

                return temp_accuity_response_raw_text
                '''
                if not finalRecords:
                    accuity_response_raw_text = "No Records Found"
                    print( f"2 --   No Records Found" )         
                else:
                    accuity_response_raw_text = temp_accuity_response_raw_text

                return accuity_response_raw_text
            
            # ======================
            # ERROR IN RESPONSE
            # ======================
            if "error" in data:
                print(f"❌ Accuity Error: {data['error']}")
                logging.error(f"Accuity Error: {data['error']}")
                return ""

            print("⚠ Unknown response format")
            return ""

        # ======================
        # HANDLE 502 / 500 ERRORS
        # ======================
        except requests.exceptions.HTTPError as e:

            if response.status_code in [500, 502, 503, 504]:
                print(f"⚠ Server error {response.status_code}, retrying...")
                logging.warning(f"Server error {response.status_code}")

                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)  # exponential backoff
                    continue
                else:
                    print("❌ Max retries reached.")
                    return ""

            else:
                print(f"HTTP Error: {e}")
                return ""

        # ======================
        # NETWORK ERROR
        # ======================
        except requests.exceptions.RequestException as e:
            print(f"🌐 Network error: {e}")

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
                continue
            else:
                print("❌ Max retries reached.")
                return ""

        except Exception as e:
            print(f"⚠ Unexpected error: {e}")
            logging.exception("Unexpected error")
            return ""

    return ""



## for Accuity DHIS2 Integration END ######


#######################################

def sendEmail():
    # creates SMTP session
    #s = smtplib.SMTP('smtp.gmail.com', 587)
    # start TLS for security
    #s.starttls()
    # Authentication
    #s.login("ipamis@hispindia.org", "IPAMIS@12345")
    # message to be sent
    
    # message to be sent
    #message = "Message_you_need_to_send"

    # sending the mail
    #s.sendmail("ipamis@hispindia.org", "mithilesh.thakur@hispindia.org",message)
    #print(f"Email send to mithilesh.thakur@hispindia.org")
    # terminating the session
    #s.quit()
    
    #fromaddr = "dss.nipi@hispindia.org"
    fromaddr = FROM_EMAIL_ADDR
    # list of email_id to send the mail
    #li = ["mithilesh.thakur@hispindia.org", "saurabh.leekha@hispindia.org","dpatankar@nipi-cure.org","mohinder.singh@hispindia.org"]
    #li = ["mithilesh.thakur@hispindia.org","sumit.tripathi@hispindia.org","RKonda@fhi360.org"]
    li = ["mithilesh.thakur@hispindia.org"]

    for toaddr in li:

        #toaddr = "mithilesh.thakur@hispindia.org"
        
        # instance of MIMEMultipart 
        msg = MIMEMultipart() 
        
        # storing the senders email address   
        msg['From'] = fromaddr 
        
        # storing the receivers email address  
        msg['To'] = toaddr 
        
        # storing the subject  
        msg['Subject'] = "Climet data push from nepalhmis to climent instance log file"
        
        # string to store the body of the mail 
        #body = "Python Script test of the Mail"

        today_date = datetime.now().strftime("%Y-%m-%d")
        #updated_odk_api_url = f"{ODK_API_URL}?$filter=__system/submissionDate ge {today_date}"
        updated_odk_api_url = f"{today_date}"

        body = f"Climet data push from nepalhmis to climent instance log file"
        
        # attach the body with the msg instance 
        msg.attach(MIMEText(body, 'plain')) 
        
        
        # open the file to be sent  

        LOG_DIR = "logs"
        PATTERN = "*_dataValueSet_post.log"

        # Find latest matching log file
        log_files = glob.glob(os.path.join(LOG_DIR, PATTERN))
        if not log_files:
            raise FileNotFoundError("No log files found")

        latest_log = max(log_files, key=os.path.getmtime)

        filename = LOG_FILE
        #attachment = open(filename, "rb") 
        attachment = open(latest_log, "rb") 
        
        # instance of MIMEBase and named as p 
        p = MIMEBase('application', 'octet-stream') 
        
        # To change the payload into encoded form 
        p.set_payload((attachment).read()) 
        
        # encode into base64 
        encoders.encode_base64(p) 
        
        p.add_header('Content-Disposition', "attachment; filename= %s" % filename) 
        
        # attach the instance 'p' to instance 'msg' 
        msg.attach(p) 
        try:
            # creates SMTP session 
            s = smtplib.SMTP('smtp.gmail.com', 587) 
            
            # start TLS for security 
            s.starttls() 
            
            # Authentication 
            #s.login(fromaddr, "NIPIODKHispIndia@123")
            #s.login(fromaddr, "dztnzuvhbxlauwxy") ## set app password App Name Mail as on 22/12/2025
            s.login(fromaddr, FROM_EMAIL_PASSWORD)
            

            # Converts the Multipart msg into a string 
            text = msg.as_string() 
            
            # sending the mail 
            s.sendmail(fromaddr, toaddr, text) 
            print(f"mail send to: {toaddr}")
            log_info(f"mail send to: {toaddr}")
            # terminating the session 
            s.quit()
        except Exception as exception:
            print("Error: %s!\n\n" % exception)
