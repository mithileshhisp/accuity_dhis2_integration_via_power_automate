## install
#pip install python-dotenv
#pip install psycopg2-binary
#pip install clickhouse-connect
#pip install --upgrade certifi
#pip install --upgrade requests certifi urllib3 ## for post data in hmis production certificate issue

import urllib3 ## for disable warning of Certificate
urllib3.disable_warnings() ## for disable warning of Certificate

import ssl
#import requests

from concurrent.futures import ThreadPoolExecutor
import requests
import certifi  ## for post data in hmis production certificate issue
import json
from datetime import datetime,date
import nepali_datetime
# main.py
from dotenv import load_dotenv
import os

load_dotenv()

from utils import (
    configure_logging,get_tei_details,
    log_info,log_error,get_tei_event_details,
    push_dataStore_tei_in_dhis2,push_dataStore_event_in_dhis2,
    get_bs_month_start_end, get_between_dates_iso, sendEmail
   
)

#print("OpenSSL version:", ssl.OPENSSL_VERSION)
#print("Certifi CA bundle:", requests.certs.where())

DHIS2_GET_API_URL = os.getenv("DHIS2_GET_API_URL")
DHIS2_GET_USER = os.getenv("DHIS2_GET_USER")
DHIS2_GET_PASSWORD = os.getenv("DHIS2_GET_PASSWORD")

DHIS2_POST_API_URL = os.getenv("DHIS2_POST_API_URL")
DHIS2_POST_USER = os.getenv("DHIS2_POST_USER")
DHIS2_POST_PASSWORD = os.getenv("DHIS2_POST_PASSWORD")


PROGRAM_UID = os.getenv("PROGRAM_UID")
PROGRAM_STAGE_UID = os.getenv("PROGRAM_STAGE_UID")
SEARCH_TEI_ATTRIBUTE_UID = os.getenv("SEARCH_TEI_ATTRIBUTE_UID")
SEARCH_VALUE = os.getenv("SEARCH_VALUE")
ORGUNIT_UID = os.getenv("ORGUNIT_UID")

tei_get_url = f"{DHIS2_GET_API_URL}trackedEntityInstances"

dataValueSet_endPoint = f"{DHIS2_POST_API_URL}dataValueSets"

namespace_url = f"{DHIS2_GET_API_URL}dataStore/accuityResponse/"

#print( f" DHIS2_GET_USER. { DHIS2_GET_USER }, DHIS2_GET_PASSWORD  { DHIS2_GET_PASSWORD} " )

#DHIS2_AUTH_POST = ("hispdev", "Devhisp@1")
#session_post = requests.Session()
#session_post.auth = DHIS2_AUTH_POST

# Create a session object for persistent connection
#session_get = requests.Session()
#session_get.auth = DHIS2_AUTH_GET

raw_auth = os.getenv("DHIS2_AUTH")

if raw_auth is None:
    raise ValueError("DHIS2_AUTH is missing in .env")

if ":" not in raw_auth:
    raise ValueError("DHIS2_AUTH must be in user:password format")

user, pwd = raw_auth.split(":", 1)
#session_get.auth = (user, pwd)
'''
session_get = requests.Session()
session_get.auth = (DHIS2_GET_USER, DHIS2_GET_PASSWORD)

session_post = requests.Session()
session_post.auth = (DHIS2_POST_USER, DHIS2_POST_PASSWORD)
'''

#session_get.verify = False


def main_with_logger():

    configure_logging()

    current_time_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print( f"Accuity to dhis2 integration process start . { current_time_start }" )
    log_info(f"Accuity to dhis2 integration process start  . { current_time_start }")

    session_get = requests.Session()
    session_get.auth = (DHIS2_GET_USER, DHIS2_GET_PASSWORD)

    session_post = requests.Session()
    session_post.auth = (DHIS2_POST_USER, DHIS2_POST_PASSWORD)

    #session = requests.Session()
    #session_post.verify = certifi.where()

    tei_list = get_tei_details( tei_get_url, session_get, ORGUNIT_UID,PROGRAM_UID,SEARCH_TEI_ATTRIBUTE_UID, SEARCH_VALUE )
    
    print(f"trackedEntityInstances list Size {len(tei_list) }")
    log_info(f"trackedEntityInstances list Size {len(tei_list) } ")

    if tei_list:

        for tei in tei_list:
            tei_uid = tei["trackedEntityInstance"]
            org_unit = tei["orgUnit"]

            # Convert attributes list into dictionary
            attributes_dict = {
                #attr["displayName"]: attr.get("value", "")
                attr["attribute"]: attr.get("value", "")
                for attr in tei.get("attributes", [])
            }

            print("TEI UID:", tei_uid)
            print("Org Unit:", org_unit)
            #print("Legal Name:", attributes_dict.get("Legal Name"))
            print("Legal Name:", attributes_dict.get("UkQI1dWzZOv"))
            print("Registration Number:", attributes_dict.get("qsASQ0NRTVA"))
            

            if attributes_dict.get("UkQI1dWzZOv") and attributes_dict.get("qsASQ0NRTVA"):
                
                attr1 = "UkQI1dWzZOv"
                attr2 = "qsASQ0NRTVA"
                combined_key_attr = f"{attr1}_{attr2}"
                new_object = {
                    "date": datetime.now().isoformat() + "Z",
                    "tei_uid": tei_uid,
                    attr1: attributes_dict.get("UkQI1dWzZOv"),
                    attr2: attributes_dict.get("qsASQ0NRTVA"),
                    combined_key_attr: "accuity_response_tei_attribute"
                }

                push_dataStore_tei_in_dhis2( session_get, namespace_url, tei_uid,  combined_key_attr, new_object )

                #namespace_url = f"{DHIS2_GET_API_URL}dataStore/accuityResponse/{tei_uid}"

                
            #print("Country:", attributes_dict.get("Country of Registration"))
            #print("Region:", attributes_dict.get("Region"))
            #print("Organisation Type:", attributes_dict.get("Organisation Type "))
            #print("Registration Number:", attributes_dict.get("Registration Number"))
            #print("Acuity Status:", attributes_dict.get("Acuity Status"))

            tei_event = get_tei_event_details(tei_get_url, session_get, tei_uid, PROGRAM_STAGE_UID )
            #print("tei_event:", tei_event)

            if tei_event:
                event_uid = tei_event.get("event")
                print("EVENT UID:", event_uid)
                programStage = tei_event.get("programStage")
                print("programStage:", programStage)
                
                event_datavalues_dict = {
                    dataValue["dataElement"]: dataValue["value"]
                    for dataValue in tei_event.get("dataValues", [])
                }
                #print("President Name:", event_datavalues_dict.get("daG91uRV8pi"))
                #print("President National ID/Tax ID:", event_datavalues_dict.get("DhSKMFMRH84"))
                if event_datavalues_dict.get("daG91uRV8pi") and event_datavalues_dict.get("DhSKMFMRH84"):
                    print("President Name:", event_datavalues_dict.get("daG91uRV8pi"))
                    print("President National ID/Tax ID:", event_datavalues_dict.get("DhSKMFMRH84"))
                    de1 = "daG91uRV8pi"
                    de2 = "DhSKMFMRH84"
                    combined_key_de1 = f"{de1}_{de2}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de1: event_datavalues_dict.get("daG91uRV8pi"),
                        de2: event_datavalues_dict.get("DhSKMFMRH84"),
                        combined_key_de1: "accuity_response_new1"
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de1, new_object )
                    
                    #namespace_url = f"{DHIS2_GET_API_URL}dataStore/accuityResponse/{tei_uid}"

                    '''
                    # Step 1: Get existing data
                    response = session_get.get(namespace_url)

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
                            record.update(new_object)   # update existing record
                            record_found = True
                            break

                    # Step 3: Append if not found
                    if not record_found:
                        data_list.append(new_object)

                    # Step 4: Save back
                    if response.status_code == 200:
                        save_response = session_get.put(namespace_url, json=data_list)
                    else:
                        save_response = session_get.post(namespace_url, json=data_list)

                    print(save_response.status_code)
                    print(save_response.text)
                    '''
                if event_datavalues_dict.get("uT1NdSet4eo") and event_datavalues_dict.get("LGaOnTyfRJ2"):
                    print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    de3 = "uT1NdSet4eo"
                    de4 = "LGaOnTyfRJ2"
                    combined_key_de2 = f"{de3}_{de4}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de3: event_datavalues_dict.get("uT1NdSet4eo"),
                        de4: event_datavalues_dict.get("LGaOnTyfRJ2"),
                        combined_key_de2: "accuity_response_new2"
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de2, new_object )
                    '''
                    namespace_url = f"{DHIS2_GET_API_URL}dataStore/accuityResponse/{tei_uid}"

                    # Step 1: Get existing data
                    response = session_get.get(namespace_url)

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
                            record.update(new_object)   # update existing record
                            record_found = True
                            break

                    # Step 3: Append if not found
                    if not record_found:
                        data_list.append(new_object)

                    # Step 4: Save back
                    if response.status_code == 200:
                        save_response = session_get.put(namespace_url, json=data_list)
                    else:
                        save_response = session_get.post(namespace_url, json=data_list)

                    print(save_response.status_code)
                    print(save_response.text)   
                    '''    
            print("-" * 50)


   
        #print( f"dataValueSet_payload . { dataValueSet_payload }" )
        #push_dataValueSet_in_dhis2( dataValueSet_endPoint, session_post, dataValueSet_payload )
    
if __name__ == "__main__":

    #main()
    main_with_logger()
    current_time_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print( f"Accuity to dhis2 integration process finished . { current_time_end }" )
    log_info(f"Accuity to dhis2 integration process finished . { current_time_end }")

    try:
        #sendEmail()
        print( f"Accuity to dhis2 integration process finished . { current_time_end }" )
    except Exception as e:
        log_error(f"Email failed: {e}")


    #sendEmail()
    #print(f"total_patient_count. {total_patient_count}, null_patient_id_count. {null_patient_id_count}, event_push_count {event_push_count}")
    #log_info(f"total_patient_count. {total_patient_count}, null_patient_id_count. {null_patient_id_count}, event_push_count {event_push_count}")
    