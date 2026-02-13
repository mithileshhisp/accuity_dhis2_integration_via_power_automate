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
#import nepali_datetime
# main.py
from dotenv import load_dotenv
import os

load_dotenv()

from utils import (
    configure_logging,get_tei_details,
    log_info,log_error,get_tei_event_details,get_accuity_response,
    push_dataStore_tei_in_dhis2,push_dataStore_event_in_dhis2
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
ACCUITY_FLOW_URL = os.getenv("ACCUITY_FLOW_URL_NEW")
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

    tei_list = get_tei_details( tei_get_url, session_get, ORGUNIT_UID, PROGRAM_UID, SEARCH_TEI_ATTRIBUTE_UID, SEARCH_VALUE )
    
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
                
                accuity_search_text = attributes_dict.get("UkQI1dWzZOv") + " " + attributes_dict.get("qsASQ0NRTVA")
                accuity_search_response_tei_attribute = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                attr1 = "UkQI1dWzZOv"
                attr2 = "qsASQ0NRTVA"
                combined_key_attr = f"{attr1}_{attr2}"
                new_object = {
                    "date": datetime.now().isoformat() + "Z",
                    "tei_uid": tei_uid,
                    attr1: attributes_dict.get("UkQI1dWzZOv"),
                    attr2: attributes_dict.get("qsASQ0NRTVA"),
                    combined_key_attr: accuity_search_response_tei_attribute
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
                    #print("President Name:", event_datavalues_dict.get("daG91uRV8pi"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("DhSKMFMRH84"))
                    accuity_search_text = event_datavalues_dict.get("daG91uRV8pi") + " " + event_datavalues_dict.get("DhSKMFMRH84")
                    
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de1 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de1 = "daG91uRV8pi"
                    de2 = "DhSKMFMRH84"
                    combined_key_de1 = f"{de1}_{de2}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de1: event_datavalues_dict.get("daG91uRV8pi"),
                        de2: event_datavalues_dict.get("DhSKMFMRH84"),
                        combined_key_de1: accuity_search_response_event_de1
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de1, new_object )
                    
                    #namespace_url = f"{DHIS2_GET_API_URL}dataStore/accuityResponse/{tei_uid}"

                if event_datavalues_dict.get("uT1NdSet4eo") and event_datavalues_dict.get("LGaOnTyfRJ2"):
                    #print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    accuity_search_text = event_datavalues_dict.get("uT1NdSet4eo") + " " + event_datavalues_dict.get("LGaOnTyfRJ2")
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de2 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de3 = "uT1NdSet4eo"
                    de4 = "LGaOnTyfRJ2"
                    combined_key_de2 = f"{de3}_{de4}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de3: event_datavalues_dict.get("uT1NdSet4eo"),
                        de4: event_datavalues_dict.get("LGaOnTyfRJ2"),
                        combined_key_de2: accuity_search_response_event_de2
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de2, new_object )
                    
                if event_datavalues_dict.get("DMJOfwrOwo8") and event_datavalues_dict.get("kezRO5k8bYy"):
                    #print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    accuity_search_text = event_datavalues_dict.get("DMJOfwrOwo8") + " " + event_datavalues_dict.get("kezRO5k8bYy")
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de3 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de5 = "DMJOfwrOwo8"
                    de6 = "kezRO5k8bYy"
                    combined_key_de3 = f"{de5}_{de6}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de5: event_datavalues_dict.get("DMJOfwrOwo8"),
                        de6: event_datavalues_dict.get("kezRO5k8bYy"),
                        combined_key_de3: accuity_search_response_event_de3
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de3, new_object )

                if event_datavalues_dict.get("fKFIKK33FRc") and event_datavalues_dict.get("ZqxEuYK8vUB"):
                    #print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    accuity_search_text = event_datavalues_dict.get("fKFIKK33FRc") + " " + event_datavalues_dict.get("ZqxEuYK8vUB")
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de4 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de7 = "fKFIKK33FRc"
                    de8 = "ZqxEuYK8vUB"
                    combined_key_de4 = f"{de7}_{de8}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de7: event_datavalues_dict.get("fKFIKK33FRc"),
                        de8: event_datavalues_dict.get("ZqxEuYK8vUB"),
                        combined_key_de4: accuity_search_response_event_de4
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de4, new_object )
                        
                if event_datavalues_dict.get("xCJOBTvagP9") and event_datavalues_dict.get("NHoDQ5DC1jY"):
                    #print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    accuity_search_text = event_datavalues_dict.get("xCJOBTvagP9") + " " + event_datavalues_dict.get("NHoDQ5DC1jY")
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de5 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de9 = "xCJOBTvagP9"
                    de10 = "NHoDQ5DC1jY"
                    combined_key_de5= f"{de9}_{de10}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de9: event_datavalues_dict.get("xCJOBTvagP9"),
                        de10: event_datavalues_dict.get("NHoDQ5DC1jY"),
                        combined_key_de5: accuity_search_response_event_de5
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de5, new_object )
                        
                if event_datavalues_dict.get("RA5zVHd7pVO") and event_datavalues_dict.get("VWdVRyHFlBh"):
                    #print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    accuity_search_text = event_datavalues_dict.get("RA5zVHd7pVO") + " " + event_datavalues_dict.get("VWdVRyHFlBh")
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de6 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de11 = "RA5zVHd7pVO"
                    de12 = "VWdVRyHFlBh"
                    combined_key_de6= f"{de11}_{de12}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de11: event_datavalues_dict.get("RA5zVHd7pVO"),
                        de12: event_datavalues_dict.get("VWdVRyHFlBh"),
                        combined_key_de6: accuity_search_response_event_de6
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de6, new_object )

                if event_datavalues_dict.get("glFVJpRaGWK") and event_datavalues_dict.get("vcR9TS21A05"):
                    #print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    accuity_search_text = event_datavalues_dict.get("glFVJpRaGWK") + " " + event_datavalues_dict.get("vcR9TS21A05")
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de7 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de13 = "glFVJpRaGWK"
                    de14 = "vcR9TS21A05"
                    combined_key_de7= f"{de13}_{de14}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de13: event_datavalues_dict.get("glFVJpRaGWK"),
                        de14: event_datavalues_dict.get("vcR9TS21A05"),
                        combined_key_de7: accuity_search_response_event_de7
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de7, new_object )
                
                if event_datavalues_dict.get("U4OSVfrlPxQ") and event_datavalues_dict.get("A46ZGJLezyc"):
                    #print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    accuity_search_text = event_datavalues_dict.get("U4OSVfrlPxQ") + " " + event_datavalues_dict.get("A46ZGJLezyc")
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de8 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de15 = "U4OSVfrlPxQ"
                    de16 = "A46ZGJLezyc"
                    combined_key_de8= f"{de15}_{de16}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de15: event_datavalues_dict.get("U4OSVfrlPxQ"),
                        de16: event_datavalues_dict.get("A46ZGJLezyc"),
                        combined_key_de8: accuity_search_response_event_de8
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de8, new_object )

                if event_datavalues_dict.get("YjmSPK8DMOZ") and event_datavalues_dict.get("nY0g2hnfnUB"):
                    #print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    accuity_search_text = event_datavalues_dict.get("YjmSPK8DMOZ") + " " + event_datavalues_dict.get("nY0g2hnfnUB")
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de9 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de17 = "YjmSPK8DMOZ"
                    de18 = "nY0g2hnfnUB"
                    combined_key_de9 = f"{de17}_{de18}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de17: event_datavalues_dict.get("YjmSPK8DMOZ"),
                        de18: event_datavalues_dict.get("nY0g2hnfnUB"),
                        combined_key_de9: accuity_search_response_event_de9
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de9, new_object )
                                                
                if event_datavalues_dict.get("TfCXfVv6j2O") and event_datavalues_dict.get("WY7Aao5rT82"):
                    #print("Vice President Name:", event_datavalues_dict.get("uT1NdSet4eo"))
                    #print("President National ID/Tax ID:", event_datavalues_dict.get("LGaOnTyfRJ2"))
                    accuity_search_text = event_datavalues_dict.get("TfCXfVv6j2O") + " " + event_datavalues_dict.get("WY7Aao5rT82")
                    print(f"accuity_search_text:, {accuity_search_text}")
                    accuity_search_response_event_de10 = get_accuity_response(ACCUITY_FLOW_URL, tei_uid, ORGUNIT_UID, PROGRAM_UID, accuity_search_text )    

                    de19 = "TfCXfVv6j2O"
                    de20 = "WY7Aao5rT82"
                    combined_key_de10 = f"{de19}_{de20}"
                    new_object = {
                        "date": datetime.now().isoformat() + "Z",
                        "event_uid": event_uid,
                        de19: event_datavalues_dict.get("TfCXfVv6j2O"),
                        de20: event_datavalues_dict.get("WY7Aao5rT82"),
                        combined_key_de10: accuity_search_response_event_de10
                    }

                    push_dataStore_event_in_dhis2( session_get, namespace_url, tei_uid, event_uid, combined_key_de10, new_object )
                         

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
    