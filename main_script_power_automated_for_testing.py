import requests
import logging, datetime
import json
from dotenv import load_dotenv
import os

## new cloud flow  Cloud flow Keyword Search  link destop flow -- Accuity_RPA_Data_Entry - KeyWord Search
ACCUITY_FLOW_URL_NEW = os.getenv("ACCUITY_FLOW_URL_NEW")

## new Cloud flow Single Search Organization Search  link destop flow -- Single Search Organization
ACCUITY_FLOW_URL_ORG_UNIT_SEARCH = os.getenv("ACCUITY_FLOW_URL_ORG_UNIT_SEARCH")
'''
payload = {
    "eventUid": "abc123",
    "action": "complete"
    "PresidentName": "sonu singh AXWPS8419G"
     "PresidentName": "test 1111"
     "PresidentName": "Antigua Planned Parenthood Organisation ATG/2007/2232"
}

r =  requests.post(FLOW_URL, json=payload)

#r = requests.post(url, json=payload)

print( f"send to power automated " )

#r = requests.post(url, json=payload)

print(r.status_code)
print(r.json())

print(r)
print(r.text)

'''

current_time_start = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print( f"send to power automated start . { current_time_start }" )

headers = {
    "Content-Type": "application/json"
}


response = requests.post(
    ACCUITY_FLOW_URL_NEW,
    headers=headers,
    json={
        "eventUid": "abc123",
        "action": "complete",
        "orgUnit": "OU_01",
        "program": "Prog_01",
        "PresidentName": "Ms.Ingrid Daniel ACJPL627361N"
    }
)

## OrganizationName serach "EntityType":"Organization",
## "OrganizationName": "Bank of Ceylon"
'''
response = requests.post(
    ACCUITY_FLOW_URL_ORG_UNIT_SEARCH,
    headers=headers,
    json={
        "eventUid": "abc123",
        "action": "complete",
        "orgUnit": "OU_01",
        "program": "Prog_01",
        "EntityType":"Organization",
        "OrganizationName": "Commonwealth Bank of Australia"
    }
)
'''
#print( f"send to power automated " )

'''
print(response.json())

print(response.status_code)
print(response.json())

print(response)
print(response.text)
'''
print(response.json())

current_time_end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print( f"Received data from power automated  . { current_time_end }" )

print("##################################################################")
#print(response.text)

data = json.loads(response.text)

print("Status", data["status"])
print("eventUid", data["eventUid"])
print("PresidentName", data["PresidentName"])
#print("EntityType", data["EntityType"])
#print("OrganizationName", data["OrganizationName"])
print("rawPageText", data["rawPageText"])


#raw_json = json.loads(data["rawPageText"])
#print(raw_json["status"])
#print(raw_json["searchValue"])
#print(raw_json["timestamp"])

#raw_text = raw_json["rawPageText"]
#lines = raw_text.splitlines()


raw_text = data["rawPageText"]
lines = raw_text.splitlines()

#raw_text = data["rawPageText"]
### Step 1: Normalize lines
tempLines = [l.strip() for l in raw_text.splitlines() if l.strip()]
print( f"send to power automated tempLines -- { tempLines }" )


## Step 2: Find header position
start_index = None
for i, line in enumerate(lines):
    if "Names" in line and "Country/Region" in line and "Class" in line:
        start_index = i + 1
        break


## Step 3: Generic parser (NO country list)
records = []

for line in lines[start_index:]:
    if "PEP" not in line:
        continue

    clean = " ".join(line.split())

    # Split class (last column)
    parts = clean.rsplit(" ", 1)
    if len(parts) != 2:
        continue

    row_body, class_value = parts
    if class_value != "PEP":
        continue

    tokens = row_body.split()

    if len(tokens) < 4:
        continue  # too short to be valid

    # Heuristic:
    # Country is the FIRST token after name block
    # Position is LONG text → name is SHORT
    # We assume name ends before first capitalized country-like token

    # Simple & safe assumption:
    # First token that is NOT part of name punctuation marks the country
    country_index = 1  # default fallback

    for i in range(1, len(tokens)):
        if tokens[i][0].isupper():
            country_index = i
            break

    name = " ".join(tokens[:country_index])
    country = tokens[country_index]
    position = " ".join(tokens[country_index + 1:])

    records.append({
        "Names": name,
        "Country/Region": country,
        "Position": position,
        "Class": class_value
    })


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


print( f"final records -- { finalRecords }" )

if len(finalRecords) == 0:
    print( f"1 --   No Records Found" )

if not finalRecords:
    print( f"2 --   No Records Found" )



###

'''
####Clean PEP extraction



pep_entries = []

for line in lines:
    if "PEP" in line and "India" in line:
        pep_entries.append(line.strip())

print(pep_entries)

####Structured parsing (better)
records = []

for line in lines:
    if "India" in line and "PEP" in line:
        records.append({
            "Country": "India",
            "RawLine": line.strip()
        })

print(records)

####If you REALLY want to extract Name / Position
records = []

for line in lines:
    if "India" in line and "PEP" in line:
        parts = line.split()
        country_index = parts.index("India")

        name = " ".join(parts[:country_index])
        position = " ".join(parts[country_index + 1:-1])

        records.append({
            "Name": name,
            "Country": "India",
            "Position": position,
            "Class": "PEP"
        })

print(records)
'''


