#!/usr/local/bin/python3

import requests
import json

KIBANA_SERVER="https://my.kibana.int"
INDEX_NAME="my-index-*"
SCROLL_TIME="5m"
BATCH_SIZE=10000  # Set to a large value for maximum batch size, 10k tops
MAX_DOCS=50000000  # Set the maximum number of documents to retrieve
USERNAME="USERNAME" # Set to your username
PASSWORD="PASSWORD" # Set to tour password
OUTPUT_FILE = "documents.json"  # Output file to write documents

# Function to open a Point in Time (PIT)
def open_pit():
    print("Opening PIT ...")
    url = f"{KIBANA_SERVER}/api/console/proxy?path=/{INDEX_NAME}/_pit?keep_alive=5m&method=POST"
    headers = {
        'kbn-xsrf': 'true',
        'Content-Type': 'application/json'
    }

    response = requests.post(url, auth=(USERNAME, PASSWORD), headers=headers)

    if response.status_code == 200:
        pit_id = response.json().get('id')
        print(f"PIT opened with ID.")
        return pit_id
    else:
        print(f"Failed to open PIT: {response.status_code}, {response.text}")
        return None

# Function to perform the search using PIT and paginate through results
def search_with_pit(pit_id):
    total_hits = 0
    batch_count = 1
    search_url = f"{KIBANA_SERVER}/api/console/proxy?path=/_search&method=POST"
    headers = {
        'kbn-xsrf': 'true',
        'Content-Type': 'application/json'
    }



    # put filters in "query", currently no filters - "match_all"
    query = {
        "size": BATCH_SIZE,
        "pit": {
            "id": pit_id,
            "keep_alive": "5m"
        },
        "query": {
            "match_all": {}
        },
        "sort": [
            {"@timestamp": {"order": "asc", "format": "strict_date_optional_time_nanos", "numeric_type" : "date_nanos" }}
        ]
    }
    with open(OUTPUT_FILE, 'w') as outfile:
        while total_hits < MAX_DOCS:
            print(f"Fetching batch {batch_count}...")

            response = requests.post(search_url, auth=(USERNAME, PASSWORD), headers=headers, data=json.dumps(query))

            if response.status_code == 200:
                search_response = response.json()
                hits = search_response['hits']['hits']
                num_hits = len(hits)

                if num_hits == 0:
                    break

                total_hits += num_hits
                print(f"Received {num_hits} documents in batch {batch_count}. Total: {total_hits}")

                for doc in hits:
                    json.dump(doc, outfile)
                    outfile.write('\n')

                if total_hits >= MAX_DOCS:
                    print('MAX_DOCS reached, exiting ...')
                    break

                last_sort = hits[-1]['sort']
                query['search_after'] = last_sort
                batch_count += 1
            else:
                print(f"Error during search: {response.status_code}, {response.text}")
                break

    outfile.close()

# Function to close the PIT
def close_pit(pit_id):
    print("Closing Point In Time (PIT)...")
    close_pit_url = f"{KIBANA_SERVER}/api/console/proxy?path=/_pit&method=DELETE"
    headers = {
        'kbn-xsrf': 'true',
        'Content-Type': 'application/json'
    }
    data = json.dumps({
        "id": pit_id
    })

    response = requests.post(close_pit_url, auth=(USERNAME, PASSWORD), headers=headers, data=data)

    if response.status_code == 200:
        print("PIT closed.")
    else:
        print(f"Failed to close PIT: {response.status_code}, {response.text}")

# Main logic
if __name__ == "__main__":
    pit_id = open_pit()

    if pit_id:
        search_with_pit(pit_id)
        close_pit(pit_id)
    else:
        print("Failed to open PIT. Exiting...")
