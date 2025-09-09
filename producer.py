from azure.identity import DefaultAzureCredential
from azure.eventhub import EventHubProducerClient, EventData
import requests
import json
import os
import time
from datetime import datetime

# Get environment variables
EVENTHUB_FULLY_QUALIFIED_NAMESPACE = os.getenv("EVENTHUB_NAMESPACE")  # e.g., "eq-eh-namespace.servicebus.windows.net"
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME", "earthquakes")
USGS_FEED = os.getenv("USGS_FEED", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))  # 5 minutes default

def send_to_eventhub(events):
    """Send a batch of events to Event Hub using Managed Identity"""
    credential = DefaultAzureCredential()
    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENTHUB_FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        credential=credential
    )
    
    with producer:
        event_data_batch = producer.create_batch()
        for event in events:
            event_data_batch.add(EventData(json.dumps(event)))
        producer.send_batch(event_data_batch)
    print(f"Sent batch of {len(events)} events at {datetime.utcnow().isoformat()}")

def fetch_earthquake_data():
    """Fetch data from USGS API"""
    try:
        print(f"Fetching data from {USGS_FEED}")
        r = requests.get(USGS_FEED, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def process_data(feed_data):
    """Process the GeoJSON feed into our event format"""
    events = []
    if feed_data and 'features' in feed_data:
        for feature in feed_data['features']:
            props = feature.get('properties', {})
            geom = feature.get('geometry', {})
            event = {
                "id": feature.get("id"),
                "time": props.get("time"),
                "mag": props.get("mag"),
                "place": props.get("place"),
                "coordinates": geom.get("coordinates"),
                "type": props.get("type"),
                "processed_time": datetime.utcnow().isoformat()
            }
            events.append(event)
    return events

def main():
    print("Starting USGS Earthquake Data Producer")
    print(f"Polling interval: {POLL_INTERVAL} seconds")
    
    while True:
        try:
            # Fetch, process, and send data
            feed_data = fetch_earthquake_data()
            if feed_data:
                events = process_data(feed_data)
                if events:
                    send_to_eventhub(events)
                    print(f"Processed {len(events)} earthquake events")
                else:
                    print("No earthquake events found in feed")
            else:
                print("Failed to fetch data from USGS")
        
        except Exception as e:
            print(f"Unexpected error: {e}")
        
        # Wait for the next poll
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()