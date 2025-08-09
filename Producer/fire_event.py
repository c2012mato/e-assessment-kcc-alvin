import functions_framework
import uuid
from flask import jsonify, Request
from google.cloud import pubsub_v1
import os
import json
from datetime import datetime, timedelta
import pytz
import time
import random
##------------------------------------------------------

PROJECT_ID = os.environ.get("PROJECT_ID")
TOPIC_ID = os.environ.get("TOPIC_ID")
API_KEY = os.environ.get("API_KEY")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Asia/Manila timezone
tz = pytz.timezone("Asia/Manila")

def now_ts():
    return datetime.now(tz).isoformat()

def random_sleep():
    delay = random.uniform(30, 60)
    time.sleep(delay)

def random_userid():
    return random.randint(111,999)

@functions_framework.http
def fire_event(request: Request):
    # some handlers for cloud run functions
    """ 
    accepts payload : {
        action : "Start"
        duration : some minutes # Max allowed : 20
    }
    """
    # Only accept POST requests
    if request.method != "POST":
        return jsonify({"error": "Method not allowed"}), 405

    # API Key check
    req_api_key = request.headers.get("Api-Key")
    if req_api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json()
    if not payload or "action" not in payload:
        return jsonify({"error": "Missing action"}), 400

    action = payload["action"]
    duration = payload.get("duration")
    # some handlers for cloud run functions

    if action == "Start":
        if not duration or not isinstance(duration, (int, float)) or duration > 20:
            return jsonify({"error": "Missing or invalid duration or Exceeded maximum allowed 20 minutes"}), 400
        start_time = datetime.now(tz) 
        end_time = start_time + timedelta(minutes=duration)
        sent_events = []
        event_count = 0
        while datetime.now(tz) < end_time:
            event_count += 1
            event_id = str(uuid.uuid4())
            user_id = str(random_userid())
            ts_now = now_ts()
            event = {
                "event_id": event_id,
                "event_name": "click",
                "user_id": user_id,
                "event_timestamp": ts_now,
                "received_timestamp": ts_now,
                "is_valid": True
            }

            # set 10% chance of getting 30-60 sleep from random events
            if random.random() < 0.10:
                random_sleep()

            # Publish events to pubsub and wait for publish to finish
            future = publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
            future.result() 
            sent_events.append(event)

            # pick only from valid events with 5-10% chance and resend the event with updated is_valid = False and updated receive_timestamp
            valid_events = [e for e in sent_events if e["is_valid"]==True]
            if valid_events and random.random() < random.uniform(0.05, 0.10):
                past_event = random.choice(valid_events)
                correction_event = past_event.copy()
                correction_event["is_valid"] = False
                correction_event["received_timestamp"] = now_ts()
                
                # apply delay before correction
                time.sleep(random.uniform(1, 5))  
                future_correction = publisher.publish(topic_path, json.dumps(correction_event).encode("utf-8"))
                future_correction.result()

            time.sleep(1)
        return_value = jsonify({
            "status" : "Successful",
            "start_time" : start_time.isoformat(),
            "end_time" : end_time.isoformat(),
            "duration" : duration
        })
        return return_value, 200
    else : 
        return jsonify({"error": "Invalid action"}), 400
