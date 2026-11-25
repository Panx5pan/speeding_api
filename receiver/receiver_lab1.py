import connexion 
from connexion import NoContent
import json

DATA_FILE = "data.json"

def read_data():
    try:
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "violation_count" : 0,
            "speed_count": 0,
            "speed_events": [],
            "violation_events": []
        }

def write_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent = 2)

def report_speed (body):
    print("Received speed event:", body)

    data = read_data()
    data["speed_events"].append(body)
    data["speed_count"] += 1
    write_data(data)

    return NoContent, 201

def report_violation(body):
    print("Received violation event:", body)

    data = read_data()
    data["violation_events"].append(body)
    data["violation_count"] += 1
    write_data(data)

    return NoContent,201

app = connexion.AsyncApp(__name__, specification_dir='') 
app.add_api("speeding.yaml", strict_validation=True, validate_responses=True)