import paho.mqtt.client as mqtt
import json
import time
import random
from faker import Faker

fake = Faker()

# =========================================================
# STATE TRACKING
# =========================================================
active_ownership_cases = {}
active_fmd_cases = {}

# =========================================================
# LOCATION MAP
# =========================================================
village_district_map = {
    "Mogoditshane": "Kweneng",
    "Letlhakane": "Central",
    "Palapye": "Central",
    "Maun": "North-West",
    "Serowe": "Central",
    "Gaborone": "South-East",
    "Kanye": "Southern",
    "Shakawe": "North-West",
    "Nxamasere": "North-West"
}

villages = list(village_district_map.keys())

# =========================================================
# ANIMAL GENERATOR (STANDARDIZED)
# =========================================================
def base_animal():
    return {
        "tag_id": f"BW-COW-{random.randint(100000, 999999)}",
        "species": "cattle",
        "breed": random.choice([
            "Brahman", "Nguni", "Bonsmara",
            "Tuli", "Tswana", "Angoni", "Simmental"
        ]),
        "sex": random.choice(["male", "female"]),
        "birth_date": f"20{random.randint(18, 23)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
    }


# =========================================================
# OWNERSHIP FLOW (FIXED + CONSISTENT PAYLOAD)
# =========================================================
def generate_ownership_flow():

    case_id = f"OWN-{random.randint(1000,9999)}"

    farmer1 = f"FARMER_{random.randint(1, 10)}"
    farmer2 = f"FARMER_{random.randint(1, 10)}"

    # avoid self-transfer
    while farmer2 == farmer1:
        farmer2 = f"FARMER_{random.randint(1, 10)}"

    animal = base_animal()

    active_ownership_cases[case_id] = {
        "from": farmer1,
        "to": farmer2,
        "tag_id": animal["tag_id"]
    }

    # ---------------- REQUEST
    request = {
        "event_type": "OWNERSHIP_TRANSFER_REQUEST",
        "event_id": fake.uuid4(),
        "case_id": case_id,
        "tag_id": animal["tag_id"],
        "from_owner": farmer1,
        "to_owner": farmer2,
        "status": "PENDING",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    yield request

    # ---------------- DECISION
    decision = random.choice(["ACCEPTED", "REJECTED"])

    if decision == "ACCEPTED":

        accept = {
            "event_type": "OWNERSHIP_TRANSFER_ACCEPT",
            "event_id": fake.uuid4(),
            "case_id": case_id,
            "tag_id": animal["tag_id"],
            "from_owner": farmer1,
            "to_owner": farmer2,
            "status": "ACCEPTED",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        yield accept

        ack = {
            "event_type": "OWNERSHIP_TRANSFER_ACK",
            "event_id": fake.uuid4(),
            "case_id": case_id,
            "tag_id": animal["tag_id"],
            "from_owner": farmer1,
            "to_owner": farmer2,
            "status": "ACKNOWLEDGED",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        yield ack

    else:

        reject = {
            "event_type": "OWNERSHIP_TRANSFER_REJECT",
            "event_id": fake.uuid4(),
            "case_id": case_id,
            "tag_id": animal["tag_id"],
            "from_owner": farmer1,
            "to_owner": farmer2,
            "status": "REJECTED",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        yield reject

        ack = {
            "event_type": "OWNERSHIP_TRANSFER_ACK",
            "event_id": fake.uuid4(),
            "case_id": case_id,
            "tag_id": animal["tag_id"],
            "from_owner": farmer1,
            "to_owner": farmer2,
            "status": "ACKNOWLEDGED",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        yield ack


# =========================================================
# MAIN EVENT GENERATOR
# =========================================================
def generate_data():

    event_type = random.choice([
        "ANIMAL_REGISTRATION",
        "ANIMAL_MOVEMENT",
        "HEALTH_TREATMENT",
        "ANIMAL_DEATH",
        "OWNERSHIP_FLOW"
    ])

    # ---------------- OWNERSHIP FLOW
    if event_type == "OWNERSHIP_FLOW":
        return list(generate_ownership_flow())

    animal = base_animal()

    village = random.choice(villages)

    event = {
        "event_type": event_type,
        "event_id": fake.uuid4(),
        "tag_id": animal["tag_id"],   # IMPORTANT FIX
        "species": animal["species"],
        "breed": animal["breed"],
        "sex": animal["sex"],
        "birth_date": animal["birth_date"],
        "location": {
            "village": village,
            "district": village_district_map[village]
        },
        "status": "NEW",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    # ---------------- OPTIONAL EVENT DETAILS
    if event_type == "ANIMAL_REGISTRATION":
        event["owner_id"] = f"FARMER_{random.randint(1,10)}"

    elif event_type == "ANIMAL_MOVEMENT":
        event["movement"] = {
            "from": random.choice(["Farm-A", "Farm-B"]),
            "to": random.choice(["Farm-C", "Farm-D"]),
            "permit_id": f"perm-{random.randint(10000,99999)}"
        }

    elif event_type == "HEALTH_TREATMENT":
        event["treatment"] = {
            "disease": random.choice(["foot_and_mouth", "anthrax"]),
            "vaccine": "FMD-O-2025",
            "dose_ml": round(random.uniform(2.0, 10.0), 2)
        }

    elif event_type == "ANIMAL_DEATH":
        event["cause"] = random.choice(["natural", "disease"])

    return [event]


# =========================================================
# MQTT PUBLISHER
# =========================================================
def publish():
    client = mqtt.Client()
    client.connect("mosquitto", 1883, 60)

    while True:
        events = generate_data()

        for event in events:
            payload = json.dumps(event)
            client.publish("baits/events", payload)
            print("Published:", payload)
            time.sleep(1)

        time.sleep(2)


# =========================================================
# SUBSCRIBER
# =========================================================
def on_message(client, userdata, msg):
    print(msg.payload.decode())


def subscribe():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect("mosquitto", 1883, 60)
    client.subscribe("baits/events")
    client.loop_forever()


# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "pub":
        publish()
    else:
        subscribe()