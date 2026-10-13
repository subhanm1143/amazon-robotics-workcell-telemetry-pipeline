"""
The scenario this project is actually testing you on: robotics engineering
onboards a new event type Cosmos has never seen, and you have to find out
where it went without anyone telling you what broke.
"""
from __future__ import annotations

import json
import time
import uuid

from monitoring.dlq_watcher import _kinesis, dead_letter_count

STREAM = "workcell-events"


def publish_unknown_event_type() -> str:
    event_id = str(uuid.uuid4())
    payload = {
        "eventId": event_id,
        "workcellId": "wc-099",
        "workcellType": "conveyor",        # a type the fleet generator never uses
        "eventType": "maintenance_due",    # not "status" or "fault" — unknown to the schema
        "status": "idle",
        "occurredAtMillis": int(time.time() * 1000),
        "attributes": {"hoursSinceService": 480},
    }
    kinesis = _kinesis()
    kinesis.put_record(
        StreamName=STREAM,
        Data=json.dumps(payload).encode(),
        PartitionKey=payload["workcellId"],
    )
    return event_id


def confirm_it_reached_the_dead_letter(before: int, attempts: int = 6) -> bool:
    # The validator (step 3) runs continuously; give it a few polls to catch
    # up before declaring the event lost.
    for _ in range(attempts):
        time.sleep(5)
        after = dead_letter_count(_kinesis())
        if after > before:
            return True
    return False


def main() -> None:
    before = dead_letter_count(_kinesis())
    event_id = publish_unknown_event_type()
    print(f"published {event_id} with eventType=maintenance_due (unknown to the schema)")
    landed = confirm_it_reached_the_dead_letter(before)
    if landed:
        print("confirmed: routed to workcell-events-dlq with a recorded error_reason")
        print("next real-incident step: file the schema change with robotics engineering,")
        print("add 'maintenance_due' to the validator's allowed eventType set, redeploy")
    else:
        print("not yet visible in the dead-letter stream — check the validator job is running")


if __name__ == "__main__":
    main()
