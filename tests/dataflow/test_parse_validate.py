import json
from datetime import datetime, timezone

from apache_beam import pvalue
from apache_beam.io.gcp.pubsub import PubsubMessage

from dataflow.transforms.parse import BRONZE, DEAD_LETTER, ParsePubsubMessage
from dataflow.transforms.validate import ValidateSale

ARRIVAL = datetime(2026, 11, 12, 10, 7, 0, tzinfo=timezone.utc)


def good_payload(**overrides):
    payload = {
        "transaction_id": "t-1", "user_id": "user-001", "region": "EU", "product_id": "prod-001",
        "amount": 12.5, "currency": "eur", "event_time": "2026-11-12T10:03:00.000Z", "schema_version": 1,
    }
    payload.update(overrides)
    return payload


def run_parse(body: bytes):
    msg = PubsubMessage(body, {"event_time": "2026-11-12T10:03:00.000Z", "transaction_id": "t-1"},
                        message_id="m-1", publish_time=ARRIVAL)
    out = list(ParsePubsubMessage().process(msg))
    tagged = {o.tag: o.value for o in out if isinstance(o, pvalue.TaggedOutput)}
    main = [o for o in out if not isinstance(o, pvalue.TaggedOutput)]
    return main, tagged


def run_validate(record):
    out = list(ValidateSale().process(record))
    dead = [o.value for o in out if isinstance(o, pvalue.TaggedOutput)]
    good = [o for o in out if not isinstance(o, pvalue.TaggedOutput)]
    return good, dead


def test_parse_keeps_arrival_time_from_pubsub_publish_time():
    main, tagged = run_parse(json.dumps(good_payload()).encode())
    assert main[0]["arrival_time"] == "2026-11-12T10:07:00.000000Z"
    assert main[0]["message_id"] == "m-1"
    assert tagged[BRONZE]["transaction_id"] == "t-1"


def test_bad_json_still_lands_in_bronze_and_dead_letter():
    main, tagged = run_parse(b"{not json")
    assert main == []
    assert tagged[BRONZE]["raw_payload"] == "{not json"
    assert tagged[DEAD_LETTER]["error_type"] == "bad_json"


def test_valid_sale_becomes_silver_row():
    main, _ = run_parse(json.dumps(good_payload()).encode())
    good, dead = run_validate(main[0])
    assert dead == []
    assert good[0]["currency"] == "EUR"
    assert good[0]["event_time"] == "2026-11-12T10:03:00.000000Z"
    assert good[0]["arrival_time"] == "2026-11-12T10:07:00.000000Z"


def test_rules_send_bad_rows_to_dead_letter():
    for bad in (good_payload(amount=-1), good_payload(region="MARS"), good_payload(amount="12")):
        main, _ = run_parse(json.dumps(bad).encode())
        good, dead = run_validate(main[0])
        assert good == [] and dead[0]["stage"] == "validate"


def test_future_event_time_is_rejected_as_clock_skew():
    main, _ = run_parse(json.dumps(good_payload(event_time="2026-11-12T10:30:00.000Z")).encode())
    good, dead = run_validate(main[0])
    assert good == [] and "clock skew" in dead[0]["errors"]
