"""Run the shop simulator against the real Pub/Sub topic.

    python -m app.simulate --minutes 30 --visits-per-minute 20

Leave it running while the pipeline runs: late data only exists when the
stream goes on long enough for the watermark to move past a window.
"""

import argparse
import logging
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from app.services.simulator import ShopSimulator, SimulatorConfig


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream realistic fake sales to Pub/Sub")
    parser.add_argument("--minutes", type=float, default=30.0)
    parser.add_argument("--visits-per-minute", type=float, default=20.0)
    parser.add_argument("--offline-ratio", type=float, default=0.08)
    parser.add_argument("--retry-ratio", type=float, default=0.03)
    parser.add_argument("--dry-run", action="store_true", help="print, do not publish")
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    sim = ShopSimulator(SimulatorConfig(
        visits_per_minute=args.visits_per_minute,
        offline_ratio=args.offline_ratio,
        retry_ratio=args.retry_ratio,
    ))
    publisher = None
    if not args.dry_run:
        from app.services.pubsub_service import pubsub_service
        publisher = pubsub_service

    end = time.time() + args.minutes * 60
    tick = 0
    while time.time() < end:
        events = sim.step(datetime.now(timezone.utc), seconds=1.0)
        if events and publisher:
            publisher.publish_events(events)
        tick += 1
        if tick % 10 == 0:
            s = sim.stats
            logging.info("sent=%d late_from_offline=%d retry_duplicates=%d visits=%d open_visits=%d",
                         s.sent, s.sent_late_from_offline_queue, s.sent_as_retry_duplicate,
                         s.visits_started, len(sim.visits))
        time.sleep(1.0)


if __name__ == "__main__":
    main()
