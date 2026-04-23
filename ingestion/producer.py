"""
Fenix - Kafka/Redpanda Producer

Fetches data from football-data.org and thesportsdb.com APIs and
produces messages to Redpanda topics with token-bucket rate limiting.
"""

import json
import sys
import time
import logging
import threading
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from config.settings import (
    FOOTBALL_DATA_API_KEY, FOOTBALL_DATA_BASE_URL,
    THESPORTSDB_BASE_URL, THESPORTSDB_PL_ID,
    PREMIER_LEAGUE_ID, REDPANDA_BROKER,
    TOPIC_MATCHES, TOPIC_STANDINGS,
    RATE_LIMIT_REQUESTS, RATE_LIMIT_PERIOD,
    DEV_MODE, DATE_FROM, DATE_TO,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# --- Token Bucket Rate Limiter ---

class TokenBucketRateLimiter:
    """Thread-safe token bucket rate limiter to prevent API 429 errors."""

    def __init__(self, max_tokens: int = RATE_LIMIT_REQUESTS,
                 refill_period: float = RATE_LIMIT_PERIOD):
        self.max_tokens = max_tokens
        self.refill_period = refill_period
        self.tokens = float(max_tokens)
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self):
        """Block until a token is available, then consume one."""
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                refill_rate = self.max_tokens / self.refill_period
                self.tokens = min(self.max_tokens, self.tokens + elapsed * refill_rate)
                self.last_refill = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
            time.sleep(0.5)


# --- API Clients ---

class FootballDataClient:
    """Client for the football-data.org API."""

    def __init__(self, rate_limiter: TokenBucketRateLimiter):
        self.rate_limiter = rate_limiter
        self.base_url = FOOTBALL_DATA_BASE_URL
        self.headers = {"X-Auth-Token": FOOTBALL_DATA_API_KEY}

    def _get(self, endpoint: str, params: dict = None) -> dict:
        self.rate_limiter.acquire()
        url = f"{self.base_url}{endpoint}"
        logger.info(f"  GET {url} params={params}")
        resp = requests.get(url, headers=self.headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _add_metadata(self, obj: dict) -> dict:
        obj["_metadata"] = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source": "football-data.org",
            "competition_id": PREMIER_LEAGUE_ID,
        }
        return obj

    def fetch_matches(self) -> list[dict]:
        """Fetch Premier League matches with optional date filtering."""
        params = {"competitions": PREMIER_LEAGUE_ID}
        if DEV_MODE and DATE_FROM and DATE_TO:
            params["dateFrom"] = DATE_FROM
            params["dateTo"] = DATE_TO
        data = self._get("/matches", params)
        matches = data.get("matches", [])
        for m in matches:
            self._add_metadata(m)
        logger.info(f"  Fetched {len(matches)} matches from football-data.org")
        return matches

    def fetch_standings(self) -> dict:
        """Fetch current Premier League standings."""
        data = self._get(f"/competitions/{PREMIER_LEAGUE_ID}/standings")
        self._add_metadata(data)
        logger.info(f"  Fetched standings from football-data.org")
        return data


class TheSportsDBClient:
    """Client for thesportsdb.com API for historical backfills."""

    def __init__(self, rate_limiter: TokenBucketRateLimiter):
        self.rate_limiter = rate_limiter
        self.base_url = THESPORTSDB_BASE_URL

    def _get(self, endpoint: str) -> dict:
        self.rate_limiter.acquire()
        url = f"{self.base_url}{endpoint}"
        logger.info(f"  GET {url}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def fetch_past_events(self) -> list[dict]:
        """Fetch historical match events."""
        data = self._get(f"/eventspastleague.php?id={THESPORTSDB_PL_ID}")
        events = data.get("events", []) or []
        now_iso = datetime.now(timezone.utc).isoformat()
        for event in events:
            event["_metadata"] = {
                "ingested_at": now_iso,
                "source": "thesportsdb.com",
                "competition_id": PREMIER_LEAGUE_ID,
            }
        logger.info(f"  Fetched {len(events)} historical events from thesportsdb.com")
        return events


# --- Redpanda Producer ---

class RedpandaProducer:
    """Handles message production to Redpanda/Kafka topics."""

    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": REDPANDA_BROKER,
            "client.id": "fenix-producer",
        })

    @staticmethod
    def _delivery_report(err, msg):
        if err:
            logger.error(f"  Delivery failed: {err}")

    def produce_matches(self, matches: list[dict]):
        """Produce match data messages."""
        for match in matches:
            key = str(match.get("id") or match.get("idEvent", "unknown"))
            self.producer.produce(
                topic=TOPIC_MATCHES,
                key=key.encode("utf-8"),
                value=json.dumps(match).encode("utf-8"),
                callback=self._delivery_report,
            )
        self.producer.flush()
        logger.info(f"  Produced {len(matches)} messages to {TOPIC_MATCHES}")

    def produce_standings(self, standings: dict):
        """Produce standings data message."""
        self.producer.produce(
            topic=TOPIC_STANDINGS,
            key=PREMIER_LEAGUE_ID.encode("utf-8"),
            value=json.dumps(standings).encode("utf-8"),
            callback=self._delivery_report,
        )
        self.producer.flush()
        logger.info(f"  Produced standings to {TOPIC_STANDINGS}")


def main(include_historical: bool = False):
    """Main ingestion entry point."""
    logger.info("=" * 60)
    logger.info("FENIX INGESTION - Starting process")
    logger.info("=" * 60)

    rate_limiter = TokenBucketRateLimiter()
    rp = RedpandaProducer()

    # football-data.org Ingestion
    fd_client = FootballDataClient(rate_limiter)
    try:
        matches = fd_client.fetch_matches()
        if matches:
            rp.produce_matches(matches)
    except Exception as e:
        logger.error(f"  football-data.org matches failed: {e}")

    try:
        standings = fd_client.fetch_standings()
        if standings:
            rp.produce_standings(standings)
    except Exception as e:
        logger.error(f"  football-data.org standings failed: {e}")

    # Historical Backfill (Optional)
    if include_historical:
        tsdb_client = TheSportsDBClient(rate_limiter)
        try:
            events = tsdb_client.fetch_past_events()
            if events:
                rp.produce_matches(events)
        except Exception as e:
            logger.error(f"  thesportsdb.com events failed: {e}")

    logger.info("FENIX INGESTION - Complete")


if __name__ == "__main__":
    historical = "--historical" in sys.argv
    main(include_historical=historical)
