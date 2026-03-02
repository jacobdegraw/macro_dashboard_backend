import logging
import random
import time
from typing import Optional, Dict, Any

import requests
from pydantic import ValidationError



from macro_dashboard.core.settings import get_settings

# Models
from macro_dashboard.core.models.Series import Series
from macro_dashboard.core.models.Observations import TimeSeries
from macro_dashboard.core.models.SeriesRelease import SeriesRelease
from macro_dashboard.core.models.Release import ReleaseCollection
from macro_dashboard.core.models.ReleaseDate import ReleaseDateCollection

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Simple, centralized limiter: enforces at most N requests/sec by spacing requests.
    Safe for single-process usage (typical ingestion worker).
    """
    def __init__(self, rate_per_sec: float):
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be > 0")
        self._min_interval = 1.0 / rate_per_sec
        self._next_allowed = 0.0

    def acquire(self) -> None:
        now = time.monotonic()
        if now < self._next_allowed:
            time.sleep(self._next_allowed - now)
            now = time.monotonic()
        self._next_allowed = now + self._min_interval


class FredHttpClient:
    """
    Owns HTTP concerns:
      - requests.Session reuse
      - throttling
      - retries + exponential backoff
      - 429 Retry-After support
      - timeout and raise_for_status
    """
    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout_seconds: float,
        retry_count: int,
        rate_limit_per_sec: float,
        backoff_max_seconds: float,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout_seconds
        self.retry_count = retry_count
        self.backoff_max_seconds = backoff_max_seconds

        self.session = requests.Session()
        self.limiter = RateLimiter(rate_limit_per_sec)

    def _sleep_backoff(self, attempt_index: int, retry_after: Optional[float] = None) -> None:
        """
        attempt_index: 0-based (0 for first failure, 1 for second, ...)
        """
        if retry_after is not None:
            wait = max(0.0, min(float(retry_after), self.backoff_max_seconds))
        else:
            # Exponential backoff + jitter
            wait = min((2 ** attempt_index), self.backoff_max_seconds) + random.random()
        time.sleep(wait)

    def get_json(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        full_params = dict(params)
        full_params["api_key"] = self.api_key
        full_params.setdefault("file_type", "json")

        # We do up to retry_count attempts total
        for attempt in range(self.retry_count):
            self.limiter.acquire()

            try:
                resp = self.session.get(url, params=full_params, timeout=self.timeout)

                # Handle rate limiting explicitly
                if resp.status_code == 429:
                    if attempt < self.retry_count - 1:
                        retry_after_hdr = resp.headers.get("Retry-After")
                        retry_after = float(retry_after_hdr) if retry_after_hdr else None
                        logger.warning("FRED 429 for %s; backing off (attempt %d/%d)", url, attempt + 1, self.retry_count)
                        self._sleep_backoff(attempt, retry_after=retry_after)
                        continue
                    resp.raise_for_status()

                # Retry on transient 5xx
                if 500 <= resp.status_code <= 599:
                    if attempt < self.retry_count - 1:
                        logger.warning(
                            "FRED %s for %s; backing off (attempt %d/%d)",
                            resp.status_code, url, attempt + 1, self.retry_count
                        )
                        self._sleep_backoff(attempt)
                        continue
                    resp.raise_for_status()

                # For 4xx (other than 429), don't retry—it's usually your params or key.
                resp.raise_for_status()

                data = resp.json()

                # FRED can return explicit error payloads
                if "error_message" in data:
                    # Treat as non-retryable (bad key/params), but log it.
                    raise RuntimeError(f"FRED error: {data.get('error_message')}")

                return data

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                # Transient network issues: retry
                if attempt < self.retry_count - 1:
                    logger.warning("Network error calling FRED (%s). Retrying (%d/%d)...", e, attempt + 1, self.retry_count)
                    self._sleep_backoff(attempt)
                    continue
                raise

            except requests.exceptions.HTTPError as e:
                # Non-retryable HTTP errors get raised here (e.g., 400/401/403/404)
                # You can choose to return {} instead, but raising is better for ingestion jobs.
                raise

            except ValueError as e:
                # JSON decode error, etc.
                if attempt < self.retry_count - 1:
                    logger.warning("Bad JSON from FRED (%s). Retrying (%d/%d)...", e, attempt + 1, self.retry_count)
                    self._sleep_backoff(attempt)
                    continue
                raise


class Fred:
    """
    Source-layer public interface: returns domain models (Series, etc).
    HTTP mechanics are delegated to FredHttpClient.
    """
    def __init__(self):
        settings = get_settings()

        self._client = FredHttpClient(
            base_url=settings.fred_base_url,
            api_key=settings.fred_api_key,
            timeout_seconds=settings.fred_timeout_seconds,
            retry_count=settings.fred_retry_count,
            rate_limit_per_sec=getattr(settings, "fred_rate_limit_per_sec", 3.0),
            backoff_max_seconds=getattr(settings, "fred_backoff_max_seconds", 30.0),
        )

    def pull_series_metadata(self, series_id: str) -> Optional[Series]:
        """
        Pull metadata for a single series_id.
        Returns None if the series doesn't exist.
        Raises on transport/auth/etc errors (good for ingestion jobs to detect failures).
        """
        params = {"series_id": series_id}

        data = self._client.get_json("/series", params)

        seriess = data.get("seriess") or []
        if not seriess:
            # series not found, or empty result
            return None

        try:
            return Series.model_validate(seriess[0])
        except ValidationError as e:
            # Surface schema mismatch clearly
            raise RuntimeError(f"Series model validation failed for {series_id}: {e}") from e

    def pull_series_observations(self, series_id: str) -> Optional[TimeSeries]:
        """
        Pull observations for a single series_id.
        Returns None if the series doesn't exist.
        """
        params = {"series_id": series_id}
        data = self._client.get_json("/series/observations", params)

        try: 
            return TimeSeries.from_fred_payload(series_id = series_id, payload = data)
        except ValidationError as e:
            # Surface schema mismatch clearly
            raise RuntimeError(f"TimeSeries model validation failed for {series_id}: {e}") from e

    def pull_series_release(self, series_id) -> Optional[SeriesRelease]:
        """
        Pull release information for a given series
        """
        params = {"series_id": series_id}
        data = self._client.get_json("/series/release", params)
        data["releases"]

        try:
            return SeriesRelease.model_validate(data)
        except ValidationError as e:
            # Surface schema mismatch clearly
            raise RuntimeError(f"SeriesRelease model validation failed for {series_id}: {e}") from e

    def pull_releases(self):
        """
        Pull all releases
        
        :param self: Description
        """

        data = self._client.get_json("/releases", {})

        try:
            return ReleaseCollection.from_fred_payload(payload=data)
        except ValidationError as e:
            raise RuntimeError(f"Release pull failed: {e}")

    def pull_release_dates(self):
        """
        Pull all release dates
        """
        data = self._client.get_json("/releases/dates", {})

        try:
            return ReleaseDateCollection.from_fred_payload(payload = data)
        except ValidationError as e:
            raise RuntimeError(f"Release date pull failed: {e}")




