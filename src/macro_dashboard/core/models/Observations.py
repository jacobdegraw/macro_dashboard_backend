from datetime import date
from typing import Any, List, Optional
from pydantic import BaseModel, Field, field_validator
import pandas as pd

class Observation(BaseModel):
    date: date
    value: Optional[float] = Field(
        None,
        description="Missing values allowed (e.g. '.', None)"
    )
    # TODO: should this go in TimeSeries? How to account for revisions?
    pull_date: date # type: ignore

    model_config = {
        "frozen": True
    }

    @field_validator("value", mode="before")
    @classmethod
    def coerce_value(cls, v: Any):
        """
        Convert non-numeric values ('.', '', None, etc.) to None.
        """
        if v in (None, "", ".", "NA", "N/A"):
            return None

        try:
            return float(v)
        except (TypeError, ValueError):
            return None


class TimeSeries(BaseModel):
    series_id: str
    observations: List[Observation]

    @classmethod
    def from_fred_payload(cls, *, series_id: str, payload: dict, pull_date: Optional[date] = None) -> "TimeSeries":
        """
        Convert a FRED 'series/observations' JSON payload into a TimeSeries.

        - Ignores extra fields in each observation (realtime_start/end, etc.)
        """
        if pull_date is None:
            pull_date = date.today()

        obs = []
        for o in payload.get("observations", []):

            obs.append(
                Observation(
                    date=o["date"],
                    value=o.get("value"),
                    pull_date=pull_date
                )
            )

        return cls(series_id=series_id, observations=obs)

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert to pandas DataFrame
        """
        df = pd.DataFrame(
            {
                "series_id": self.series_id,
                "date": [o.date for o in self.observations],
                "value": [o.value for o in self.observations],
                "pull_date": [o.pull_date for o in self.observations]
            }
        )

        return df.sort_values("date")

    def to_json(self, *, indent: int = 2) -> str:
        """
        Serialize to JSON string.
        """
        return self.model_dump_json(indent = indent)
    
    def to_dict(self) -> dict:
        """
        Python-native dict
        """
        return self.model_dump()