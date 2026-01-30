from datetime import date
from typing import List, Optional
from pydantic import BaseModel, Field
import pandas as pd

class Observation(BaseModel):
    date: date
    value: Optional[float] = Field(
        None,
        description="Missing values allowed (e.g. '.', None)"
    )
    # TODO: should this go in TimeSeries? How to account for revisions?
    pull_date: date

    model_config = {
        "frozen": True
    }


class TimeSeries(BaseModel):
    series_id: str
    observations: List[Observation]

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