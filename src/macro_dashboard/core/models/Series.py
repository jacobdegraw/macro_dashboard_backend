from datetime import date
from pydantic import BaseModel, StringConstraints
from typing_extensions import Annotated
from typing import List
import pandas as pd

# TODO: Make this more robust, make enum if there are only a few?
FrequencyCode = Annotated[
    str,
    StringConstraints(min_length=1, max_length=1)
]

class Series(BaseModel):
    series_id: str
    title: str
    observation_start: date
    observation_end: date
    frequency: str
    frequency_short: FrequencyCode
    units: str
    units_short: str
    seasonal_adjustment: str

    # TODO: Make this an enum instead of string?
    seasonal_adjustment_short: str
    last_updated: date
    popularity: int
    notes: str




class SeriesCollection(BaseModel):
    series_list: List[Series]

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert to pandas DataFrame
        """
        df = pd.DataFrame(
            {
                "series_id": [s.series_id for s in self.series_list],
                "title": [s.title for s in self.series_list],
                "observation_start": [s.observation_start for s in self.series_list],
                "observation_end": [s.observation_end for s in self.series_list],
                "frequency": [s.frequency for s in self.series_list],
                "frequency_short": [s.frequency_short for s in self.series_list],
                "units": [s.units for s in self.series_list],
                "units_short": [s.units_short for s in self.series_list],
                "seasonal_adjustment": [s.seasonal_adjustment for s in self.series_list],
                "seasonal_adjustment_short": [s.seasonal_adjustment_short for s in self.series_list],
                "last_updated": [s.last_updated for s in self.series_list],
                "popularity": [s.popularity for s in self.series_list],
                "notes": [s.notes for s in self.series_list]
            }
        )

        return df
    
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