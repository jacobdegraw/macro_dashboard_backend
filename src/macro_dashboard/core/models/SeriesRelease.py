# TODO: add updated date or smth
from datetime import date
from pydantic import BaseModel
from typing import List
import pandas as pd

class SeriesRelease(BaseModel):
    series_id: str
    release_id: int
    release_name: str


class SeriesReleaseCollection(BaseModel):
    series_release_list: List[SeriesRelease]

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert to pandas DataFrame
        """
        df = pd.DataFrame({
                "series_id": [s.series_id for s in self.series_release_list],
                "release_id": [s.release_id for s in self.series_release_list],
                "release_name": [s.release_name for s in self.series_release_list]
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