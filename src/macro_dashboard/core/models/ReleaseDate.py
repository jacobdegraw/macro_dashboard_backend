from datetime import date
from pydantic import BaseModel, StringConstraints
from typing_extensions import Annotated
from typing import List
import pandas as pd

class ReleaseDate(BaseModel):
    release_id: int
    release_date: date


class ReleaseDateCollection(BaseModel):
    release_date_list: List[ReleaseDate]

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert to pandas DataFrame
        """
        df = pd.DataFrame(
            {
                "release_id": [r.release_id for r in self.release_date_list],
                "release_date": [r.release_date for r in self.release_date_list]
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