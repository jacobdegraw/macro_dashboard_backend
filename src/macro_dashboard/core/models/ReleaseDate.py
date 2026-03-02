from datetime import date, datetime
from pydantic import BaseModel, StringConstraints
from typing_extensions import Annotated
from typing import List
import pandas as pd

class ReleaseDate(BaseModel):
    release_id: int
    release_name: str
    release_date: date
    pull_date: datetime


class ReleaseDateCollection(BaseModel):
    release_date_list: List[ReleaseDate]

    @classmethod
    def from_fred_payload(cls, *, payload: dict, pull_date = None):
        """
        Convert a FRED 'release/date' JSON payload into a ReleaseDateCollection.
        """
        if pull_date is None:
            pull_date = datetime.now()

        release_dates = []
        for rd in payload.get("release_dates"):
            release_dates.append(
                ReleaseDate(
                    release_id=rd["release_id"],
                    release_name=rd["release_name"],
                    release_date=rd["date"],
                    pull_date=pull_date
                )
            )

        return cls(release_date_list=release_dates)

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert to pandas DataFrame
        """
        df = pd.DataFrame(
            {
                "release_id": [r.release_id for r in self.release_date_list],
                "release_name": [r.release_name for r in self.release_date_list],
                "release_date": [r.release_date for r in self.release_date_list],
                "pull_date": [r.pull_date for r in self.release_date_list]
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