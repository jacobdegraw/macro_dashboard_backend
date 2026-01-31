# TODO: add updateddate or smth
from datetime import date
from pydantic import BaseModel
from typing import List
import pandas as pd

class Release(BaseModel):
    id: int
    name: str
    press_release: bool
    # TODO: make sure this is robust
    link: str


class ReleaseCollection(BaseModel):
    release_list: List[Release]

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert to pandas DataFrame
        """
        df = pd.DataFrame(
            {
                "id": [r.id for r in self.release_list],
                "name": [r.name for r in self.release_list],
                "press_release": [r.press_release for r in self.release_list],
                "link": [r.link for r in self.release_list]
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