from __future__ import annotations

from dataclasses import dataclass
from typing import List

from sqlalchemy import text
from sqlalchemy.orm import Session

@dataclass(frozen=True)
class TrackedSeriesRepository:
    """
    Repository for CRUD on tracked_series.
    """

    session: Session

    def add(self, series_id: str) -> None:
        """
        Insert series_id into tracked_series.
        """

        stmt = text("""
            INSERT INTO tracked_series (series_id)
            VALUES (:series_id)
            ON CONFLICT (series_id) DO NOTHING
        """)

        self.session.execute(
            stmt,
            {"series_id": series_id}
        )

    def remove(self, series_id: str) -> None:
        """
        Remove series_id from tracked_series.
        """
        stmt = text("""
            DELETE FROM tracked_series
            WHERE series_id = :series_id
        """)

        self.session.execute(
            stmt,
            {"series_id": series_id}
        )

    def exists(self, series_id: str) -> bool:
        """
        Return True if series_id exists in tracked_series, else False.
        """
        stmt = text("""
            SELECT 1
            FROM tracked_series
            WHERE series_id = :series_id
            LIMIT 1
        """)

        result = self.session.execute(
            stmt,
            {"series_id": series_id}
        )

        row = result.fetchone()

        return row is not None


    def list_all(self) -> List[str]:
        """
        Return all tracked series_ids.
        """
        stmt = text("""
            SELECT series_id
            FROM tracked_series
            ORDER BY series_id
        """)

        result = self.session.execute(stmt)

        series_ids = [row.series_id for row in result]

        return series_ids