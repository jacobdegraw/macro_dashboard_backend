from __future__ import annotations

from dataclasses import dataclass
from typing import List

from sqlalchemy import text
from sqlalchemy.orm import Session

from macro_dashboard.core.models.series import Series


@dataclass(frozen=True)
class SeriesRepository:
    """
    Repository for reads/writes against series_current and series_history.

    Usage:
        with session_scope() as session:
            repo = SeriesRepository(session)
            repo.upsert_current(series)
    """

    session: Session

    @staticmethod
    def _row_to_series(row) -> Series:
        return Series(
                    series_id = row.series_id, 
                    title = row.title,
                    observation_start = row.observation_start,
                    observation_end = row.observation_end,
                    frequency = row.frequency,
                    frequency_short = row.frequency_short,
                    units = row.units,
                    units_short = row.units_short,
                    seasonal_adjustment = row.seasonal_adjustment,
                    seasonal_adjustment_short = row.seasonal_adjustment_short,
                    last_updated = row.last_updated,
                    popularity = row.popularity,
                    notes = row.notes,
                    realtime_start = row.realtime_start,
                    realtime_end = row.realtime_end,
                    ingested_at = row.ingested_at
                )

    def upsert_current(self, series: Series) -> None:
        """
        Insert or update one row in series_current.
        """
        stmt = text("""
            INSERT INTO series_current (
                        series_id, title, observation_start, observation_end, frequency, frequency_short, 
                        units, units_short, seasonal_adjustment, seasonal_adjustment_short, last_updated, 
                        popularity, notes, realtime_start, realtime_end, ingested_at
                    )
            VALUES (
                    :series_id, :title, :observation_start, :observation_end, :frequency, :frequency_short,
                    :units, :units_short, :seasonal_adjustment, :seasonal_adjustment_short, :last_updated, 
                    :popularity, :notes, :realtime_start, :realtime_end, :ingested_at
                    )
            ON CONFLICT (series_id)
            DO UPDATE SET
                    title = EXCLUDED.title,
                    observation_start = EXCLUDED.observation_start,
                    observation_end = EXCLUDED.observation_end,
                    frequency = EXCLUDED.frequency,
                    frequency_short = EXCLUDED.frequency_short,
                    units = EXCLUDED.units,
                    units_short = EXCLUDED.units_short,
                    seasonal_adjustment = EXCLUDED.seasonal_adjustment,
                    seasonal_adjustment_short = EXCLUDED.seasonal_adjustment_short,
                    last_updated = EXCLUDED.last_updated,
                    popularity = EXCLUDED.popularity,
                    notes = EXCLUDED.notes,
                    realtime_start = EXCLUDED.realtime_start,
                    realtime_end = EXCLUDED.realtime_end,
                    ingested_at = EXCLUDED.ingested_at
        """)

        self.session.execute(
            stmt,
            {
                "series_id": series.series_id,
                "title": series.title,
                "observation_start": series.observation_start,
                "observation_end": series.observation_end,
                "frequency": series.frequency,
                "frequency_short": series.frequency_short,
                "units": series.units,
                "units_short": series.units_short,
                "seasonal_adjustment": series.seasonal_adjustment,
                "seasonal_adjustment_short": series.seasonal_adjustment_short,
                "last_updated": series.last_updated,
                "popularity": series.popularity,
                "notes": series.notes,
                "realtime_start": series.realtime_start,
                "realtime_end": series.realtime_end,
                "ingested_at": series.ingested_at
            }
        )


    def insert_history(self, series: Series) -> None:
        """
        Insert one row into series_history.
        """
        stmt = text("""
            INSERT INTO series_history (
                        series_id, title, observation_start, observation_end, frequency, frequency_short, 
                        units, units_short, seasonal_adjustment, seasonal_adjustment_short, last_updated, 
                        popularity, notes, realtime_start, realtime_end, ingested_at
                    )
            VALUES (
                    :series_id, :title, :observation_start, :observation_end, :frequency, :frequency_short,
                    :units, :units_short, :seasonal_adjustment, :seasonal_adjustment_short, :last_updated, 
                    :popularity, :notes, :realtime_start, :realtime_end, :ingested_at
                    )
        """)

        self.session.execute(
            stmt,
            {
                "series_id": series.series_id,
                "title": series.title,
                "observation_start": series.observation_start,
                "observation_end": series.observation_end,
                "frequency": series.frequency,
                "frequency_short": series.frequency_short,
                "units": series.units,
                "units_short": series.units_short,
                "seasonal_adjustment": series.seasonal_adjustment,
                "seasonal_adjustment_short": series.seasonal_adjustment_short,
                "last_updated": series.last_updated,
                "popularity": series.popularity,
                "notes": series.notes,
                "realtime_start": series.realtime_start,
                "realtime_end": series.realtime_end,
                "ingested_at": series.ingested_at
            }
        )

    def exists(self, series_id: str) -> bool:
        """
        Return True if series_id exists in series_current, else False.
        """
        stmt = text("""
            SELECT 1
            FROM series_current
            WHERE series_id = :series_id
            LIMIT 1
        """)
        
        result = self.session.execute(
            stmt,
            {"series_id": series_id}
        )

        row = result.fetchone()

        return row is not None
        

    def get_series_current(self, series_id: str) -> Series | None:
        """
        Fetch one series from series_current by series_id.
        """
        stmt = text("""
            SELECT series_id, title, observation_start, observation_end, frequency, frequency_short, units, 
                    units_short, seasonal_adjustment, seasonal_adjustment_short, last_updated, popularity, 
                    notes, realtime_start, realtime_end, ingested_at
            FROM series_current
            WHERE series_id = :series_id
        """)

        result = self.session.execute(
                stmt,
                {"series_id": series_id}
            ).fetchone()
        
        if result is None:
            return None
        else:
            return self._row_to_series(result)

    
    def get_series_history(self, series_id: str) -> List[Series]:
        """
        Get history of metadata of a given series (or empty list if none)
        """
        stmt = text("""
            SELECT series_id, title, observation_start, observation_end, frequency, frequency_short, units, 
                    units_short, seasonal_adjustment, seasonal_adjustment_short, last_updated, popularity, 
                    notes, realtime_start, realtime_end, ingested_at
            FROM series_history
            WHERE series_id = :series_id
            ORDER BY realtime_start DESC
            """)
        
        result = self.session.execute(
                stmt,
                {"series_id": series_id}
            )
        
        return [self._row_to_series(row) for row in result]


    def get_all_current(self) -> List[Series]:
        """
        Fetch all rows from series_current (or empty list if none).
        """
        stmt = text("""
            SELECT series_id, title, observation_start, observation_end, frequency, frequency_short, units, 
                    units_short, seasonal_adjustment, seasonal_adjustment_short, last_updated, popularity, 
                    notes, realtime_start, realtime_end, ingested_at 
            FROM series_current
            ORDER BY series_id
        """)

        result = self.session.execute(stmt)

        return [self._row_to_series(row) for row in result]
