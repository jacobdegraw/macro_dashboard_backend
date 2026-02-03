import requests
from macro_dashboard.core.settings import get_settings
from macro_dashboard.core.models.Series import Series, SeriesCollection


class Fred:
    def __init__(self):
        settings = get_settings()

        self.fred_api_key = settings.fred_api_key
        self.fred_base_url = settings.fred_base_url
        self.fred_timeout_seconds = settings.fred_timeout_seconds
        self.fred_retry_count = settings.fred_retry_count



    def pull_series_metadata(self, series_id: str) -> Series:
        """
        Function to pull metadata for a single series
        """
        series: Series = None
        endpoint = f"{self.fred_base_url}/series"

        params = {
            "series_id": series_id,
            "api_key": self.fred_api_key,
            "file_type": "json"
        }

        for i in range(self.fred_retry_count):
            if series != None:
                break

            try:
                response = requests.get(endpoint, params = params)
                data = response.json()

                series = Series.model_validate(data["seriess"][0])

            except:
                if i != self.fred_retry_count:
                    print(f"Unable to pull the series: {series_id}. Trying again...")
                

                else:
                    print(f"Unable to pull the series: {series_id}")

                    return None
                
        return series


        

    def pull_series_observations(series_id: str) -> Observations.TimeSeries:
        pass

    def pull_series_release(series_id):
        pass

    def pull_releases():
        pass

    def pull_release_dates(release_id):
        pass