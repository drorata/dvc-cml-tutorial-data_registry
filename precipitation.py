import datetime
import pickle
import fire
import requests


def get_last_N_days(
    N: int,
    station: str = "USW00094728",
    data_type: str = "PRCP",
    output_filename: str = None,
):
    last_date = datetime.datetime.now() - datetime.timedelta(1)
    first_date = last_date - datetime.timedelta(N + 1)
    res = requests.get(
        "https://www.ncei.noaa.gov/access/services/data/v1",
        {
            "dataset": "daily-summaries",
            "stations": station,
            "startDate": datetime.datetime.strftime(first_date, "%Y-%m-%d"),
            "endDate": datetime.datetime.strftime(last_date, "%Y-%m-%d"),
            "format": "json",
            "dataTypes": data_type,
        },
    ).json()

    if output_filename is None:
        output_filename = f"precipitation_last_{N}_days.pkl"

    with open(output_filename, "wb") as f:
        pickle.dump(res, f)


if __name__ == "__main__":
    fire.Fire(get_last_N_days)
